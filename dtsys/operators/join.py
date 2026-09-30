"""连接算子：分块 + 临时落盘的递归分区哈希连接。

保留全部重复匹配（多对多）：同一键的每个左行 × 每个右行都输出。
工作集超过内存预算时，按哈希把两侧数据递归分区，直到某个分区可放入
内存再做哈希探测；任何一侧都无需整体驻留内存。

倾斜键（两侧连接键完全相同，哈希分区无法再切开）在递归位耗尽后，
改用分块嵌套循环连接（blocked nested loop join）：每一侧一次只驻留
不超过预算一半的一个块，左块 × 右块逐块配对，输出行产生一行写出一行，
既不物化整个乘积结果，也不靠丢弃/细分同一个键来回避乘积数量。

params:
  right_source: 右数据源名称（左输入为上一阶段结果）
  left_on / right_on: 连接键（右键可缺省，表示同名）
  how: inner（默认）或 left
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil

from ..batches import jsonl_batches, merge_columns, read_jsonl, write_jsonl, row_size
from .base import Operator
from .registry import register_operator


MAX_PARTITION_DEPTH = 16  # blake2b 十六进制位数量，用尽后键无法再分区


def _key_hash(value) -> str:
    return hashlib.blake2b(repr(value).encode("utf-8"), digest_size=16).hexdigest()


def _merged_row(left, right, left_on, right_on, right_columns):
    merged = dict(left)
    for col in right_columns:
        if col == right_on:
            continue
        merged[col] = right[col]
    if right_on != left_on:
        merged[right_on] = right.get(right_on)
    return merged


def _left_unmatched(left, left_on, right_on, right_columns):
    merged = dict(left)
    for col in right_columns:
        if col == right_on:
            continue
        merged.setdefault(col, None)
    if right_on != left_on:
        merged.setdefault(right_on, None)
    return merged


def _iter_jsonl_blocks(paths, max_bytes):
    """逐块（不超过 max_bytes 字节，单行例外）流式读出 JSONL。

    任何时刻只保留一个块在内存中。
    """
    if max_bytes <= 0:
        max_bytes = 1
    block: list[dict] = []
    buffered = 0
    for path in paths:
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                size = len(line.encode("utf-8"))
                if block and buffered + size > max_bytes:
                    yield block
                    block = []
                    buffered = 0
                block.append(row)
                buffered += size
    if block:
        yield block


@register_operator("join")
class JoinOperator(Operator):
    streaming = False

    def __init__(self, params: dict, ctx):
        super().__init__(params, ctx)
        self.right_source = params["right_source"]
        self.left_on = params["left_on"]
        self.right_on = params.get("right_on", self.left_on)
        self.how = params.get("how", "inner")
        if self.how not in ("inner", "left"):
            raise ValueError(f"不支持的连接方式: {self.how}")
        self.peak_inflight_bytes = 0

    def _note_inflight(self, num_bytes: int) -> None:
        """记录当前驻留内存的中间数据量（用于预算校验与测试观测）。"""
        if num_bytes > self.peak_inflight_bytes:
            self.peak_inflight_bytes = num_bytes
        stats = getattr(self.ctx, "stats", None)
        if isinstance(stats, dict):
            prev = stats.get("join_peak_inflight_bytes", 0)
            if num_bytes > prev:
                stats["join_peak_inflight_bytes"] = num_bytes

    def run(self, input_files, columns):
        right_path = self.ctx.resolve_source(self.right_source)
        right_columns: list[str] = []
        for batch in jsonl_batches(right_path, max(1, self.ctx.work_bytes // 2)):
            right_columns = merge_columns(right_columns, batch.rows)
            self._note_inflight(self._rows_bytes(batch.rows))
        self._note_inflight(0)

        left_columns = list(columns)
        final_columns: list[str] = []
        for col in left_columns:
            if col not in final_columns:
                final_columns.append(col)
        for col in right_columns:
            if col != self.right_on and col not in final_columns:
                final_columns.append(col)
        if self.right_on not in final_columns:
            final_columns.append(self.right_on)

        output_file = os.path.join(self.ctx.temp_dir, f"join-out-{id(self)}.jsonl")
        spill_dir = os.path.join(self.ctx.temp_dir, f"join-spill-{id(self)}")
        os.makedirs(spill_dir, exist_ok=True)
        self.peak_inflight_bytes = 0
        try:
            with open(output_file + ".tmp", "w", encoding="utf-8") as out_fh:
                self._join_files(
                    input_files, right_path, right_columns, final_columns,
                    depth=0, spill_dir=spill_dir, out_fh=out_fh,
                )
            os.replace(output_file + ".tmp", output_file)
        except BaseException:
            tmp_path = output_file + ".tmp"
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            shutil.rmtree(spill_dir, ignore_errors=True)
            raise
        return [output_file], final_columns

    @staticmethod
    def _rows_bytes(rows) -> int:
        return sum(row_size(row) for row in rows)

    def _emit_row(self, row, out_fh) -> None:
        # 输出行只以 JSON 字节形态短暂经过写缓冲，绝不累积成结果列表
        out_fh.write(json.dumps(row, ensure_ascii=False, default=str))
        out_fh.write("\n")

    def _join_in_memory(self, left_rows, right_rows, right_columns,
                        out_fh) -> None:
        """两侧整体驻留内存（已确认在预算内）：哈希探测，逐行产出。"""
        index: dict = {}
        for row in right_rows:
            index.setdefault(repr(row.get(self.right_on)), []).append(row)
        self._note_inflight(
            self._rows_bytes(left_rows) + self._rows_bytes(right_rows)
        )
        for left in left_rows:
            matches = index.get(repr(left.get(self.left_on)))
            if matches:
                for right in matches:  # 多对多：重复匹配全部保留
                    self._emit_row(
                        _merged_row(left, right, self.left_on,
                                    self.right_on, right_columns),
                        out_fh,
                    )
            elif self.how == "left":
                self._emit_row(
                    _left_unmatched(left, self.left_on,
                                    self.right_on, right_columns),
                    out_fh,
                )

    def _blocked_nested_loop_join(self, left_paths, right_path, right_columns,
                                  spill_dir, out_fh) -> None:
        """倾斜键兜底：分块嵌套循环连接，内存占用有界。

        两侧各切成不超过预算一半的块（落盘复用），左块 × 右块逐对配对；
        每个左块记录其各行是否曾匹配，全部右块消费完后再为未匹配行补
        left join 空值行。输出逐行写盘，乘积行数一行不少。
        """
        budget = max(1, self.ctx.work_bytes)
        side_budget = max(1, budget // 2)
        block_dir = os.path.join(
            spill_dir, f"bnlj-{id(self)}-{os.getpid()}"
        )
        os.makedirs(block_dir, exist_ok=True)

        def split_to_blocks(paths, side):
            block_paths: list[str] = []
            for block in _iter_jsonl_blocks(paths, side_budget):
                path = os.path.join(block_dir, f"{side}-{len(block_paths)}.jsonl")
                write_jsonl(path, block, [])
                block_paths.append(path)
            return block_paths

        left_block_paths = split_to_blocks(left_paths, "l")
        right_block_paths = split_to_blocks([right_path], "r")

        try:
            for left_block_path in left_block_paths:
                self.check_cancelled()
                left_block = read_jsonl(left_block_path)
                matched = bytearray(len(left_block))
                for right_block_path in right_block_paths:
                    right_block = read_jsonl(right_block_path)
                    self._note_inflight(
                        self._rows_bytes(left_block)
                        + self._rows_bytes(right_block)
                    )
                    for pos, left in enumerate(left_block):
                        lkey = repr(left.get(self.left_on))
                        for right in right_block:
                            if lkey == repr(right.get(self.right_on)):
                                matched[pos] = 1
                                self._emit_row(
                                    _merged_row(left, right, self.left_on,
                                                self.right_on, right_columns),
                                    out_fh,
                                )
                    self._note_inflight(self._rows_bytes(left_block))
                if self.how == "left":
                    for pos, left in enumerate(left_block):
                        if not matched[pos]:
                            self._emit_row(
                                _left_unmatched(left, self.left_on,
                                                self.right_on, right_columns),
                                out_fh,
                            )
                self._note_inflight(0)
            if self.how == "left" and not left_block_paths:
                return
        finally:
            shutil.rmtree(block_dir, ignore_errors=True)

    def _spill_partitions(self, left_paths, right_path, depth, spill_dir):
        """按哈希的第 depth 个十六进制位（16 桶）把两侧数据分桶落盘。"""
        tag = f"d{depth}-{id(self)}"
        handles: dict[str, dict] = {}

        def writer(side, bucket):
            group = handles.setdefault(bucket, {})
            if side not in group:
                path = os.path.join(spill_dir, f"{tag}-{side}-{bucket}.jsonl")
                group[side] = open(path, "w", encoding="utf-8")
            return group[side]

        def feed(paths, side, key):
            for path in paths:
                with open(path, "r", encoding="utf-8") as fh:
                    for line in fh:
                        if not line.strip():
                            continue
                        row = json.loads(line)
                        bucket = _key_hash(row.get(key))[depth % 16]
                        writer(side, bucket).write(line if line.endswith("\n")
                                                   else line + "\n")

        try:
            feed(left_paths, "l", self.left_on)
            feed([right_path], "r", self.right_on)
        finally:
            for group in handles.values():
                for fh in group.values():
                    fh.close()

        paths = {}
        for bucket in handles:
            paths[bucket] = {
                side: os.path.join(spill_dir, f"{tag}-{side}-{bucket}.jsonl")
                for side in handles[bucket]
            }
        return paths

    def _join_files(self, left_paths, right_path, right_columns,
                    final_columns, depth, spill_dir, out_fh):
        self.check_cancelled()
        left_size = sum(
            os.path.getsize(p) for p in left_paths if os.path.exists(p)
        )
        right_size = os.path.getsize(right_path) if os.path.exists(right_path) else 0

        if left_size + right_size <= self.ctx.work_bytes:
            left_rows = []
            for path in left_paths:
                if os.path.exists(path):
                    left_rows.extend(read_jsonl(path))
            right_rows = read_jsonl(right_path) if os.path.exists(right_path) else []
            self._note_inflight(
                self._rows_bytes(left_rows) + self._rows_bytes(right_rows)
            )
            self._join_in_memory(
                left_rows, right_rows, right_columns, out_fh,
            )
            self._note_inflight(0)
            return

        if depth >= MAX_PARTITION_DEPTH:
            # 哈希位已用尽仍超预算：典型的倾斜键（两侧全部同键），
            # 分区无法再把它们拆开，改用有界的分块嵌套循环连接。
            self._blocked_nested_loop_join(
                left_paths, right_path, right_columns, spill_dir, out_fh,
            )
            return

        bucket_paths = self._spill_partitions(
            left_paths, right_path, depth, spill_dir
        )
        for bucket, paths in sorted(bucket_paths.items()):
            left_bucket = [paths["l"]] if "l" in paths else []
            right_bucket = paths.get("r")
            if not left_bucket and self.how != "left":
                continue
            if right_bucket is None:
                if self.how == "inner":
                    continue
                right_bucket = os.path.join(spill_dir, f"empty-{bucket}.jsonl")
                write_jsonl(right_bucket, [], [])
            self._join_files(
                left_bucket, right_bucket, right_columns, final_columns,
                depth + 1, spill_dir, out_fh,
            )
            for path in paths.values():
                if os.path.exists(path):
                    os.remove(path)
