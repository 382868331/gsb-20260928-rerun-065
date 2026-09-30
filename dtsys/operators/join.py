"""连接算子：分块 + 临时落盘的递归分区哈希连接，倾斜键走分块嵌套循环。

保留全部重复匹配（多对多）：同一键的每个左行 × 每个右行都输出，
乘积数量一行不少，但结果逐行推入有界通道，从不在内存中攒成整表。

两层预算控制：
- 工作集：两侧总量超预算时按哈希位递归分区；若某个子分区几乎没有变小
  （热键/同键倾斜，继续细分同一个键没有意义），或哈希位用尽，
  改为分块嵌套循环：构建侧只保留一个预算内的块，探测侧逐行流式读取。
- 在途结果：匹配结果逐行进入 BoundedResultSink，下游写出很慢时
  生产者在预算上限处阻塞（背压），中间结果不会无界堆积。

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

from ..batches import merge_columns, row_size, write_jsonl
from ..flow import BoundedResultSink
from .base import Operator
from .registry import register_operator


def _key_hash(value) -> str:
    return hashlib.blake2b(repr(value).encode("utf-8"), digest_size=16).hexdigest()


def _merged_columns(left_columns, right_columns, right_on):
    out_columns = list(left_columns)
    for col in right_columns:
        if col != right_on and col not in out_columns:
            out_columns.append(col)
    if right_on not in out_columns:
        out_columns.append(right_on)
    return out_columns


def _merge_match(left, right, left_on, right_on, right_columns):
    merged = dict(left)
    for col in right_columns:
        if col == right_on:
            continue
        merged[col] = right[col]
    if right_on != left_on:
        merged[right_on] = right.get(right_on)
    return merged


def _merge_unmatched(left, left_on, right_on, right_columns):
    merged = dict(left)
    for col in right_columns:
        if col == right_on:
            continue
        merged.setdefault(col, None)
    if right_on != left_on:
        merged.setdefault(right_on, None)
    return merged


def _iter_jsonl_rows(paths):
    for path in paths:
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                yield json.loads(line)


def _iter_row_chunks(paths, budget_bytes):
    """把若干 JSONL 文件按行字节预算切成块（单超大行独占一块）。"""
    chunk = []
    buffered = 0
    for row in _iter_jsonl_rows(paths):
        size = row_size(row)
        if chunk and buffered + size > budget_bytes:
            yield chunk, buffered
            chunk = []
            buffered = 0
        chunk.append(row)
        buffered += size
        if buffered >= budget_bytes:
            yield chunk, buffered
            chunk = []
            buffered = 0
    if chunk:
        yield chunk, buffered


def _files_size(paths) -> int:
    return sum(os.path.getsize(p) for p in paths if os.path.exists(p))


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
        self._sink: BoundedResultSink | None = None

    def run(self, input_files, columns):
        right_path = self.ctx.resolve_source(self.right_source)
        right_columns: list[str] = []
        with open(right_path, "r", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    right_columns = merge_columns(
                        right_columns, [json.loads(line)])

        final_columns = _merged_columns(columns, right_columns, self.right_on)

        output_file = os.path.join(self.ctx.temp_dir, f"join-out-{id(self)}.jsonl")
        spill_dir = os.path.join(self.ctx.temp_dir, f"join-spill-{id(self)}")
        os.makedirs(spill_dir, exist_ok=True)
        self._sink = BoundedResultSink(
            output_file,
            budget_bytes=self.ctx.work_bytes,
            is_cancelled=self.ctx.is_cancelled,
            metrics=self.ctx.metrics,
            slow_write_bytes=self.ctx.slow_write_bytes,
        )
        try:
            self._join_files(
                input_files, right_path, list(columns), right_columns,
                final_columns, depth=0, spill_dir=spill_dir,
                parent_total=None,
            )
            self._sink.close()
        except BaseException:
            self._sink.abort()
            raise
        finally:
            shutil.rmtree(spill_dir, ignore_errors=True)
        return [output_file], final_columns

    def _spill_partitions(self, left_paths, right_path, depth, spill_dir):
        """按哈希的第 depth 个十六进制位（16 桶）把两侧数据分桶落盘。"""
        tag = f"d{depth}-{id(self)}"
        handles: dict[str, dict] = {}
        sizes: dict[str, dict] = {}

        def writer(side, bucket):
            group = handles.setdefault(bucket, {})
            if side not in group:
                path = os.path.join(spill_dir, f"{tag}-{side}-{bucket}.jsonl")
                group[side] = open(path, "w", encoding="utf-8")
                sizes.setdefault(bucket, {})[side] = 0
            return group[side]

        def feed(paths, side, key):
            for path in paths:
                with open(path, "r", encoding="utf-8") as fh:
                    for line in fh:
                        if not line.strip():
                            continue
                        row = json.loads(line)
                        bucket = _key_hash(row.get(key))[depth % 16]
                        fh_out = writer(side, bucket)
                        fh_out.write(line if line.endswith("\n") else line + "\n")
                        sizes.setdefault(bucket, {}).setdefault(side, 0)
                        sizes[bucket][side] += len(line.encode("utf-8"))

        try:
            feed(left_paths, "l", self.left_on)
            if os.path.exists(right_path):
                feed([right_path], "r", self.right_on)
        finally:
            for group in handles.values():
                for fh in group.values():
                    fh.close()

        paths = {}
        for bucket, group in handles.items():
            paths[bucket] = {
                side: os.path.join(spill_dir, f"{tag}-{side}-{bucket}.jsonl")
                for side in group
            }
        return paths, sizes

    def _join_files(self, left_paths, right_path, left_columns,
                    right_columns, final_columns, depth, spill_dir,
                    parent_total):
        self.check_cancelled()
        left_size = _files_size(left_paths)
        right_size = _files_size([right_path])
        total = left_size + right_size

        fits = total <= self.ctx.work_bytes
        # 哈希位用尽，或本次分区几乎没有把数据切小（热键倾斜）：
        # 继续细分同一个键无意义，直接进入分块嵌套循环。
        skewed = (
            parent_total is not None
            and total >= int(parent_total * 0.9)
            and not fits
        )
        if fits or skewed or depth >= 16:
            self._block_join(
                left_paths, right_path, left_columns, right_columns,
            )
            return

        bucket_paths, sizes = self._spill_partitions(
            left_paths, right_path, depth, spill_dir
        )
        for bucket, paths in sorted(bucket_paths.items()):
            left_bucket = [paths["l"]] if "l" in paths else []
            right_bucket = paths.get("r")
            if not left_bucket and self.how != "left":
                self._remove_paths(paths)
                continue
            if right_bucket is None:
                if self.how == "inner":
                    self._remove_paths(paths)
                    continue
                right_bucket = os.path.join(spill_dir, f"empty-{bucket}.jsonl")
                write_jsonl(right_bucket, [], [])
            self._join_files(
                left_bucket, right_bucket, left_columns, right_columns,
                final_columns, depth + 1, spill_dir,
                parent_total=total,
            )
            self._remove_paths(paths)

    @staticmethod
    def _remove_paths(paths) -> None:
        for path in paths.values():
            if os.path.exists(path):
                os.remove(path)

    def _block_join(self, left_paths, right_path, left_columns, right_columns):
        """分块嵌套循环：构建侧每次只留一个预算内的块，逐对逐行流式输出。

        热键下两侧再多重复也只产生逐行乘积，内存占用与块大小成正比，
        与匹配结果总数无关。
        """
        budget = self.ctx.chunk_bytes or max(1, self.ctx.work_bytes // 2)
        budget = max(1, min(budget, self.ctx.work_bytes))

        left_size = _files_size(left_paths)
        right_size = _files_size([right_path])

        if self.how == "inner" and left_size < right_size and left_size > 0:
            self._block_join_build_left(
                left_paths, right_path, right_columns, budget)
            return

        self._block_join_build_right(
            left_paths, right_path, left_columns, right_columns, budget)

    def _emit(self, row) -> None:
        assert self._sink is not None
        self._sink.push(row)

    def _block_join_build_right(self, left_paths, right_path,
                                left_columns, right_columns, budget):
        # left join 需要知道哪些左键在整个右表中出现过；
        # 倾斜分区里不同键很少，非倾斜数据则根本不会走到这里。
        right_keys: set = set()
        if self.how == "left":
            for right in _iter_jsonl_rows([right_path]):
                right_keys.add(repr(right.get(self.right_on)))
            self.ctx.note_peak("join_work", len("".join(right_keys)))

        for chunk, chunk_bytes in _iter_row_chunks([right_path], budget):
            self.check_cancelled()
            index: dict = {}
            for right in chunk:
                index.setdefault(repr(right.get(self.right_on)), []).append(right)
            self.ctx.note_peak("join_work", chunk_bytes)
            for left in _iter_jsonl_rows(left_paths):
                matches = index.get(repr(left.get(self.left_on)))
                if not matches:
                    continue
                for right in matches:  # 多对多：重复匹配全部保留
                    self._emit(_merge_match(
                        left, right, self.left_on, self.right_on,
                        right_columns))

        if self.how == "left":
            for left in _iter_jsonl_rows(left_paths):
                if repr(left.get(self.left_on)) in right_keys:
                    continue
                self._emit(_merge_unmatched(
                    left, self.left_on, self.right_on, right_columns))

    def _block_join_build_left(self, left_paths, right_path,
                               right_columns, budget):
        """inner 连接且左表更小：左表分块建索引，右表逐行探测。"""
        for chunk, chunk_bytes in _iter_row_chunks(left_paths, budget):
            self.check_cancelled()
            index: dict = {}
            for left in chunk:
                index.setdefault(repr(left.get(self.left_on)), []).append(left)
            self.ctx.note_peak("join_work", chunk_bytes)
            for right in _iter_jsonl_rows([right_path]):
                matches = index.get(repr(right.get(self.right_on)))
                if not matches:
                    continue
                for left in matches:
                    self._emit(_merge_match(
                        left, right, self.left_on, self.right_on,
                        right_columns))
