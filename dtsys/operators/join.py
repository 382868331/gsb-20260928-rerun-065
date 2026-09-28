"""连接算子：分块 + 临时落盘的递归分区哈希连接。

保留全部重复匹配（多对多）：同一键的每个左行 × 每个右行都输出。
工作集超过内存预算时，按哈希把两侧数据递归分区，
直到某个分区可放入内存再做哈希探测；任何一侧都无需整体驻留内存。

params:
  right_source: 右数据源名称（左输入为上一阶段结果）
  left_on / right_on: 连接键（右键可缺省，表示同名）
  how: inner（默认）或 left
"""

from __future__ import annotations

import hashlib
import json
import os

from ..batches import merge_columns, read_jsonl, write_jsonl, row_size
from .base import Operator
from .registry import register_operator


def _key_hash(value) -> str:
    return hashlib.blake2b(repr(value).encode("utf-8"), digest_size=16).hexdigest()


def _join_rows(left_rows, right_rows, left_on, right_on, right_columns, how):
    index: dict = {}
    for row in right_rows:
        index.setdefault(repr(row.get(right_on)), []).append(row)

    out_columns: list[str] = []
    for row in left_rows:
        for key in row:
            if key not in out_columns:
                out_columns.append(key)
    for col in right_columns:
        if col != right_on and col not in out_columns:
            out_columns.append(col)
    if right_on not in out_columns:
        out_columns.append(right_on)

    results = []
    for left in left_rows:
        matches = index.get(repr(left.get(left_on)))
        if matches:
            for right in matches:  # 多对多：重复匹配全部保留
                merged = dict(left)
                for col in right_columns:
                    if col == right_on:
                        continue
                    merged[col] = right[col]
                if right_on != left_on:
                    merged[right_on] = right.get(right_on)
                results.append(merged)
        elif how == "left":
            merged = dict(left)
            for col in right_columns:
                if col == right_on:
                    continue
                merged.setdefault(col, None)
            if right_on != left_on:
                merged.setdefault(right_on, None)
            results.append(merged)
    return results, out_columns


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

    def run(self, input_files, columns):
        right_path = self.ctx.resolve_source(self.right_source)
        right_columns: list[str] = []
        for row in read_jsonl(right_path):
            right_columns = merge_columns(right_columns, [row])

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
        out_fh = open(output_file + ".tmp", "w", encoding="utf-8")
        try:
            self._join_files(
                input_files, right_path, right_columns, final_columns,
                depth=0, spill_dir=spill_dir, out_fh=out_fh,
            )
        finally:
            out_fh.close()
        os.replace(output_file + ".tmp", output_file)
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
                        writer(side, bucket).write(line if line.endswith("\n")
                                                   else line + "\n")
                        sizes.setdefault(bucket, {}).setdefault(side, 0)
                        sizes[bucket][side] += len(line.encode("utf-8"))

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
        return paths, sizes

    def _join_files(self, left_paths, right_path, right_columns,
                    final_columns, depth, spill_dir, out_fh):
        self.check_cancelled()
        left_size = sum(
            os.path.getsize(p) for p in left_paths if os.path.exists(p)
        )
        right_size = os.path.getsize(right_path) if os.path.exists(right_path) else 0

        if left_size + right_size <= self.ctx.work_bytes or depth >= 16:
            left_rows = []
            for path in left_paths:
                if os.path.exists(path):
                    left_rows.extend(read_jsonl(path))
            right_rows = read_jsonl(right_path) if os.path.exists(right_path) else []
            rows, _ = _join_rows(
                left_rows, right_rows, self.left_on, self.right_on,
                right_columns, self.how,
            )
            for row in rows:
                out_fh.write(json.dumps(row, ensure_ascii=False, default=str))
                out_fh.write("\n")
            return

        bucket_paths, sizes = self._spill_partitions(
            left_paths, right_path, depth, spill_dir
        )
        for bucket, paths in sorted(bucket_paths.items()):
            left_bucket = [paths["l"]] if "l" in paths else []
            right_bucket = paths.get("r")
            bucket_total = sizes[bucket].get("l", 0) + sizes[bucket].get("r", 0)
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
