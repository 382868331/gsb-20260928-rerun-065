"""分组聚合算子：有界外部哈希聚合。

支持 count / count(field) / sum(field) / min(field) / max(field) / avg(field)。
状态超过工作预算时按分组键递归分桶；单个分组的聚合状态为常量大小。
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil

from ..batches import row_size
from ..expr import Expression
from .base import Operator
from .registry import register_operator


MAX_PARTITION_DEPTH = 16


def _group_key(row, keys):
    return tuple(row.get(key) for key in keys)


def _key_hash(key) -> str:
    data = json.dumps(key, ensure_ascii=False, default=str, sort_keys=True)
    return hashlib.blake2b(data.encode("utf-8"), digest_size=16).hexdigest()


@register_operator("aggregate")
class AggregateOperator(Operator):
    streaming = False

    def __init__(self, params: dict, ctx):
        super().__init__(params, ctx)
        self.group_by = list(params.get("group_by", []))
        self.aggregations = dict(params.get("aggregations", {}))
        if not self.aggregations:
            raise ValueError("aggregate 至少需要一个 aggregation")
        self.funcs = {
            name: self._parse_aggregation(expr)
            for name, expr in self.aggregations.items()
        }
        self.peak_inflight_bytes = 0

    @staticmethod
    def _parse_aggregation(expr: str):
        text = expr.strip()
        if text in ("count", "count(*)"):
            return ("count", None)
        if "(" not in text or not text.endswith(")"):
            raise ValueError(f"不支持的聚合表达式: {expr}")
        name, raw_arg = text[:-1].split("(", 1)
        name = name.strip()
        arg = raw_arg.strip()
        if name not in {"count", "sum", "min", "max", "avg"}:
            raise ValueError(f"不支持的聚合函数: {name}")
        if not arg or arg == "*":
            if name != "count":
                raise ValueError(f"{name} 需要字段参数")
            return ("count", None)
        return (name, Expression(arg))

    def _columns(self):
        return self.group_by + list(self.aggregations)

    def _note_inflight(self, num_bytes: int) -> None:
        if num_bytes > self.peak_inflight_bytes:
            self.peak_inflight_bytes = num_bytes
        stats = self.ctx.stats
        prev = stats.get("aggregate_peak_inflight_bytes", 0)
        if num_bytes > prev:
            stats["aggregate_peak_inflight_bytes"] = num_bytes

    def run(self, input_files, columns):
        output_file = os.path.join(
            self.ctx.temp_dir, f"aggregate-out-{id(self)}.jsonl"
        )
        spill_dir = os.path.join(
            self.ctx.temp_dir, f"aggregate-spill-{id(self)}"
        )
        os.makedirs(spill_dir, exist_ok=True)
        self.peak_inflight_bytes = 0
        try:
            with open(output_file + ".tmp", "w", encoding="utf-8") as out_fh:
                self._aggregate_files(
                    input_files, depth=0, spill_dir=spill_dir, out_fh=out_fh
                )
            os.replace(output_file + ".tmp", output_file)
        except BaseException:
            tmp_path = output_file + ".tmp"
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            shutil.rmtree(spill_dir, ignore_errors=True)
            raise
        return [output_file], self._columns()

    def _iter_rows(self, paths):
        for path in paths:
            if not os.path.exists(path):
                continue
            with open(path, "r", encoding="utf-8") as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    yield json.loads(line)

    def _initial_state(self):
        return {
            name: (0 if func == "count" else None)
            for name, (func, _field) in self.funcs.items()
        }

    def _accumulate(self, state, row):
        for name, (func, field) in self.funcs.items():
            if func == "count":
                value = field(row) if isinstance(field, Expression) else None
                if field is None or value is not None:
                    state[name] += 1
                continue
            value = field(row) if isinstance(field, Expression) else row.get(field)
            if value is None:
                continue
            if func == "sum":
                state[name] = value if state[name] is None else state[name] + value
            elif func == "avg":
                if state[name] is None:
                    state[name] = [value, 1]
                else:
                    state[name][0] += value
                    state[name][1] += 1
            elif state[name] is None:
                state[name] = value
            elif func == "min":
                state[name] = min(state[name], value)
            elif func == "max":
                state[name] = max(state[name], value)

    def _finalize_state(self, key, state):
        row = dict(zip(self.group_by, key))
        for name, (func, _field) in self.funcs.items():
            value = state[name]
            if func == "avg":
                value = None if value is None else value[0] / value[1]
            row[name] = value
        return row

    def _state_bytes(self, states):
        total = 0
        for key, state in states.items():
            row = dict(zip(self.group_by, key))
            row.update(state)
            total += row_size(row)
        return total

    def _aggregate_files(self, paths, depth, spill_dir, out_fh):
        self.check_cancelled()
        try:
            self._aggregate_in_memory(paths, out_fh)
            return
        except _AggregateStateTooLarge:
            if depth >= MAX_PARTITION_DEPTH:
                self._aggregate_in_memory(
                    paths, out_fh, allow_unbounded_single_partition=True
                )
                return
        bucket_paths = self._spill_partitions(paths, depth, spill_dir)
        for bucket in sorted(bucket_paths):
            path = bucket_paths[bucket]
            self._aggregate_files([path], depth + 1, spill_dir, out_fh)
            if os.path.exists(path):
                os.remove(path)

    def _aggregate_in_memory(self, paths, out_fh,
                             allow_unbounded_single_partition=False):
        states = {}
        saw_row = False
        for row in self._iter_rows(paths):
            saw_row = True
            key = _group_key(row, self.group_by)
            projected_bytes = self._state_bytes(states) + row_size(row)
            if (projected_bytes >= self.ctx.work_bytes
                    and (states or key in states)
                    and not allow_unbounded_single_partition):
                self._note_inflight(self._state_bytes(states))
                raise _AggregateStateTooLarge
            state = states.get(key)
            if state is None:
                state = self._initial_state()
                states[key] = state
            self._accumulate(state, row)
            state_bytes = self._state_bytes(states)
            self._note_inflight(state_bytes)
        if not saw_row and not self.group_by:
            states[()] = self._initial_state()
        for key in sorted(states, key=lambda item: json.dumps(
                item, ensure_ascii=False, default=str, sort_keys=True)):
            row = self._finalize_state(key, states[key])
            out_fh.write(json.dumps(row, ensure_ascii=False, default=str))
            out_fh.write("\n")
        self._note_inflight(0)

    def _spill_partitions(self, paths, depth, spill_dir):
        tag = f"aggregate-d{depth}-{id(self)}"
        handles = {}
        paths_by_bucket = {}
        try:
            for row in self._iter_rows(paths):
                key = _group_key(row, self.group_by)
                bucket = _key_hash(key)[depth % 16]
                fh = handles.get(bucket)
                if fh is None:
                    path = os.path.join(spill_dir, f"{tag}-{bucket}.jsonl")
                    paths_by_bucket[bucket] = path
                    fh = open(path, "w", encoding="utf-8")
                    handles[bucket] = fh
                fh.write(json.dumps(row, ensure_ascii=False, default=str))
                fh.write("\n")
        finally:
            for fh in handles.values():
                fh.close()
        return paths_by_bucket


class _AggregateStateTooLarge(RuntimeError):
    pass
