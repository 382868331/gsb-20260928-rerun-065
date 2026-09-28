from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .records import read_jsonl_batches, write_jsonl_rows


@dataclass(frozen=True)
class ExecutionContext:
    run_id: str
    temp_dir: Path
    memory_bytes: int | None


class Operator(Protocol):
    type_name: str

    def execute(self, params, inputs, output, context):
        ...


class FilterOperator:
    type_name = "filter"

    def execute(self, params, inputs, output, context):
        from .records import SafeExpressionEvaluator

        predicate = params["expression"]
        with output.open("w", encoding="utf-8") as out:
            for batch in read_jsonl_batches(inputs["input"], memory_bytes=context.memory_bytes):
                kept = [row for row in batch.rows if SafeExpressionEvaluator(row).evaluate(predicate)]
                write_jsonl_rows(out, kept)


class DeriveOperator:
    type_name = "derive"

    def execute(self, params, inputs, output, context):
        from .records import SafeExpressionEvaluator

        fields = params["fields"]
        with output.open("w", encoding="utf-8") as out:
            for batch in read_jsonl_batches(inputs["input"], memory_bytes=context.memory_bytes):
                changed = []
                for row in batch.rows:
                    result = dict(row)
                    for name, expression in fields.items():
                        result[name] = SafeExpressionEvaluator(result).evaluate(expression)
                    changed.append(result)
                write_jsonl_rows(out, changed)


def _join_key(row, keys):
    return tuple(row[key] for key in keys)


def _hash_join_key(key):
    return hash(json.dumps(key, ensure_ascii=False, sort_keys=True))


def _merge_right(row, right_row, suffix):
    result = dict(row)
    for key, value in right_row.items():
        target = f"{key}{suffix}" if key in result and result[key] != value else key
        result[target] = value
    return result


class JoinOperator:
    type_name = "join"

    def execute(self, params, inputs, output, context):
        keys = list(params["keys"])
        how = params.get("how", "inner")
        suffix = params.get("right_suffix", "_right")
        if how not in {"inner", "left"}:
            raise ValueError(f"unsupported join type: {how}")
        join_temp = context.temp_dir / f"join-{_safe_name(output.stem)}"
        join_temp.mkdir(parents=True, exist_ok=True)
        budget = context.memory_bytes // 2 if context.memory_bytes is not None else None
        with output.open("w", encoding="utf-8") as out:
            self._process(inputs["left"], inputs["right"], keys, how, suffix, budget, join_temp, 0, out)

    def _process(self, left, right, keys, how, suffix, budget, temp_root, depth, out):
        if budget is None or self._pair_size(left, right) <= budget:
            write_jsonl_rows(out, self._in_memory_join(left, right, keys, how, suffix, budget))
            return
        if depth >= 2:
            self._nested_loop_join(left, right, keys, how, suffix, out)
            return
        count = 8 * (8 ** depth)
        left_buckets = self._partition(left, keys, count, temp_root / f"l{depth}")
        right_buckets = self._partition(right, keys, count, temp_root / f"r{depth}")
        for bucket_id in sorted(set(left_buckets) | set(right_buckets)):
            next_left = left_buckets.get(bucket_id) or self._empty_file(temp_root, f"missing-l-{bucket_id}")
            next_right = right_buckets.get(bucket_id) or self._empty_file(temp_root, f"missing-r-{bucket_id}")
            self._process(next_left, next_right, keys, how, suffix, budget, temp_root / bucket_id, depth + 1, out)

    def _in_memory_join(self, left_path, right_path, keys, how, suffix, budget):
        left = self._index(left_path, keys, budget)
        right = self._index(right_path, keys, budget)
        rows = []
        for key, left_rows in left.items():
            right_rows = right.get(key, [])
            if right_rows:
                rows.extend(_merge_right(l, r, suffix) for l in left_rows for r in right_rows)
            elif how == "left":
                rows.extend(dict(row) for row in left_rows)
        return rows

    def _nested_loop_join(self, left_path, right_path, keys, how, suffix, out):
        for left_batch in read_jsonl_batches(left_path, memory_bytes=1):
            for left_row in left_batch.rows:
                matched = False
                for right_batch in read_jsonl_batches(right_path, memory_bytes=1):
                    for right_row in right_batch.rows:
                        if _join_key(left_row, keys) == _join_key(right_row, keys):
                            matched = True
                            write_jsonl_rows(out, [_merge_right(left_row, right_row, suffix)])
                if how == "left" and not matched:
                    write_jsonl_rows(out, [dict(left_row)])

    def _index(self, path, keys, budget):
        groups = defaultdict(list)
        for batch in read_jsonl_batches(path, memory_bytes=budget):
            for row in batch.rows:
                groups[_join_key(row, keys)].append(row)
        return groups

    def _partition(self, path, keys, count, root):
        root.mkdir(parents=True, exist_ok=True)
        handles = {str(i): (root / f"{i}.jsonl").open("w", encoding="utf-8") for i in range(count)}
        try:
            for batch in read_jsonl_batches(path, memory_bytes=1):
                for row in batch.rows:
                    bucket_id = str(_hash_join_key(_join_key(row, keys)) % count)
                    write_jsonl_rows(handles[bucket_id], [row])
        finally:
            for handle in handles.values():
                handle.close()
        return {item.stem: item for item in root.glob("*.jsonl") if item.stat().st_size > 0}

    def _pair_size(self, left, right):
        return left.stat().st_size + (right.stat().st_size if right.exists() else 0)

    def _empty_file(self, root, name):
        root.mkdir(parents=True, exist_ok=True)
        path = root / f"{name}.jsonl"
        path.touch(exist_ok=True)
        return path


def _safe_name(value):
    return "".join(char if char.isalnum() or char in "._-" else "_" for char in value)


OPERATORS = {"filter": FilterOperator(), "derive": DeriveOperator(), "join": JoinOperator()}
