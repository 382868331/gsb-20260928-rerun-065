"""批次与临时落盘的统一约定。

所有算子都以 Batch（行字典列表 + 有序列名）为输入输出粒度；
中间数据一律以 JSONL 分块落盘到本次运行的临时目录。
新增算子只要遵守这一约定，即可自动复用分块、落盘与发布机制。
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Iterator


def row_size(row: dict) -> int:
    """单行的近似内存占用（JSON 字节数，作为内存预算口径）。"""
    return len(json.dumps(row, ensure_ascii=False, default=str))


@dataclass
class Batch:
    rows: list[dict]
    columns: list[str] = field(default_factory=list)

    @property
    def size(self) -> int:
        return sum(row_size(row) for row in self.rows)


def merge_columns(columns: list[str], rows) -> list[str]:
    result = list(columns)
    known = set(result)
    for row in rows:
        for key in row:
            if key not in known:
                known.add(key)
                result.append(key)
    return result


def read_jsonl(path: str) -> list[dict]:
    rows = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def write_jsonl(path: str, rows, columns: list[str] | None = None) -> None:
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, default=str))
            fh.write("\n")
    os.replace(tmp_path, path)


def jsonl_batches(path: str, target_bytes: int) -> Iterator[Batch]:
    """按目标字节数把 JSONL 文件切分为 Batch 流。"""
    if target_bytes <= 0:
        raise ValueError("target_bytes 必须为正数")
    batch_rows: list[dict] = []
    columns: list[str] = []
    buffered = 0
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            columns = merge_columns(columns, [row])
            batch_rows.append(row)
            buffered += row_size(row)
            if buffered >= target_bytes:
                yield Batch(batch_rows, columns)
                batch_rows = []
                buffered = 0
    if batch_rows:
        yield Batch(batch_rows, columns)
