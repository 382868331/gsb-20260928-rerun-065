from __future__ import annotations

import ast
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator


@dataclass
class Batch:
    rows: list[dict[str, Any]]
    estimated_bytes: int


def row_size(row: dict[str, Any]) -> int:
    return len(json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def make_batch(rows: list[dict[str, Any]]) -> Batch:
    return Batch(rows=rows, estimated_bytes=sum(row_size(row) for row in rows))


def read_jsonl_batches(
    path: str | Path, *, memory_bytes: int | None, target_ratio: float = 0.35
) -> Iterator[Batch]:
    limit = None
    if memory_bytes is not None:
        limit = max(1, int(memory_bytes * target_ratio))
    rows: list[dict[str, Any]] = []
    size = 0
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            current = row_size(row)
            if rows and limit is not None and size + current > limit:
                yield Batch(rows, size)
                rows = []
                size = 0
            rows.append(row)
            size += current
    if rows:
        yield Batch(rows, size)

def write_jsonl_rows(handle, rows: list[dict[str, Any]]) -> None:
    for row in rows:
        handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
        handle.write("\n")


_SAFE_FUNCTIONS = {
    "abs": abs,
    "round": round,
    "min": min,
    "max": max,
    "str": str,
    "int": int,
    "float": float,
    "len": len,
}


class SafeExpressionEvaluator(ast.NodeVisitor):
    def __init__(self, names: dict[str, Any]):
        self.names = names

    def evaluate(self, expression: str) -> Any:
        return self.visit(ast.parse(expression, mode="eval").body)

    def visit_Name(self, node: ast.Name) -> Any:
        if node.id not in self.names:
            raise NameError(f"unknown field: {node.id}")
        return self.names[node.id]

    def visit_Constant(self, node: ast.Constant) -> Any:
        return node.value

    def visit_BinOp(self, node: ast.BinOp) -> Any:
        left = self.visit(node.left)
        right = self.visit(node.right)
        operators = {
            ast.Add: lambda a, b: a + b,
            ast.Sub: lambda a, b: a - b,
            ast.Mult: lambda a, b: a * b,
            ast.Div: lambda a, b: a / b,
            ast.FloorDiv: lambda a, b: a // b,
            ast.Mod: lambda a, b: a % b,
        }
        return operators[type(node.op)](left, right)

    def visit_UnaryOp(self, node: ast.UnaryOp) -> Any:
        value = self.visit(node.operand)
        if isinstance(node.op, ast.USub):
            return -value
        if isinstance(node.op, ast.UAdd):
            return +value
        raise ValueError("unsupported unary operator")

    def visit_Compare(self, node: ast.Compare) -> bool:
        left = self.visit(node.left)
        for operator, comparator_node in zip(node.ops, node.comparators):
            right = self.visit(comparator_node)
            if isinstance(operator, ast.Eq):
                result = left == right
            elif isinstance(operator, ast.NotEq):
                result = left != right
            elif isinstance(operator, ast.Gt):
                result = left > right
            elif isinstance(operator, ast.GtE):
                result = left >= right
            elif isinstance(operator, ast.Lt):
                result = left < right
            elif isinstance(operator, ast.LtE):
                result = left <= right
            else:
                raise ValueError("unsupported comparison")
            if not result:
                return False
            left = right
        return True

    def visit_BoolOp(self, node: ast.BoolOp) -> bool:
        values = [self.visit(value) for value in node.values]
        return all(values) if isinstance(node.op, ast.And) else any(values)

    def visit_IfExp(self, node: ast.IfExp) -> Any:
        return self.visit(node.body) if self.visit(node.test) else self.visit(node.orelse)

    def visit_Call(self, node: ast.Call) -> Any:
        if not isinstance(node.func, ast.Name) or node.func.id not in _SAFE_FUNCTIONS:
            raise ValueError("unsupported function")
        return _SAFE_FUNCTIONS[node.func.id](*(self.visit(arg) for arg in node.args))

    def generic_visit(self, node: ast.AST) -> Any:
        raise ValueError(f"unsupported expression node: {type(node).__name__}")
