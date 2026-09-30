"""安全的行表达式求值（白名单 AST），用于筛选条件与字段推导。

表达式中直接写字段名即引用当前行的字段值，例如：
    amount >= 100 and level == "VIP"
    round(amount * 1.1, 2)
不提供任意属性访问 / 导入能力。
"""

from __future__ import annotations

import ast


class ExpressionError(ValueError):
    pass


_ALLOWED_FUNCS = {
    "round": round,
    "abs": abs,
    "len": len,
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "min": min,
    "max": max,
}

_BIN_OPS = {
    ast.Add: lambda a, b: None if a is None or b is None else a + b,
    ast.Sub: lambda a, b: None if a is None or b is None else a - b,
    ast.Mult: lambda a, b: None if a is None or b is None else a * b,
    ast.Div: lambda a, b: None if a is None or b is None else a / b,
    ast.FloorDiv: lambda a, b: None if a is None or b is None else a // b,
    ast.Mod: lambda a, b: None if a is None or b is None else a % b,
}

_CMP_OPS = {
    ast.Eq: lambda a, b: a == b,
    ast.NotEq: lambda a, b: a != b,
    ast.Lt: lambda a, b: a < b,
    ast.LtE: lambda a, b: a <= b,
    ast.Gt: lambda a, b: a > b,
    ast.GtE: lambda a, b: a >= b,
    ast.In: lambda a, b: a in b,
    ast.NotIn: lambda a, b: a not in b,
}


class Expression:
    """编译一次、可对多行重复求值的安全表达式。"""

    def __init__(self, source: str):
        self.source = source
        try:
            tree = ast.parse(source, mode="eval")
        except SyntaxError as exc:
            raise ExpressionError(f"表达式语法错误: {source!r}: {exc}") from exc
        self._node = self._compile_node(tree.body)

    @classmethod
    def _compile_node(cls, node: ast.AST):
        if isinstance(node, ast.BoolOp):
            values = [cls._compile_node(v) for v in node.values]
            if isinstance(node.op, ast.And):
                def evaluate(row):
                    result = True
                    for fn in values:
                        result = fn(row)
                        if not result:
                            return False
                    return result
                return evaluate
            def evaluate_or(row):
                for fn in values:
                    if fn(row):
                        return True
                return False
            return evaluate_or
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.Not, ast.USub, ast.UAdd)):
            value = cls._compile_node(node.operand)
            if isinstance(node.op, ast.Not):
                return lambda row: not value(row)
            if isinstance(node.op, ast.USub):
                return lambda row: -value(row)
            return value
        if isinstance(node, ast.BinOp) and type(node.op) in _BIN_OPS:
            left = cls._compile_node(node.left)
            right = cls._compile_node(node.right)
            op = _BIN_OPS[type(node.op)]
            return lambda row: op(left(row), right(row))
        if isinstance(node, ast.Compare) and len(node.ops) == 1:
            cmp_type = type(node.ops[0])
            if cmp_type not in _CMP_OPS:
                raise ExpressionError("不支持的比较运算")
            left = cls._compile_node(node.left)
            right = cls._compile_node(node.comparators[0])
            op = _CMP_OPS[cmp_type]
            return lambda row: op(left(row), right(row))
        if isinstance(node, ast.IfExp):
            test = cls._compile_node(node.test)
            body = cls._compile_node(node.body)
            orelse = cls._compile_node(node.orelse)
            return lambda row: body(row) if test(row) else orelse(row)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id not in _ALLOWED_FUNCS or node.keywords:
                raise ExpressionError(f"不允许的函数调用: {ast.dump(node)}")
            func = _ALLOWED_FUNCS[node.func.id]
            args = [cls._compile_node(a) for a in node.args]
            return lambda row: func(*(a(row) for a in args))
        if isinstance(node, ast.Name):
            return lambda row: row[node.id]
        if isinstance(node, ast.Constant):
            value = node.value
            if not isinstance(value, (str, int, float, bool, type(None))):
                raise ExpressionError("不支持的常量类型")
            return lambda row: value
        raise ExpressionError(f"不允许的表达式节点: {type(node).__name__}")

    def evaluate(self, row: dict):
        return self._node(row)

    def __call__(self, row: dict):
        return self._node(row)

    def __repr__(self):
        return f"Expression({self.source!r})"
