"""算子注册表：算子名 -> 算子类。

之后新增算子只需实现 Operator 子类并调用 register_operator，
批次（Batch）与落盘/发布约定由执行器统一处理，无需改动。
"""

from __future__ import annotations


class OperatorRegistry:
    def __init__(self):
        self._ops: dict[str, type] = {}

    def register(self, name: str, cls: type) -> None:
        if name in self._ops:
            raise ValueError(f"算子已注册: {name}")
        self._ops[name] = cls

    def get(self, name: str) -> type:
        if name not in self._ops:
            raise KeyError(f"未知算子: {name}（已注册: {sorted(self._ops)}）")
        return self._ops[name]

    def names(self) -> list[str]:
        return sorted(self._ops)

    def build(self, name: str, params: dict, ctx):
        return self.get(name)(params, ctx)


registry = OperatorRegistry()


def register_operator(name: str):
    def decorator(cls):
        registry.register(name, cls)
        return cls
    return decorator
