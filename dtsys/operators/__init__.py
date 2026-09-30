"""内置算子集合。新增算子时在此导入即可完成注册。"""

from .registry import registry, register_operator
from .base import Operator, OperatorContext
from .filter import FilterOperator
from .derive import DeriveOperator
from .join import JoinOperator
from .aggregate import AggregateOperator

__all__ = [
    "registry",
    "register_operator",
    "Operator",
    "OperatorContext",
    "FilterOperator",
    "DeriveOperator",
    "JoinOperator",
    "AggregateOperator",
]
