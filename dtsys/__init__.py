"""数据转换执行系统核心包。

模块划分：
- plan:    转换计划（与运行实例分开保存）
- batches: 统一的批次与落盘约定
- operators: 算子基类、注册表与内置算子（筛选 / 字段推导 / 连接）
- runner:  执行器，负责运行实例、内存预算、取消与原子发布
"""

from .plan import TransformPlan, OperatorSpec, PlanStore
from .batches import Batch, jsonl_batches, row_size
from .operators.base import Operator, OperatorContext
from .operators.registry import registry, register_operator
from .runner import Runner, RunConfig, RunStatus, CancellationToken
from .streamwin import StreamEngine, WindowSpec

__all__ = [
    "TransformPlan",
    "OperatorSpec",
    "PlanStore",
    "Batch",
    "jsonl_batches",
    "row_size",
    "Operator",
    "OperatorContext",
    "registry",
    "register_operator",
    "Runner",
    "RunConfig",
    "RunStatus",
    "CancellationToken",
    "StreamEngine",
    "WindowSpec",
]
