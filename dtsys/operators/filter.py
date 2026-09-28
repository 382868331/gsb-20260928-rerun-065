"""筛选算子：按布尔表达式保留行。"""

from __future__ import annotations

from ..batches import Batch
from ..expr import Expression
from .base import Operator
from .registry import register_operator


@register_operator("filter")
class FilterOperator(Operator):
    def __init__(self, params: dict, ctx):
        super().__init__(params, ctx)
        self.expr = Expression(params["expr"])

    def process_batch(self, batch: Batch) -> Batch:
        self.check_cancelled()
        kept = [row for row in batch.rows if self.expr(row)]
        return Batch(kept, batch.columns)
