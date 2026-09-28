"""字段推导算子：按表达式新增或覆盖字段。

params: {fields: {字段名: 表达式}, keep: [保留列, 可选]}
"""

from __future__ import annotations

from ..batches import Batch, merge_columns
from ..expr import Expression
from .base import Operator
from .registry import register_operator


@register_operator("derive")
class DeriveOperator(Operator):
    def __init__(self, params: dict, ctx):
        super().__init__(params, ctx)
        self.fields = {
            name: Expression(expr) for name, expr in params["fields"].items()
        }
        self.keep = params.get("keep")

    def process_batch(self, batch: Batch) -> Batch:
        self.check_cancelled()
        new_rows = []
        for row in batch.rows:
            if self.keep is not None:
                new_row = {key: row.get(key) for key in self.keep if key in row}
            else:
                new_row = dict(row)
            for name, expr in self.fields.items():
                new_row[name] = expr(row)
            new_rows.append(new_row)
        columns = list(self.keep) if self.keep is not None else list(batch.columns)
        columns = merge_columns(columns, new_rows)
        for name in self.fields:
            if name not in columns:
                columns.append(name)
        return Batch(new_rows, columns)
