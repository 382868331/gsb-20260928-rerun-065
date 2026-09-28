"""算子基类与执行上下文。

算子以批次为处理单位：streaming 算子实现 process_batch，逐批处理；
需要全量数据的阻塞算子实现 run（如连接）。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from ..batches import Batch


@dataclass
class OperatorContext:
    run_dir: str
    temp_dir: str
    work_bytes: int
    resolve_source: Callable[[str], str]
    is_cancelled: Callable[[], bool]


class Operator:
    streaming = True

    def __init__(self, params: dict, ctx: OperatorContext):
        self.params = params
        self.ctx = ctx

    def check_cancelled(self) -> None:
        if self.ctx.is_cancelled():
            raise RuntimeError("运行已取消")

    def process_batch(self, batch: Batch) -> Batch:
        raise NotImplementedError

    def run(self, input_files: list[str], columns: list[str]):
        """阻塞算子：消费全部落盘分块，返回 (输出分块文件列表, 输出列)。"""
        raise NotImplementedError
