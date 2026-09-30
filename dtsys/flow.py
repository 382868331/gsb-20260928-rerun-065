"""有界结果通道：生产者按内存预算推送结果行，消费者线程落盘。

下游（磁盘/读取方）很慢时，生产者会在预算上限处阻塞，
从而对“在途数据”形成背压：既不靠减少输出回避乘积结果，
也不允许中间结果无界堆积。

口径（字节）与批次一致：行的 JSON 编码长度（见 batches.row_size）。
"""

from __future__ import annotations

import json
import os
import threading
import time

from collections import deque


def encode_row(row: dict) -> bytes:
    return (json.dumps(row, ensure_ascii=False, default=str) + "\n").encode("utf-8")


class BoundedResultSink:
    """单行粒度的有界在途通道。

    - push() 按字节记账，超过预算且仍有未写完的数据时阻塞；
    - 单独的写出线程尽快把通道排空，慢速下游时对生产者形成背压；
    - 单条结果即使超过预算也允许独占通道（保证可推进，不丢输出）；
    - 记录累计推送字节与在途峰值，供运行实例观测预算控制。
    """

    def __init__(self, output_path: str, budget_bytes: int,
                 is_cancelled=None, metrics: dict | None = None,
                 slow_write_bytes: float = 0.0):
        self.output_path = output_path
        self.budget_bytes = max(1, int(budget_bytes))
        self._is_cancelled = is_cancelled or (lambda: False)
        self._metrics = metrics
        self._slow_write_bytes = max(0.0, slow_write_bytes)

        self._cond = threading.Condition()
        self._queue: deque = deque()
        self._inflight = 0
        self._error: Exception | None = None

        self.peak_inflight_bytes = 0
        self.total_output_bytes = 0
        self.row_count = 0

        self._fh = open(output_path + ".tmp", "wb")
        self._thread = threading.Thread(target=self._write_loop, daemon=True)
        self._thread.start()

    def _write_loop(self) -> None:
        try:
            while True:
                with self._cond:
                    while not self._queue:
                        if self._is_cancelled():
                            return
                        self._cond.wait(timeout=0.1)
                    item = self._queue.popleft()
                if item is None:
                    self._fh.flush()
                    return
                self._fh.write(item)
                if self._slow_write_bytes:
                    end = time.monotonic() + len(item) / self._slow_write_bytes
                    while time.monotonic() < end:
                        if self._is_cancelled():
                            return
                        time.sleep(min(0.005, max(0.0, end - time.monotonic())))
                with self._cond:
                    self._inflight -= len(item)
                    self._cond.notify_all()
        except Exception as exc:
            with self._cond:
                self._error = exc
                self._cond.notify_all()
        finally:
            try:
                self._fh.close()
            except Exception:
                pass

    def _raise_if_bad(self) -> None:
        if self._error is not None:
            raise self._error
        if self._is_cancelled():
            raise RuntimeError("运行已取消")

    def push(self, row: dict) -> None:
        line = encode_row(row)
        size = len(line)
        with self._cond:
            while self._inflight > 0 and self._inflight + size > self.budget_bytes:
                self._raise_if_bad()
                self._cond.wait(timeout=0.1)
            self._raise_if_bad()
            self._inflight += size
            if self._inflight > self.peak_inflight_bytes:
                self.peak_inflight_bytes = self._inflight
            self._queue.append(line)
            self._cond.notify_all()
        self.total_output_bytes += size
        self.row_count += 1

    def close(self) -> None:
        """等待全部在途数据落盘并原子替换为最终文件。"""
        with self._cond:
            self._raise_if_bad()
            self._queue.append(None)
            self._cond.notify_all()
        self._thread.join()
        with self._cond:
            if self._error is not None:
                self.abort()
                raise self._error
        os.replace(self.output_path + ".tmp", self.output_path)
        if self._metrics is not None:
            prev = int(self._metrics.get("peak_inflight_bytes", 0))
            self._metrics["peak_inflight_bytes"] = max(
                prev, self.peak_inflight_bytes)
            self._metrics["total_output_bytes"] = (
                int(self._metrics.get("total_output_bytes", 0))
                + self.total_output_bytes)

    def abort(self) -> None:
        """取消或失败时：丢弃在途数据并清理临时文件。"""
        with self._cond:
            self._queue.clear()
            self._inflight = 0
            self._queue.append(None)
            self._cond.notify_all()
        self._thread.join()
        for path in (self.output_path + ".tmp", self.output_path):
            if os.path.exists(path):
                os.remove(path)

