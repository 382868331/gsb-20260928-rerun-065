"""单元/集成测试：批次约定、多对多连接、取消与失败清理、发布不可变。"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dtsys import OperatorSpec, Runner, RunConfig, CancellationToken, TransformPlan


def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False))
            fh.write("\n")


def read_jsonl(path):
    with open(path, "r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def make_plan(orders, customers, join_how="inner"):
    tmp = tempfile.mkdtemp()
    orders_path = os.path.join(tmp, "orders.jsonl")
    customers_path = os.path.join(tmp, "customers.jsonl")
    write_jsonl(orders_path, orders)
    write_jsonl(customers_path, customers)
    plan = TransformPlan(
        plan_id="p", name="t", version=1,
        primary_source="orders",
        sources={"orders": orders_path, "customers": customers_path},
        stages=[
            OperatorSpec("filter", {"expr": "v >= 2"}),
            OperatorSpec("derive", {"fields": {"d": "v * 10"}}),
            OperatorSpec("join", {"right_source": "customers",
                                  "left_on": "k", "right_on": "k",
                                  "how": join_how}),
        ],
    )
    return tmp, plan


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.runner = Runner(os.path.join(self.tmp, "store"))

    def _execute(self, plan, run_id, memory_bytes=1 << 16, chunk_bytes=None):
        return self.runner.execute(
            plan, RunConfig(run_id=run_id, memory_bytes=memory_bytes,
                            chunk_bytes=chunk_bytes))

    def test_many_to_many_join_preserves_duplicates(self):
        orders = [{"id": i % 3 + 1, "k": "x", "v": i + 1} for i in range(6)]
        customers = [{"k": "x", "info": "A"}, {"k": "x", "info": "B"}]
        tmp, plan = make_plan(orders, customers)
        self._execute(plan, "r1", memory_bytes=128, chunk_bytes=64)
        rows = read_jsonl(os.path.join(
            self.runner.run_dir("p", "r1"), "output", "result.jsonl"))
        self.assertEqual(len(rows), 10)  # 5 个通过筛选的左行 × 2 个右键
        self.assertEqual(
            sorted(r["info"] for r in rows).count("A"), 5)
        shuffled = self._execute(plan, "r2", memory_bytes=1 << 20)
        rows2 = read_jsonl(os.path.join(
            self.runner.run_dir("p", "r2"), "output", "result.jsonl"))
        key = lambda rs: sorted(json.dumps(r, sort_keys=True) for r in rs)
        self.assertEqual(key(rows), key(rows2))

    def test_tiny_memory_matches_normal(self):
        orders = [{"id": i, "k": f"k{i % 5}", "v": i} for i in range(40)]
        customers = [{"k": f"k{j}", "info": j} for j in range(7)]
        tmp, plan = make_plan(orders, customers)
        self._execute(plan, "big", memory_bytes=1 << 20)
        self._execute(plan, "tiny", memory_bytes=200, chunk_bytes=100)
        big = read_jsonl(os.path.join(self.runner.run_dir("p", "big"),
                                     "output", "result.jsonl"))
        tiny = read_jsonl(os.path.join(self.runner.run_dir("p", "tiny"),
                                      "output", "result.jsonl"))
        key = lambda rs: sorted(json.dumps(r, sort_keys=True) for r in rs)
        self.assertEqual(key(big), key(tiny))
        self.assertEqual(len(big), 38)  # v>=2 的 38 行，每个键只有一个客户

    def test_cancellation_cleans_tmp_and_keeps_published(self):
        orders = [{"id": i, "k": "x", "v": i} for i in range(30)]
        customers = [{"k": "x", "info": "a"}]
        tmp, plan = make_plan(orders, customers)
        self._execute(plan, "good", memory_bytes=1 << 20)
        token = CancellationToken()
        token.cancel()
        with self.assertRaises(RuntimeError):
            self.runner.execute(plan, RunConfig(run_id="cancelled",
                                                memory_bytes=200,
                                                chunk_bytes=80), token)
        run_dir = self.runner.run_dir("p", "cancelled")
        self.assertFalse(os.path.exists(os.path.join(run_dir, "tmp")))
        self.assertFalse(os.path.exists(os.path.join(run_dir, "output")))
        instance = self.runner.get_instance("p", "cancelled")
        self.assertEqual(instance["status"], "cancelled")
        good = self.runner.get_instance("p", "good")
        self.assertEqual(good["status"], "published")
        self.assertTrue(os.path.exists(
            os.path.join(self.runner.run_dir("p", "good"),
                         "output", "result.jsonl")))

    def test_failure_cleans_tmp(self):
        orders = [{"k": "x", "v": 3}]
        customers = [{"k": "x", "info": "a"}]
        tmp, plan = make_plan(orders, customers)
        plan.sources["customers"] = os.path.join(tmp, "missing.jsonl")
        with self.assertRaises(FileNotFoundError):
            self._execute(plan, "boom", memory_bytes=1 << 20)
        run_dir = self.runner.run_dir("p", "boom")
        self.assertFalse(os.path.exists(os.path.join(run_dir, "tmp")))
        self.assertFalse(os.path.exists(os.path.join(run_dir, "output")))
        self.assertEqual(self.runner.get_instance("p", "boom")["status"],
                         "failed")

    def test_published_version_is_immutable(self):
        orders = [{"k": "x", "v": 3}]
        customers = [{"k": "x", "info": "a"}]
        tmp, plan = make_plan(orders, customers)
        self.runner.save_plan(plan)
        self._execute(plan, "r1")
        self._execute(plan, "r2")
        pointer = os.path.join(self.runner.published_dir, "p", "v1.json")
        with open(pointer, encoding="utf-8") as fh:
            self.assertEqual(json.load(fh)["run_id"], "r1")
        with self.assertRaises(FileExistsError):
            self.runner.save_plan(plan)


if __name__ == "__main__":
    unittest.main()
