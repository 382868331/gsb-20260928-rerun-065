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

    def _skew_plan(self, left_rows, right_rows, how="inner", plan_id="skew"):
        tmp = tempfile.mkdtemp()
        left_path = os.path.join(tmp, "left.jsonl")
        right_path = os.path.join(tmp, "right.jsonl")
        write_jsonl(left_path, left_rows)
        write_jsonl(right_path, right_rows)
        return TransformPlan(
            plan_id=plan_id, name="skew", version=1,
            primary_source="left",
            sources={"left": left_path, "right": right_path},
            stages=[OperatorSpec("join", {
                "right_source": "right", "left_on": "k",
                "right_on": "k", "how": how})],
        )

    @staticmethod
    def _reference_join(left_rows, right_rows, how):
        expected = []
        for left in left_rows:
            matches = [r for r in right_rows
                       if repr(r.get("k")) == repr(left.get("k"))]
            if matches:
                for right in matches:
                    merged = dict(left)
                    merged["info"] = right["info"]
                    expected.append(merged)
            elif how == "left":
                merged = dict(left)
                merged.setdefault("info", None)
                expected.append(merged)
        return expected

    def test_skewed_join_respects_budget_and_keeps_product(self):
        from dtsys.operators.join import JoinOperator
        hot = ([{"id": i, "k": "HOT" if i % 10 else f"cold{i // 10}",
                 "v": i} for i in range(60)],
               [{"k": "HOT", "info": "a"}, {"k": "HOT", "info": "b"},
                {"k": "cold0", "info": "z"}])
        same = ([{"id": i, "k": "x", "v": i} for i in range(40)],
                [{"k": "x", "info": "p"}, {"k": "x", "info": "q"},
                 {"k": "x", "info": "r"}])
        nulls = ([{"id": i,
                   "k": None if i % 4 == 0 else ("n" if i % 4 == 1 else "x"),
                   "v": i} for i in range(40)],
                 [{"k": None, "info": "N"}, {"k": "n", "info": "nn"},
                  {"k": "x", "info": "xx"}, {"k": None, "info": "N2"}])
        cases = {"hot": hot, "same": same, "nulls": nulls}
        for name, (left_rows, right_rows) in cases.items():
            for how in ("inner", "left"):
                plan_id = f"skew-{name}-{how}"
                plan = self._skew_plan(left_rows, right_rows, how, plan_id)
                expected = self._reference_join(left_rows, right_rows, how)
                self._execute(plan, "big", memory_bytes=1 << 20)
                tiny = self._execute(plan, "tiny",
                                     memory_bytes=128, chunk_bytes=64)
                big_rows = read_jsonl(os.path.join(
                    self.runner.run_dir(plan_id, "big"),
                    "output", "result.jsonl"))
                tiny_rows = read_jsonl(os.path.join(
                    self.runner.run_dir(plan_id, "tiny"),
                    "output", "result.jsonl"))
                key = lambda rs: sorted(
                    json.dumps(r, sort_keys=True, ensure_ascii=False) for r in rs)
                self.assertEqual(len(tiny_rows), len(expected), plan_id)
                self.assertEqual(key(big_rows), key(expected), plan_id)
                self.assertEqual(key(tiny_rows), key(expected), plan_id)
                # 累计在途数据峰值必须压在极小预算之内（行 JSON 字节口径）
                self.assertLessEqual(
                    tiny["peak_inflight_bytes"], 128, plan_id)

    def test_cancellation_during_skewed_join_cleans_tmp(self):
        from dtsys.operators.join import JoinOperator
        left_rows = [{"id": i, "k": "HOT", "v": i} for i in range(200)]
        right_rows = [{"k": "HOT", "info": "a"}, {"k": "HOT", "info": "b"}]
        plan = self._skew_plan(left_rows, right_rows, "left", "p")
        self._execute(plan, "good", memory_bytes=1 << 20)

        original = JoinOperator._blocked_nested_loop_join
        token = CancellationToken()

        def cancel_on_entry(self, *args, **kwargs):
            token.cancel()
            return original(self, *args, **kwargs)

        JoinOperator._blocked_nested_loop_join = cancel_on_entry
        try:
            with self.assertRaises(RuntimeError):
                self.runner.execute(
                    plan, RunConfig(run_id="cancelled",
                                    memory_bytes=128, chunk_bytes=64), token)
        finally:
            JoinOperator._blocked_nested_loop_join = original
        run_dir = self.runner.run_dir("p", "cancelled")
        self.assertFalse(os.path.exists(os.path.join(run_dir, "tmp")))
        self.assertFalse(os.path.exists(os.path.join(run_dir, "output")))
        self.assertEqual(self.runner.get_instance("p", "cancelled")["status"],
                         "cancelled")
        with open(os.path.join(self.runner.published_dir, "p", "v1.json"),
                  encoding="utf-8") as fh:
            self.assertEqual(json.load(fh)["run_id"], "good")


if __name__ == "__main__":
    unittest.main()
