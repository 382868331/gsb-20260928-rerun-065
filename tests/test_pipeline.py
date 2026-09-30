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

    def _skew_plan(self, left_rows, right_rows, how="inner"):
        tmp = tempfile.mkdtemp()
        left_path = os.path.join(tmp, "left.jsonl")
        right_path = os.path.join(tmp, "right.jsonl")
        write_jsonl(left_path, left_rows)
        write_jsonl(right_path, right_rows)
        plan = TransformPlan(
            plan_id="skew", name="t", version=1,
            primary_source="left",
            sources={"left": left_path, "right": right_path},
            stages=[OperatorSpec("join", {"right_source": "right",
                                          "left_on": "k", "right_on": "k",
                                          "how": how})],
        )
        return tmp, plan

    @staticmethod
    def _reference_join(left_rows, right_rows, how="inner"):
        """直接嵌套循环计算，空键与 None/缺失/空串按取值区分。"""
        def key(row, name):
            return row.get(name)

        result = []
        for left in left_rows:
            matched = False
            for right in right_rows:
                if key(left, "k") == key(right, "k"):
                    merged = dict(left)
                    for col, value in right.items():
                        if col != "k":
                            merged[col] = value
                    result.append(merged)
                    matched = True
            if how == "left" and not matched:
                merged = dict(left)
                for col in right_rows[0] if right_rows else {}:
                    if col != "k":
                        merged.setdefault(col, None)
                result.append(merged)
        return result

    def _skew_dataset(self):
        # 热键 "hot" 占绝大多数；少量冷键；混合空值（None / 缺失 / 空串）。
        hot_n, hot_m = 120, 60
        left_rows = (
            [{"k": "hot", "v": i} for i in range(hot_n)]
            + [{"k": "cold1", "v": 900}, {"k": "cold2", "v": 901}]
            + [{"k": None, "v": 902}, {"v": 903}, {"k": "", "v": 904},
               {"k": "nope", "v": 905}]
        )
        right_rows = (
            [{"k": "hot", "tag": f"r{j}"} for j in range(hot_m)]
            + [{"k": "cold1", "tag": "cold"}]
            + [{"k": None, "tag": "null"}, {"k": "", "tag": "empty"}]
        )
        return left_rows, right_rows

    def test_skewed_hotkey_join_tiny_budget_matches_reference(self):
        left_rows, right_rows = self._skew_dataset()
        _, plan = self._skew_plan(left_rows, right_rows, how="inner")
        instance = self.runner.execute(
            plan, RunConfig(run_id="tiny", memory_bytes=160, chunk_bytes=80))
        got = read_jsonl(os.path.join(
            self.runner.run_dir("skew", "tiny"), "output", "result.jsonl"))
        expected = self._reference_join(left_rows, right_rows, "inner")
        multiset = lambda rs: sorted(
            json.dumps(r, sort_keys=True, ensure_ascii=False) for r in rs)
        self.assertEqual(multiset(got), multiset(expected))
        # 热键 120×60 + 冷键 1 + 空键：None×None 1、空串×空串 1
        self.assertEqual(len(got), 120 * 60 + 4)
        # 极小预算下在途数据不得无界堆积：允许一条超大结果行独占通道。
        max_row = max(len(json.dumps(r, ensure_ascii=False)) + 1 for r in got)
        metrics = instance["metrics"]
        self.assertLessEqual(
            metrics["peak_inflight_bytes"], 160 + max_row)
        # 构建侧工作块同样受控（近似 JSON 字节口径 + 字典开销余量）。
        self.assertLess(metrics["peak_join_work_bytes"], 160 * 4)

    def test_identical_key_only_join_is_full_cartesian_product(self):
        left_rows = [{"k": "z", "v": i} for i in range(80)]
        right_rows = [{"k": "z", "tag": j} for j in range(40)]
        _, plan = self._skew_plan(left_rows, right_rows)
        self.runner.execute(
            plan, RunConfig(run_id="same", memory_bytes=128, chunk_bytes=64))
        got = read_jsonl(os.path.join(
            self.runner.run_dir("skew", "same"), "output", "result.jsonl"))
        self.assertEqual(len(got), 80 * 40)  # 完整乘积，一行不少
        multiset = lambda rs: sorted(
            json.dumps(r, sort_keys=True, ensure_ascii=False) for r in rs)
        self.assertEqual(
            multiset(got),
            multiset(self._reference_join(left_rows, right_rows)))

    def test_left_join_null_semantics_tiny_budget(self):
        left_rows, right_rows = self._skew_dataset()
        _, plan = self._skew_plan(left_rows, right_rows, how="left")
        self.runner.execute(
            plan, RunConfig(run_id="l", memory_bytes=128, chunk_bytes=64))
        got = read_jsonl(os.path.join(
            self.runner.run_dir("skew", "l"), "output", "result.jsonl"))
        expected = self._reference_join(left_rows, right_rows, "left")
        multiset = lambda rs: sorted(
            json.dumps(r, sort_keys=True, ensure_ascii=False) for r in rs)
        self.assertEqual(multiset(got), multiset(expected))
        # 缺失键的左行无匹配 -> 补空；空串与 None 各自只与同类相等。
        unmatched = [r for r in got if r["v"] == 905]
        self.assertEqual(len(unmatched), 1)
        self.assertIsNone(unmatched[0]["tag"])

    def test_slow_downstream_backpressure_then_cancel_cleans(self):
        left_rows, right_rows = self._skew_dataset()
        _, plan = self._skew_plan(left_rows, right_rows)
        # 先发布一个成功版本。
        self.runner.execute(
            plan, RunConfig(run_id="published", memory_bytes=128,
                            chunk_bytes=64))
        token = CancellationToken()
        import threading

        def cancel_soon():
            token.cancel()

        timer = threading.Timer(0.2, cancel_soon)
        timer.start()
        try:
            with self.assertRaises(RuntimeError):
                self.runner.execute(
                    plan,
                    RunConfig(run_id="slow", memory_bytes=128,
                              chunk_bytes=64, slow_write_bytes=64),
                    token,
                )
        finally:
            timer.cancel()
        run_dir = self.runner.run_dir("skew", "slow")
        self.assertFalse(os.path.exists(os.path.join(run_dir, "tmp")))
        self.assertFalse(os.path.exists(os.path.join(run_dir, "output")))
        self.assertEqual(self.runner.get_instance("skew", "slow")["status"],
                         "cancelled")
        # 已发布版本保持可用且指针不变。
        pointer = os.path.join(self.runner.published_dir, "skew", "v1.json")
        with open(pointer, encoding="utf-8") as fh:
            self.assertEqual(json.load(fh)["run_id"], "published")
        published_rows = read_jsonl(os.path.join(
            self.runner.run_dir("skew", "published"),
            "output", "result.jsonl"))
        self.assertEqual(len(published_rows), 120 * 60 + 4)

    def test_slow_downstream_complete_with_bounded_inflight(self):
        # 下游写出被人为限速（64 字节/秒），完整结果仍必须产出，
        # 且在途峰值不超过预算加单条余量——验证背压而非丢输出。
        left_rows, right_rows = self._skew_dataset()
        _, plan = self._skew_plan(left_rows, right_rows)
        instance = self.runner.execute(
            plan, RunConfig(run_id="slow-ok", memory_bytes=128,
                            chunk_bytes=64, slow_write_bytes=128 * 1024))
        got = read_jsonl(os.path.join(
            self.runner.run_dir("skew", "slow-ok"),
            "output", "result.jsonl"))
        multiset = lambda rs: sorted(
            json.dumps(r, sort_keys=True, ensure_ascii=False) for r in rs)
        self.assertEqual(
            multiset(got),
            multiset(self._reference_join(left_rows, right_rows)))
        max_row = max(len(json.dumps(r, ensure_ascii=False)) + 1 for r in got)
        self.assertLessEqual(
            instance["metrics"]["peak_inflight_bytes"], 128 + max_row)


if __name__ == "__main__":
    unittest.main()
