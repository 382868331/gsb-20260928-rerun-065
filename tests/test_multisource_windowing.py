"""多来源窗口引擎测试：独立水位线、停发/恢复、来源作用域、
双流连接进度约束与保存点续跑。"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dtsys.windowing import WindowEngine, diff_revisions


def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False))
            fh.write("\n")


def read_jsonl(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def ev(source, seq, rid, ts, cust, amount, **extra):
    event = {"op": "upsert", "source": source, "id": rid, "seq": seq,
             "event_time": ts, "cust_id": cust, "amount": amount}
    event.update(extra)
    return event


def wm(source, ts):
    return {"op": "watermark", "source": source, "ts": ts}


W0 = "2026-01-01T00:00:00Z"


def make_spec(streams=("orders", "payments"), stages=None):
    return {
        "streams": list(streams),
        "primary_source": streams[0],
        "sources": {},
        "stages": stages or [],
        "window": {
            "time_field": "event_time",
            "size_seconds": 60,
            "group_by": ["cust_id"],
            "aggregates": {"total": "sum(amount)", "n": "count()"},
        },
    }


def view_map(rows):
    return {(r["window_start"], r.get("cust_id")): r for r in rows}


class TestMultiSourceWatermark(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def state_dir(self, name="state"):
        return os.path.join(self.tmp, name)

    def test_window_waits_for_slow_source(self):
        spec = make_spec()
        engine = WindowEngine(spec, self.state_dir())
        engine.apply_event(ev("orders", 1, "o1",
                              "2026-01-01T00:00:10Z", "c1", 100))
        engine.apply_event(wm("orders", "2026-01-01T00:05:00Z"))
        # payments 没有任何进度：没数据不等于已结束，窗口仍暂定
        row = view_map(engine.tentative_view())[(W0, "c1")]
        self.assertEqual(row["status"], "tentative")
        self.assertEqual(row["waiting_on"], ["payments"])
        self.assertEqual(engine.confirmed_view(), [])
        # payments 推进但未越过窗口结束：仍在等
        engine.apply_event(wm("payments", "2026-01-01T00:00:30Z"))
        row = view_map(engine.tentative_view())[(W0, "c1")]
        self.assertEqual(row["waiting_on"], ["payments"])
        # payments 越过后窗口确认，不能只按较快一侧提前宣布
        engine.apply_event(wm("payments", "2026-01-01T00:01:00Z"))
        row = view_map(engine.tentative_view())[(W0, "c1")]
        self.assertEqual(row["status"], "confirmed")
        self.assertEqual(row["waiting_on"], [])

    def test_stall_publishes_then_resume_revises(self):
        spec = make_spec()
        engine = WindowEngine(spec, self.state_dir())
        engine.apply_event(ev("orders", 1, "o1",
                              "2026-01-01T00:00:10Z", "c1", 100))
        engine.apply_event(wm("orders", "2026-01-01T00:05:00Z"))
        self.assertEqual(engine.confirmed_view(), [])
        # 显式停发 payments：视为暂时不再推进，窗口立即发布
        impact = engine.set_stalled("payments", True)
        self.assertIsNotNone(impact["revision"])
        self.assertEqual(impact["newly_confirmed"], [W0])
        rev1 = os.path.join(self.state_dir(), "revisions",
                            "rev-%05d.jsonl" % impact["revision"])
        rev1_rows = read_jsonl(rev1)
        self.assertEqual(rev1_rows[0]["total"], 100)
        status = engine.source_status()
        self.assertTrue(status["payments"]["stalled"])
        self.assertFalse(status["orders"]["stalled"])
        # 恢复后 payments 的迟到记录改动已发布窗口 -> 有依据的新修订
        engine.set_stalled("payments", False)
        engine.apply_event(wm("payments", "2026-01-01T00:05:00Z"))
        impact = engine.apply_event(ev("orders", 2, "o1",
                                       "2026-01-01T00:00:10Z", "c1", 130))
        self.assertIsNotNone(impact["revision"])
        rev2 = os.path.join(self.state_dir(), "revisions",
                            "rev-%05d.jsonl" % impact["revision"])
        self.assertEqual(read_jsonl(rev1), rev1_rows)  # 旧文件不改写
        changes = diff_revisions(rev1, rev2)
        self.assertEqual(changes[0]["deltas"]["total"],
                         {"old": 100, "new": 130, "delta": 30})

    def test_same_id_different_sources_are_independent(self):
        spec = make_spec()
        engine = WindowEngine(spec, self.state_dir())
        engine.apply_event(ev("orders", 1, "x",
                              "2026-01-01T00:00:10Z", "c1", 100))
        # 同编号 x 在 payments 是另一条记录（维流，不产生窗口行）
        engine.apply_event(ev("payments", 1, "x",
                              "2026-01-01T00:00:20Z", "c1", 7))
        # payments 撤回 x 不影响 orders 的 x
        engine.apply_event({"op": "retract", "source": "payments",
                            "id": "x", "seq": 2})
        row = view_map(engine.tentative_view())[(W0, "c1")]
        self.assertEqual(row["total"], 100)
        # orders 的重放被跳过；payments 的 seq 独立计数
        skipped = engine.apply_event(ev("orders", 1, "x",
                                        "2026-01-01T00:00:10Z", "c1", 100))
        self.assertEqual(skipped["skipped"], "replay")
        self.assertEqual(view_map(engine.tentative_view())[(W0, "c1")]["n"], 1)

    def test_stream_join_waits_for_both_sides(self):
        spec = make_spec(streams=("orders", "customers"), stages=[
            {"op": "join", "right_source": "customers",
             "left_on": "cust_id", "how": "inner"},
        ])
        engine = WindowEngine(spec, self.state_dir())
        # 维流合法一对多：c1 有两个分段
        engine.apply_event(ev("customers", 1, "d1",
                              "2026-01-01T00:00:00Z", "c1", 0, seg="A"))
        engine.apply_event(ev("customers", 2, "d2",
                              "2026-01-01T00:00:00Z", "c1", 0, seg="B"))
        impact = engine.apply_event(ev("orders", 1, "o1",
                                       "2026-01-01T00:00:10Z", "c1", 100))
        self.assertEqual(len(impact["join_added"]), 2)  # 放大保留
        engine.apply_event(wm("orders", "2026-01-01T00:05:00Z"))
        # 较快一侧推进不能宣布完整：还在等 customers
        row = view_map(engine.tentative_view())[(W0, "c1")]
        self.assertEqual(row["status"], "tentative")
        self.assertEqual(row["waiting_on"], ["customers"])
        self.assertEqual(engine.confirmed_view(), [])
        engine.apply_event(wm("customers", "2026-01-01T00:05:00Z"))
        row = view_map(engine.confirmed_view())[(W0, "c1")]
        self.assertEqual(row["total"], 200)
        self.assertEqual(row["n"], 2)
        # 维流更正（撤掉一个分段）改动已确认窗口 -> 新修订
        impact = engine.apply_event({"op": "retract", "source": "customers",
                                     "id": "d2", "seq": 3})
        self.assertIsNotNone(impact["revision"])
        row = view_map(engine.confirmed_view())[(W0, "c1")]
        self.assertEqual(row["total"], 100)
        self.assertEqual(row["n"], 1)

    def test_savepoint_restores_offsets_and_stall(self):
        spec = make_spec()
        orders_path = os.path.join(self.tmp, "orders.jsonl")
        payments_path = os.path.join(self.tmp, "payments.jsonl")
        write_jsonl(orders_path, [
            ev("orders", 1, "o1", "2026-01-01T00:00:10Z", "c1", 100),
            ev("orders", 2, "o2", "2026-01-01T00:00:20Z", "c1", 40),
            wm("orders", "2026-01-01T00:05:00Z"),
            ev("orders", 3, "o1", "2026-01-01T00:00:10Z", "c1", 130),
        ])
        write_jsonl(payments_path, [
            wm("payments", "2026-01-01T00:00:30Z"),
        ])

        # 一次性跑完作为参照
        full = WindowEngine(spec, self.state_dir("full"))
        full.run_events(orders_path, source="orders")
        full.run_events(payments_path, source="payments")
        full.set_stalled("payments", True)
        full.run_events(orders_path, source="orders")  # 无新增
        expected_confirmed = full.confirmed_view()
        expected_tentative = full.tentative_view()

        # 分来源暂停续跑：保存点还原各来源偏移与停发决定
        paused = WindowEngine(spec, self.state_dir("paused"))
        paused.run_events(orders_path, source="orders", limit=2)
        paused.run_events(payments_path, source="payments")
        paused.set_stalled("payments", True)
        paused = WindowEngine(spec, self.state_dir("paused"))  # 模拟重开
        status = paused.source_status()
        self.assertTrue(status["payments"]["stalled"])   # 停发决定保留
        self.assertGreater(status["orders"]["offset"], 0)
        self.assertEqual(status["payments"]["watermark"],
                         "2026-01-01T00:00:30Z")
        paused.run_events(orders_path, source="orders")  # 从偏移续跑
        self.assertEqual(expected_confirmed, paused.confirmed_view())
        self.assertEqual(expected_tentative, paused.tentative_view())
        # 逐笔影响合计（确认后到达的事件）= 停发发布修订到最终修订的差额
        rev_dir = os.path.join(self.state_dir("paused"), "revisions")
        revs = sorted(os.listdir(rev_dir))
        impacts = read_jsonl(paused.impact_path)
        per_event = [d["deltas"]["total"]["delta"]
                     for impact in impacts
                     for d in impact.get("window_deltas", {}).values()
                     if "total" in d.get("deltas", {}) and d["confirmed"]]
        stall_rev = next(name for name in revs
                         if any(r.get("total") == 140
                                for r in read_jsonl(os.path.join(rev_dir,
                                                                 name))))
        changes = diff_revisions(os.path.join(rev_dir, stall_rev),
                                 os.path.join(rev_dir, revs[-1]))
        self.assertEqual(sum(c["deltas"]["total"]["delta"] for c in changes),
                         sum(per_event))
        final_total = sum(r["total"] for r in paused.confirmed_view())
        self.assertEqual(final_total, 170)  # 130(更正后) + 40

    def test_stall_decision_survives_restart_and_blocks_again(self):
        spec = make_spec()
        engine = WindowEngine(spec, self.state_dir())
        engine.apply_event(ev("orders", 1, "o1",
                              "2026-01-01T00:00:10Z", "c1", 100))
        engine.apply_event(wm("orders", "2026-01-01T00:05:00Z"))
        engine.set_stalled("payments", True)
        self.assertEqual(len(engine.confirmed_view()), 1)
        # 恢复后该来源重新计入有效水位线：新窗口再次被它阻塞
        engine.set_stalled("payments", False)
        engine.apply_event(ev("orders", 2, "o2",
                              "2026-01-01T00:02:10Z", "c1", 5))
        row = view_map(engine.tentative_view())[
            ("2026-01-01T00:02:00Z", "c1")]
        self.assertEqual(row["status"], "tentative")
        self.assertEqual(row["waiting_on"], ["payments"])
        # 重开进程后停发/进度状态仍然清楚保留
        engine = WindowEngine(spec, self.state_dir())
        status = engine.source_status()
        self.assertFalse(status["payments"]["stalled"])
        self.assertIsNone(status["payments"]["watermark"])


if __name__ == "__main__":
    unittest.main()
