"""允许迟到策略运行中调整 + 来源重启代际：

- 新策略默认只影响今后的窗口，已发布窗口不被当作从未关闭；
- 修订研究（restudy）只作用于指定历史范围，无实际变化不产生重复修订；
- 来源重启后 seq 重新计数（按代际去重），旧代际水位线不能推进新实例；
- 一条来源重启、另一条持续静默时，历史修订文件不被覆盖，
  保存点重开后结果一致。
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dtsys.windowing import WindowEngine


def read_jsonl(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def read_bytes(path):
    with open(path, "rb") as fh:
        return fh.read()


def ev(source, seq, rid, ts, cust, amount, **extra):
    event = {"op": "upsert", "source": source, "id": rid, "seq": seq,
             "event_time": ts, "cust_id": cust, "amount": amount}
    event.update(extra)
    return event


def wm(source, ts, **extra):
    event = {"op": "watermark", "source": source, "ts": ts}
    event.update(extra)
    return event


W0 = "2026-01-01T00:00:00Z"
W2 = "2026-01-01T00:02:00Z"
W6 = "2026-01-01T00:06:00Z"


def make_spec():
    return {
        "streams": ["orders", "payments"],
        "primary_source": "orders",
        "sources": {},
        "stages": [],
        "window": {
            "time_field": "event_time",
            "size_seconds": 60,
            "group_by": ["cust_id"],
            "aggregates": {"total": "sum(amount)", "n": "count()"},
        },
    }


def view_map(rows):
    return {(r["window_start"], r.get("cust_id")): r for r in rows}


class TestLatenessAndGenerations(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.state = os.path.join(self.tmp, "state")

    def rev_path(self, n):
        return os.path.join(self.state, "revisions", "rev-%05d.jsonl" % n)

    def test_restart_and_silent_peer_preserves_history(self):
        engine = WindowEngine(make_spec(), self.state)
        engine.apply_event(ev("orders", 1, "e1",
                              "2026-01-01T00:00:10Z", "c1", 100))
        engine.apply_event(wm("orders", "2026-01-01T00:05:00Z"))
        # payments 持续静默：没数据不能解释为已结束，窗口一直暂定
        row = view_map(engine.tentative_view())[(W0, "c1")]
        self.assertEqual(row["status"], "tentative")
        self.assertEqual(row["waiting_on"], ["payments"])
        # 显式停发静默来源后 W0 确认发布（rev1）
        impact = engine.set_stalled("payments", True)
        self.assertEqual(impact["revision"], 1)
        rev1_bytes = read_bytes(self.rev_path(1))
        self.assertEqual(view_map(engine.confirmed_view())[(W0, "c1")]["total"],
                         100)

        # orders 重启：进入第 1 代，水位线重置为未知，但已发布窗口不回退
        impact = engine.restart_source("orders")
        self.assertEqual(impact["generation"], 1)
        self.assertIsNone(impact["revision"])  # 重启本身不制造数据变化
        self.assertIn((W0, "c1"), view_map(engine.confirmed_view()))
        self.assertIsNone(engine.source_status()["orders"]["watermark"])

        # 旧代际迟到更正（gen 0 的 e1 seq 2）：仍按迟到数据修订已发布窗口
        impact = engine.apply_event(ev("orders", 2, "e1",
                                       "2026-01-01T00:00:10Z", "c1", 130,
                                       generation=0))
        self.assertTrue(impact["stale_generation"])
        self.assertIsNotNone(impact["revision"])
        meta = engine.revision_meta(impact["revision"])
        self.assertIn({"type": "late_event", "source": "orders", "id": "e1",
                       "op": "upsert", "seq": 2, "generation": 0},
                      meta["causes"])
        self.assertEqual(view_map(engine.confirmed_view())[(W0, "c1")]["total"],
                         130)

        # 旧代际水位线不能推进新实例的进度
        impact = engine.apply_event(wm("orders", "2026-01-01T00:10:00Z",
                                       generation=0))
        self.assertEqual(impact["skipped"], "stale_generation")
        self.assertIsNone(engine.source_status()["orders"]["watermark"])

        # 新代际 seq 从 1 重新开始：不与旧代际混淆，各自去重
        engine.apply_event(ev("orders", 1, "e2",
                              "2026-01-01T00:02:10Z", "c1", 7, generation=1))
        skipped = engine.apply_event(ev("orders", 1, "e2",
                                        "2026-01-01T00:02:10Z", "c1", 7,
                                        generation=1))
        self.assertEqual(skipped["skipped"], "replay")       # 新代际重放
        skipped = engine.apply_event(ev("orders", 1, "e1",
                                        "2026-01-01T00:00:10Z", "c1", 100,
                                        generation=0))
        self.assertEqual(skipped["skipped"], "replay")       # 旧代际重放

        # 新代际水位线推进后 W2 确认（payments 仍停发静默）
        engine.apply_event(wm("orders", "2026-01-01T00:06:00Z", generation=1))
        self.assertIn((W2, "c1"), view_map(engine.confirmed_view()))

        # 新的宽松迟到策略默认只影响今后的窗口：已发布窗口原样保留
        engine.apply_event(ev("orders", 2, "e3",
                              "2026-01-01T00:06:10Z", "c1", 3, generation=1))
        impact = engine.set_lateness("orders", 600)
        self.assertIsNone(impact["revision"])   # 没有实际变化，不制造修订
        self.assertEqual(impact["effective_from"], "2026-01-01T00:06:00Z")
        self.assertEqual(view_map(engine.confirmed_view())[(W0, "c1")]["total"],
                         130)
        # 宽松策略推迟今后窗口的确认：水位线越过窗口结束也不够
        impact = engine.apply_event(wm("orders", "2026-01-01T00:07:00Z",
                                       generation=1))
        self.assertIsNone(impact["revision"])
        row = view_map(engine.tentative_view())[(W6, "c1")]
        self.assertEqual(row["status"], "tentative")
        self.assertEqual(row["waiting_on"], ["orders"])

        # 对指定历史范围建立修订研究：W6 按 0 秒迟到重新评估后确认
        impact = engine.set_lateness(
            "orders", 0,
            restudy=("2026-01-01T00:06:00Z", "2026-01-01T00:07:00Z"))
        self.assertIsNotNone(impact["revision"])
        self.assertEqual(impact["newly_confirmed"], [W6])
        meta = engine.revision_meta(impact["revision"])
        lateness_causes = [c for c in meta["causes"]
                           if c["type"] == "lateness_change" and c["restudy"]]
        self.assertEqual(len(lateness_causes), 1)
        self.assertEqual(lateness_causes[0]["source"], "orders")
        # 同样的研究再来一次：没有实际数据变化，不产生重复修订
        again = engine.set_lateness(
            "orders", 0,
            restudy=("2026-01-01T00:06:00Z", "2026-01-01T00:07:00Z"))
        self.assertIsNone(again["revision"])

        # 历史修订文件永不覆盖；保存点重开后结果一致
        self.assertEqual(read_bytes(self.rev_path(1)), rev1_bytes)
        confirmed_before = engine.confirmed_view()
        tentative_before = engine.tentative_view()
        reopened = WindowEngine(make_spec(), self.state)
        self.assertEqual(reopened.confirmed_view(), confirmed_before)
        self.assertEqual(reopened.tentative_view(), tentative_before)
        status = reopened.source_status()
        self.assertEqual(status["orders"]["generation"], 1)
        self.assertTrue(status["payments"]["stalled"])
        self.assertEqual(status["orders"]["watermark"],
                         "2026-01-01T00:07:00Z")
        self.assertEqual(read_bytes(self.rev_path(1)), rev1_bytes)

    def test_event_with_higher_generation_implies_restart(self):
        engine = WindowEngine(make_spec(), self.state)
        engine.apply_event(ev("orders", 5, "e1",
                              "2026-01-01T00:00:10Z", "c1", 100))
        # 事件显式带来更高代际：视为来源已重启，seq 重新计数
        impact = engine.apply_event(ev("orders", 1, "e2",
                                       "2026-01-01T00:00:20Z", "c1", 5,
                                       generation=3))
        self.assertEqual(impact["generation"], 3)
        self.assertEqual(engine.source_status()["orders"]["generation"], 3)
        # 旧代际（含重启前）的重放仍被各自代际跳过
        skipped = engine.apply_event(ev("orders", 5, "e1",
                                        "2026-01-01T00:00:10Z", "c1", 100,
                                        generation=0))
        self.assertEqual(skipped["skipped"], "replay")
        row = view_map(engine.tentative_view())[(W0, "c1")]
        self.assertEqual(row["total"], 105)
        self.assertEqual(row["n"], 2)


if __name__ == "__main__":
    unittest.main()
