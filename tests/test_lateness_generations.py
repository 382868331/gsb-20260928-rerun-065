"""宽松迟到策略、来源代际与修订研究测试。

覆盖：按来源调整允许迟到时间、future/revision 两种作用域、
来源重启换代（序号重计、旧代际水位线隔离）、修订依据可解释、
无实际变化不产生重复修订，以及"一源重启序号重开、另一源持续
静默"场景下历史发布不被覆盖、重开后结果一致。
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
W1 = "2026-01-01T00:01:00Z"


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


class TestLatenessPolicy(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def state_dir(self, name="state"):
        return os.path.join(self.tmp, name)

    def rev_dir(self, name="state"):
        return os.path.join(self.state_dir(name), "revisions")

    def _confirmed_w0(self, engine):
        """payments 停发，orders 推进到 00:05：W0 确认并发布。"""
        engine.apply_event(ev("orders", 1, "o1",
                              "2026-01-01T00:00:10Z", "c1", 100))
        engine.apply_event(wm("orders", "2026-01-01T00:05:00Z"))
        engine.set_stalled("payments", True)
        self.assertEqual(len(engine.confirmed_view()), 1)

    def test_lateness_drops_beyond_horizon_keeps_within(self):
        spec = make_spec()
        engine = WindowEngine(spec, self.state_dir())
        engine.set_lateness("orders", 60)       # 允许迟到 60 秒
        self._confirmed_w0(engine)
        # 迟到 30 秒（水位线 300 - 60 = 240 之前才算超时）：接受并修订
        late_ok = engine.apply_event(ev("orders", 2, "o2",
                                        "2026-01-01T00:04:30Z", "c1", 7))
        self.assertNotEqual(late_ok.get("dropped"), "too_late")
        # 迟到超过 60 秒：丢弃、记录、窗口不变、无新修订
        rev_before = engine.revision
        dropped = engine.apply_event(ev("orders", 3, "o3",
                                        "2026-01-01T00:00:30Z", "c1", 9))
        self.assertEqual(dropped["dropped"], "too_late")
        self.assertEqual(dropped["lateness_seconds"], 60)
        self.assertIsNone(dropped["revision"])
        self.assertEqual(engine.revision, rev_before)
        self.assertEqual(view_map(engine.confirmed_view())[(W0, "c1")]["n"], 1)
        status = engine.source_status()
        self.assertEqual(status["orders"]["dropped_late"], 1)
        self.assertEqual(status["orders"]["lateness_seconds"], 60)

    def test_future_scope_does_not_reopen_published_windows(self):
        spec = make_spec()
        engine = WindowEngine(spec, self.state_dir())
        engine.set_lateness("orders", 60)
        self._confirmed_w0(engine)
        # 放宽到 1 小时，只影响今后的窗口
        impact = engine.set_lateness("orders", 3600, scope="future")
        self.assertIsNone(impact["revision"])
        # 已发布的 W0 仍按确认时的 60 秒策略：迟到 280 秒照样丢弃
        still = engine.apply_event(ev("orders", 2, "o2",
                                      "2026-01-01T00:00:30Z", "c1", 9))
        self.assertEqual(still["dropped"], "too_late")
        self.assertEqual(still["lateness_seconds"], 60)
        # 新窗口 W1 在放宽后确认，快照新策略：同样的迟到 280 秒被接受
        engine.apply_event(ev("orders", 3, "o3",
                              "2026-01-01T00:01:10Z", "c1", 5))
        engine.apply_event(wm("orders", "2026-01-01T00:06:00Z"))
        self.assertIn((W1, "c1"), view_map(engine.confirmed_view()))
        accepted = engine.apply_event(ev("orders", 4, "o4",
                                         "2026-01-01T00:01:20Z", "c1", 9))
        self.assertNotEqual(accepted.get("dropped"), "too_late")
        self.assertIsNotNone(accepted["revision"])
        self.assertEqual(view_map(engine.confirmed_view())[(W1, "c1")]["n"], 2)

    def test_revision_study_reapplies_dropped_with_causes(self):
        spec = make_spec()
        engine = WindowEngine(spec, self.state_dir())
        engine.set_lateness("orders", 60)
        self._confirmed_w0(engine)
        engine.apply_event(ev("orders", 2, "o2",
                              "2026-01-01T00:00:30Z", "c1", 9))   # 被丢弃
        self.assertEqual(engine.source_status()["orders"]["dropped_late"], 1)
        rev_before = engine.revision
        rev1 = os.path.join(self.rev_dir(), "rev-%05d.jsonl" % rev_before)
        rev1_bytes = open(rev1, "rb").read()

        # 对历史范围建立修订研究：放宽 W0 所在区间的策略
        impact = engine.set_lateness("orders", 3600, scope="revision",
                                     window_range=(W0, W1), confirm=True)
        self.assertEqual(impact["reapplied"],
                         [{"id": "o2", "seq": 2, "generation": 0}])
        self.assertIsNotNone(impact["revision"])
        self.assertEqual(view_map(engine.confirmed_view())[(W0, "c1")]["total"],
                         109)
        # 修订依据列明：规则变化 + 具体来源事件
        causes = engine.revision_causes()[impact["revision"]]
        kinds = {c["type"] for c in causes}
        self.assertEqual(kinds, {"lateness_rule", "late_event"})
        rule = next(c for c in causes if c["type"] == "lateness_rule")
        self.assertEqual(rule["source"], "orders")
        self.assertEqual(rule["lateness_seconds"], 3600)
        late = next(c for c in causes if c["type"] == "late_event")
        self.assertEqual((late["source"], late["id"]), ("orders", "o2"))
        # 旧修订文件不被改写
        self.assertEqual(open(rev1, "rb").read(), rev1_bytes)
        # 没有实际数据变化时不制造重复修订
        again = engine.set_lateness("orders", 7200, scope="revision",
                                    window_range=(W0, W1), confirm=True)
        self.assertEqual(again["reapplied"], [])
        self.assertIsNone(again["revision"])
        self.assertEqual(engine.revision, impact["revision"])
        # 重开进程：策略、丢弃记录与修订依据都还在
        engine = WindowEngine(spec, self.state_dir())
        self.assertEqual(engine.source_status()["orders"]["dropped_late"], 0)
        self.assertIn(impact["revision"], engine.revision_causes())


class TestConfirmationPolicy(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def _run_policy(self, policy, name):
        spec = make_spec()
        spec["streams"] = ["events"]
        spec["primary_source"] = "events"
        spec["lateness"] = {"events": 60}
        spec["confirmation_policy"] = policy
        state_dir = os.path.join(self.tmp, name)
        engine = WindowEngine(spec, state_dir)
        engine.apply_event(ev("events", 1, "e1",
                              "2026-01-01T00:00:10Z", "c1", 100))
        engine.apply_event(wm("events", "2026-01-01T00:01:00Z"))
        at_deadline = engine.confirmed_view()
        engine.apply_event(wm("events", "2026-01-01T00:01:30Z"))
        late = engine.apply_event(ev("events", 2, "e2",
                                    "2026-01-01T00:00:40Z", "c1", 7))
        return engine, state_dir, at_deadline, late

    def test_same_event_stream_compares_on_watermark_and_after_lateness(self):
        immediate, _, immediate_at, immediate_late = self._run_policy(
            "on_watermark", "immediate")
        delayed, delayed_state, delayed_at, delayed_late = self._run_policy(
            "after_lateness", "delayed")

        self.assertEqual(len(immediate_at), 1)
        immediate_row = immediate_at[0]
        self.assertEqual(immediate_row["confirmation_policy"], "on_watermark")
        self.assertEqual(immediate_row["confirmed_watermark"], W1)
        self.assertIsNone(immediate_row["confirmation_deadline"])
        self.assertIsNotNone(immediate_late["revision"])
        immediate_causes = immediate.revision_causes()[
            immediate_late["revision"]]
        self.assertEqual(immediate_causes[0]["type"], "late_event")
        self.assertEqual(immediate.revision_causes()[1], [])

        self.assertEqual(delayed_at, [])
        delayed_tentative = view_map(delayed.tentative_view())[(W0, "c1")]
        self.assertEqual(delayed_tentative["waiting_on"], ["events"])
        self.assertEqual(delayed_tentative["confirmation_policy"],
                         "after_lateness")

        delayed.apply_event(wm("events", "2026-01-01T00:02:00Z"))
        delayed_rows = view_map(delayed.confirmed_view())
        self.assertIn((W0, "c1"), delayed_rows)
        delayed_row = delayed_rows[(W0, "c1")]
        self.assertEqual(delayed_row["confirmation_policy"], "after_lateness")
        self.assertEqual(delayed_row["confirmed_watermark"],
                         "2026-01-01T00:02:00Z")
        self.assertEqual(delayed_row["confirmation_deadline"],
                         "2026-01-01T00:02:00Z")
        self.assertEqual(delayed_row["confirmation_deadlines"],
                         {"events": "2026-01-01T00:02:00Z"})
        self.assertIsNone(delayed_late["revision"])
        self.assertEqual(delayed.revision_causes()[1], [])

        immediate_final = view_map(immediate.confirmed_view())[(W0, "c1")]
        delayed_final = view_map(delayed.confirmed_view())[(W0, "c1")]
        self.assertEqual((immediate_final["total"], immediate_final["n"]),
                         (delayed_final["total"], delayed_final["n"]))
        self.assertEqual(immediate_final["total"], 107)
        self.assertEqual(delayed_final["n"], 2)

        reopened = WindowEngine(make_spec(), delayed_state)
        reopened_row = view_map(reopened.confirmed_view())[(W0, "c1")]
        self.assertEqual(reopened_row, delayed_final)
        self.assertEqual(reopened.revision_causes(), delayed.revision_causes())

    def test_future_policy_change_keeps_first_policy_for_open_window(self):
        spec = make_spec()
        spec["streams"] = ["events"]
        spec["primary_source"] = "events"
        spec["lateness"] = {"events": 60}
        spec["confirmation_policy"] = "on_watermark"
        engine = WindowEngine(spec, os.path.join(self.tmp, "future"))
        engine.apply_event(ev("events", 1, "e1",
                              "2026-01-01T00:00:10Z", "c1", 100))
        engine.set_confirmation_policy("after_lateness")
        engine.apply_event(wm("events", W1))
        self.assertEqual(len(engine.confirmed_view()), 1)

        engine.apply_event(ev("events", 2, "e2",
                              "2026-01-01T00:01:10Z", "c1", 5))
        engine.apply_event(wm("events", "2026-01-01T00:02:00Z"))
        w1 = view_map(engine.tentative_view())[(W1, "c1")]
        self.assertEqual(w1["waiting_on"], ["events"])
        engine.apply_event(wm("events", "2026-01-01T00:03:00Z"))
        self.assertEqual(view_map(engine.confirmed_view())[(W1, "c1")]
                         ["confirmation_policy"], "after_lateness")

    def test_historical_policy_restudy_requires_confirmation_and_keeps_release(self):
        spec = make_spec()
        spec["streams"] = ["events"]
        spec["primary_source"] = "events"
        spec["lateness"] = {"events": 60}
        spec["confirmation_policy"] = "on_watermark"
        state_dir = os.path.join(self.tmp, "history")
        engine = WindowEngine(spec, state_dir)
        engine.apply_event(ev("events", 1, "e1",
                              "2026-01-01T00:00:10Z", "c1", 100))
        engine.apply_event(wm("events", W1))
        rev1 = os.path.join(state_dir, "revisions", "rev-00001.jsonl")
        rev1_bytes = open(rev1, "rb").read()
        with self.assertRaisesRegex(ValueError, "confirm=True"):
            engine.set_confirmation_policy(
                "after_lateness", scope="revision",
                window_range=(W0, W1))
        impact = engine.set_confirmation_policy(
            "after_lateness", scope="revision",
            window_range=(W0, W1), confirm=True)
        self.assertTrue(impact["acknowledged"])
        self.assertEqual(impact["preserved_published_windows"], [
            {"window_start": W0,
             "revision_policy_kept": "on_watermark"}])
        self.assertEqual(view_map(engine.confirmed_view())[(W0, "c1")]
                         ["confirmation_policy"], "on_watermark")
        self.assertEqual(open(rev1, "rb").read(), rev1_bytes)

    def test_historical_restudy_to_on_watermark_confirms_waiting_window(self):
        spec = make_spec()
        spec["streams"] = ["events"]
        spec["primary_source"] = "events"
        spec["lateness"] = {"events": 3600}
        spec["confirmation_policy"] = "after_lateness"
        state_dir = os.path.join(self.tmp, "restudy")
        engine = WindowEngine(spec, state_dir)
        engine.apply_event(ev("events", 1, "e1",
                              "2026-01-01T00:00:10Z", "c1", 100))
        engine.apply_event(ev("events", 2, "e2",
                              "2026-01-01T00:01:10Z", "c1", 5))
        engine.apply_event(wm("events", "2026-01-01T00:02:00Z"))
        # 两个窗口都在等迟到期限，尚未确认
        self.assertEqual(engine.confirmed_view(), [])
        with self.assertRaisesRegex(ValueError, "confirm=True"):
            engine.set_confirmation_policy(
                "on_watermark", scope="revision", window_range=(W0, W1))
        # 只对 [W0, W1) 范围重研：W0 窗口立即确认并产生独立修订
        impact = engine.set_confirmation_policy(
            "on_watermark", scope="revision",
            window_range=(W0, W1), confirm=True)
        self.assertEqual(impact["newly_confirmed"], [W0])
        self.assertIsNotNone(impact["revision"])
        rev_no = impact["revision"]
        confirmed = view_map(engine.confirmed_view())
        self.assertEqual(confirmed[(W0, "c1")]["confirmation_policy"],
                         "on_watermark")
        # 范围隔离：W1 窗口保留原口径，仍在等待迟到期限
        w1 = view_map(engine.tentative_view())[(W1, "c1")]
        self.assertEqual(w1["status"], "tentative")
        self.assertEqual(w1["confirmation_policy"], "after_lateness")
        self.assertEqual(w1["waiting_on"], ["events"])
        # 无实际变化的重研不产生重复修订
        again = engine.set_confirmation_policy(
            "on_watermark", scope="revision",
            window_range=(W0, W1), confirm=True)
        self.assertIsNone(again["revision"])
        self.assertEqual(engine.revision, rev_no)
        # 修订依据可解释
        causes = engine.revision_causes()[rev_no]
        self.assertEqual(causes[0]["type"], "confirmation_policy_rule")
        self.assertEqual(causes[0]["confirmation_policy"], "on_watermark")
        # 保存重开后结果一致，已发布修订文件不被改写
        rev_files_before = {
            name: open(os.path.join(state_dir, "revisions", name), "rb").read()
            for name in os.listdir(os.path.join(state_dir, "revisions"))}
        reopened = WindowEngine(spec, state_dir)
        self.assertEqual(reopened.confirmed_view(), engine.confirmed_view())
        self.assertEqual(reopened.revision_causes(),
                         engine.revision_causes())
        w1_reopened = view_map(reopened.tentative_view())[(W1, "c1")]
        self.assertEqual(w1_reopened["confirmation_policy"], "after_lateness")
        for name, content in rev_files_before.items():
            self.assertEqual(
                open(os.path.join(state_dir, "revisions", name), "rb").read(),
                content)


class TestSourceGenerations(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def state_dir(self, name="state"):
        return os.path.join(self.tmp, name)

    def test_restart_restarts_seq_and_isolates_watermark(self):
        spec = make_spec()
        engine = WindowEngine(spec, self.state_dir())
        engine.apply_event(ev("orders", 1, "o1",
                              "2026-01-01T00:00:10Z", "c1", 100))
        engine.apply_event(wm("orders", "2026-01-01T00:05:00Z"))
        engine.set_stalled("payments", True)
        self.assertEqual(len(engine.confirmed_view()), 1)

        # 重启换代：进度重置为未知，重新参与阻塞确认（可解释）
        impact = engine.restart_source("orders")
        self.assertEqual(impact["generation"], 1)
        status = engine.source_status()
        self.assertEqual(status["orders"]["generation"], 1)
        self.assertIsNone(status["orders"]["watermark"])
        engine.apply_event(ev("orders", 1, "o9",
                              "2026-01-01T00:02:10Z", "c1", 5))
        waiting = view_map(engine.tentative_view())[
            ("2026-01-01T00:02:00Z", "c1")]["waiting_on"]
        self.assertEqual(waiting, ["orders"])

        # 新代际 seq 从 1 重新计数：不被旧代际的去重表挡住
        engine.apply_event(ev("orders", 2, "o10",
                              "2026-01-01T00:02:20Z", "c1", 6))
        self.assertEqual(view_map(engine.tentative_view())[
            ("2026-01-01T00:02:00Z", "c1")]["n"], 2)
        # 交错到达的旧代际重放被跳过，不会重复计入
        replay = engine.apply_event(ev("orders", 1, "o1",
                                       "2026-01-01T00:00:10Z", "c1", 100,
                                       generation=0))
        self.assertEqual(replay["skipped"], "replay")
        # 旧代际水位线被忽略：不能推进新实例的进度
        ignored = engine.apply_event(wm("orders", "2026-01-01T00:09:00Z",
                                        generation=0))
        self.assertEqual(ignored["ignored"], "old_generation")
        self.assertIsNone(engine.source_status()["orders"]["watermark"])
        # 旧实例的迟到记录仍可落入窗口（生成修订），但不推进水位
        late_old = engine.apply_event(ev("orders", 4, "o1",
                                         "2026-01-01T00:00:10Z", "c1", 130,
                                         generation=0))
        self.assertIsNotNone(late_old["revision"])
        self.assertEqual(view_map(engine.confirmed_view())[(W0, "c1")]["total"],
                         130)
        self.assertIsNone(engine.source_status()["orders"]["watermark"])
        # 新代际水位线正常推进
        engine.apply_event(wm("orders", "2026-01-01T00:06:00Z", generation=1))
        self.assertEqual(engine.source_status()["orders"]["watermark"],
                         "2026-01-01T00:06:00Z")

    def test_restart_with_silent_source_preserves_history(self):
        """一源重启序号重开、另一源持续静默：历史发布不被覆盖，
        保存重开后结果一致。"""
        spec = make_spec()
        state = self.state_dir()
        engine = WindowEngine(spec, state)
        # payments 全程静默：没有数据不能解释为已结束，一直阻塞确认
        engine.apply_event(ev("orders", 1, "o1",
                              "2026-01-01T00:00:10Z", "c1", 100))
        engine.apply_event(ev("orders", 2, "o2",
                              "2026-01-01T00:00:40Z", "c1", 50))
        engine.apply_event(wm("orders", "2026-01-01T00:05:00Z"))
        self.assertEqual(engine.confirmed_view(), [])
        self.assertEqual(view_map(engine.tentative_view())[(W0, "c1")]
                         ["waiting_on"], ["payments"])
        # 显式停发静默来源后 W0 发布（rev1）
        engine.set_stalled("payments", True)
        rev1 = os.path.join(state, "revisions", "rev-00001.jsonl")
        rev1_bytes = open(rev1, "rb").read()
        self.assertEqual(view_map(engine.confirmed_view())[(W0, "c1")]["total"],
                         150)

        # orders 重启换代：序号从 1 重新开始，新记录落入已发布窗口
        engine.restart_source("orders")
        impact = engine.apply_event(ev("orders", 1, "o3",
                                       "2026-01-01T00:00:20Z", "c1", 70))
        self.assertEqual(impact["generation"], 1)
        self.assertIsNotNone(impact["revision"])   # 迟到记录 -> 新修订
        # 交错的重放：旧代际 seq1、新代际 seq1 各算各的
        self.assertEqual(engine.apply_event(
            ev("orders", 1, "o1", "2026-01-01T00:00:10Z", "c1", 100,
               generation=0))["skipped"], "replay")
        self.assertEqual(engine.apply_event(
            ev("orders", 1, "o3", "2026-01-01T00:00:20Z", "c1", 70))
            ["skipped"], "replay")
        # 历史发布不被覆盖：rev1 字节不变，结果为 150 + 70
        self.assertEqual(open(rev1, "rb").read(), rev1_bytes)
        confirmed = view_map(engine.confirmed_view())
        self.assertEqual(confirmed[(W0, "c1")]["total"], 220)
        self.assertEqual(confirmed[(W0, "c1")]["n"], 3)
        causes = engine.revision_causes()[impact["revision"]]
        self.assertEqual(causes, [{"type": "late_event", "source": "orders",
                                   "id": "o3", "seq": 1, "generation": 1}])

        # 保存重开后结果一致：代际、停发决定、已确认视图全部保留
        tentative_before = engine.tentative_view()
        engine = WindowEngine(spec, state)
        status = engine.source_status()
        self.assertEqual(status["orders"]["generation"], 1)
        self.assertTrue(status["payments"]["stalled"])
        self.assertEqual(view_map(engine.confirmed_view()), confirmed)
        self.assertEqual(engine.tentative_view(), tentative_before)
        self.assertEqual(open(rev1, "rb").read(), rev1_bytes)
        # 重开后新代际继续推进，结果与重开前一致
        engine.apply_event(wm("orders", "2026-01-01T00:06:00Z"))
        self.assertEqual(view_map(engine.confirmed_view()), confirmed)


if __name__ == "__main__":
    unittest.main()
