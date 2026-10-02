"""窗口引擎测试：乱序、跨窗口更正、空输入、连接放大、重放去重、暂停续跑。"""

from __future__ import annotations

import json
import os
import random
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


def ev(seq, rid, ts, cust, amount):
    return {"op": "upsert", "id": rid, "seq": seq, "event_time": ts,
            "cust_id": cust, "amount": amount}


def wm(ts):
    return {"op": "watermark", "ts": ts}


BASE = "2026-01-01T00:00:00Z"


def make_spec(tmp, stages=None, customers=None):
    sources = {}
    if customers is not None:
        cust_path = os.path.join(tmp, "customers.jsonl")
        write_jsonl(cust_path, customers)
        sources["customers"] = cust_path
    return {
        "sources": sources,
        "stages": stages or [],
        "window": {
            "time_field": "event_time",
            "size_seconds": 60,
            "group_by": ["cust_id"],
            "aggregates": {"total": "sum(amount)", "n": "count()"},
        },
    }


def run_all(spec, events, state_dir):
    engine = WindowEngine(spec, state_dir)
    for event in events:
        engine.apply_event(event)
    return engine


def view_key(row):
    return (row["window_start"], row.get("cust_id"))


def view_map(rows):
    return {view_key(r): r for r in rows}


class TestWindowEngine(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def state_dir(self, name="state"):
        return os.path.join(self.tmp, name)

    def test_out_of_order_and_replay_same_final(self):
        events = [
            ev(1, "a", "2026-01-01T00:00:10Z", "c1", 100),
            ev(2, "b", "2026-01-01T00:00:40Z", "c1", 50),
            ev(3, "c", "2026-01-01T00:01:05Z", "c1", 70),
            ev(4, "d", "2026-01-01T00:00:20Z", "c2", 30),
            wm("2026-01-01T00:03:00Z"),
        ]
        spec = make_spec(self.tmp)
        ref = view_map(run_all(spec, events, self.state_dir("s1")).confirmed_view())

        shuffled = events[:-1]
        random.Random(7).shuffle(shuffled)
        # 乱序 + 每条重放一次
        replayed = []
        for event in shuffled:
            replayed.append(event)
            replayed.append(dict(event))
        replayed.append(events[-1])
        got = view_map(run_all(spec, replayed, self.state_dir("s2")).confirmed_view())
        self.assertEqual(ref, got)
        # 重放没有重复计入
        self.assertEqual(got[("2026-01-01T00:00:00Z", "c1")]["total"], 150)
        self.assertEqual(got[("2026-01-01T00:00:00Z", "c1")]["n"], 2)

    def test_tentative_vs_confirmed(self):
        spec = make_spec(self.tmp)
        engine = WindowEngine(spec, self.state_dir())
        engine.apply_event(ev(1, "a", "2026-01-01T00:00:10Z", "c1", 100))
        engine.apply_event(ev(2, "b", "2026-01-01T00:05:00Z", "c1", 5))
        engine.advance_watermark("2026-01-01T00:01:00Z")
        tentative = view_map(engine.tentative_view())
        confirmed = view_map(engine.confirmed_view())
        self.assertEqual(tentative[("2026-01-01T00:00:00Z", "c1")]["status"],
                         "confirmed")
        self.assertEqual(tentative[("2026-01-01T00:05:00Z", "c1")]["status"],
                         "tentative")
        self.assertEqual(set(confirmed), {("2026-01-01T00:00:00Z", "c1")})

    def test_cross_window_correction_creates_revision(self):
        spec = make_spec(self.tmp)
        engine = WindowEngine(spec, self.state_dir())
        engine.apply_event(ev(1, "a", "2026-01-01T00:00:10Z", "c1", 100))
        engine.advance_watermark("2026-01-01T00:02:00Z")  # 确认 00:00 窗口
        rev_dir = os.path.join(self.state_dir(), "revisions")
        rev1 = os.path.join(rev_dir, "rev-00001.jsonl")
        rev1_before = read_jsonl(rev1)
        self.assertEqual(rev1_before[0]["total"], 100)

        # 迟到更正：把记录从 00:00 窗口改到 00:01 窗口（两窗均已确认）
        impact = engine.apply_event(
            ev(2, "a", "2026-01-01T00:01:10Z", "c1", 100))
        self.assertIsNotNone(impact["revision"])
        rev2 = os.path.join(rev_dir, "rev-%05d.jsonl" % impact["revision"])
        # 旧修订文件未被改写
        self.assertEqual(read_jsonl(rev1), rev1_before)
        changes = diff_revisions(rev1, rev2)
        by_start = {c["window_start"]: c for c in changes}
        old_win = by_start["2026-01-01T00:00:00Z"]
        new_win = by_start["2026-01-01T00:01:00Z"]
        self.assertEqual(old_win["change"], "removed")  # 撤回差额
        self.assertEqual(old_win["old"]["total"], 100)
        self.assertEqual(new_win["change"], "added")
        self.assertEqual(new_win["new"]["total"], 100)
        # 影响报告逐笔给出窗口差额
        deltas = impact["window_deltas"]
        amounts = sorted(
            d["deltas"]["total"]["delta"] for d in deltas.values())
        self.assertEqual(amounts, [-100, 100])

    def test_retract_after_confirm(self):
        spec = make_spec(self.tmp)
        engine = WindowEngine(spec, self.state_dir())
        engine.apply_event(ev(1, "a", "2026-01-01T00:00:10Z", "c1", 100))
        engine.apply_event(ev(2, "b", "2026-01-01T00:00:20Z", "c1", 40))
        engine.advance_watermark("2026-01-01T00:01:00Z")
        rev1 = os.path.join(self.state_dir(), "revisions", "rev-00001.jsonl")
        impact = engine.apply_event({"op": "retract", "id": "a", "seq": 3})
        rev2 = os.path.join(self.state_dir(), "revisions",
                            "rev-%05d.jsonl" % impact["revision"])
        changes = diff_revisions(rev1, rev2)
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0]["deltas"]["total"],
                         {"old": 140, "new": 40, "delta": -100})
        self.assertEqual(view_map(engine.confirmed_view())
                         [("2026-01-01T00:00:00Z", "c1")]["total"], 40)

    def test_empty_input(self):
        spec = make_spec(self.tmp)
        engine = WindowEngine(spec, self.state_dir())
        self.assertEqual(engine.tentative_view(), [])
        self.assertEqual(engine.confirmed_view(), [])
        engine.advance_watermark("2026-01-01T00:10:00Z")
        self.assertEqual(engine.confirmed_view(), [])
        # 空事件文件续跑
        events_path = os.path.join(self.tmp, "events.jsonl")
        write_jsonl(events_path, [])
        self.assertEqual(engine.run_events(events_path), 0)

    def test_join_amplification_and_replay(self):
        customers = [
            {"cust_id": "c1", "seg": "A"},
            {"cust_id": "c1", "seg": "B"},   # 维表合法重复 -> 放大
        ]
        spec = make_spec(self.tmp, stages=[
            {"op": "filter", "expr": "amount >= 10"},
            {"op": "join", "right_source": "customers",
             "left_on": "cust_id", "how": "inner"},
        ], customers=customers)
        engine = WindowEngine(spec, self.state_dir())
        impact = engine.apply_event(
            ev(1, "a", "2026-01-01T00:00:10Z", "c1", 100))
        self.assertEqual(len(impact["join_added"]), 2)  # 一条事实放大为两行
        engine.apply_event(dict(ev(1, "a", "2026-01-01T00:00:10Z", "c1", 100)))
        engine.advance_watermark("2026-01-01T00:01:00Z")
        row = view_map(engine.confirmed_view())[("2026-01-01T00:00:00Z", "c1")]
        self.assertEqual(row["n"], 2)          # 合法重复保留
        self.assertEqual(row["total"], 200)    # 重放未再次计入
        # 撤回影响全部连接结果
        impact = engine.apply_event({"op": "retract", "id": "a", "seq": 2})
        self.assertEqual(len(impact["join_removed"]), 2)
        self.assertEqual(engine.tentative_view(), [])

    def test_correction_reports_join_impact(self):
        customers = [{"cust_id": "c1", "seg": "A"},
                     {"cust_id": "c2", "seg": "B"}]
        spec = make_spec(self.tmp, stages=[
            {"op": "join", "right_source": "customers",
             "left_on": "cust_id", "how": "inner"},
        ], customers=customers)
        engine = WindowEngine(spec, self.state_dir())
        engine.apply_event(ev(1, "a", "2026-01-01T00:00:10Z", "c1", 100))
        # 更正把记录改到 c2：连接结果从 seg A 换成 seg B
        impact = engine.apply_event(
            ev(2, "a", "2026-01-01T00:00:10Z", "c2", 100))
        self.assertEqual([r["seg"] for r in impact["join_removed"]], ["A"])
        self.assertEqual([r["seg"] for r in impact["join_added"]], ["B"])
        amounts = sorted(
            d["deltas"]["total"]["delta"]
            for d in impact["window_deltas"].values())
        self.assertEqual(amounts, [-100, 100])

    def test_pause_resume_matches_uninterrupted(self):
        events = [ev(i + 1, "r%d" % i,
                     "2026-01-01T00:%02d:%02dZ" % (i % 3, (i * 7) % 60),
                     "c%d" % (i % 2), 10 + i) for i in range(12)]
        events.append(wm("2026-01-01T00:05:00Z"))
        events_path = os.path.join(self.tmp, "events.jsonl")
        write_jsonl(events_path, events)
        spec = make_spec(self.tmp)

        # 一次性跑完
        full = WindowEngine(spec, self.state_dir("full"))
        full.run_events(events_path)
        expected = full.confirmed_view()

        # 中途暂停两次，重建引擎续跑
        paused = WindowEngine(spec, self.state_dir("paused"))
        paused.run_events(events_path, limit=5)
        paused = WindowEngine(spec, self.state_dir("paused"))  # 模拟重开进程
        paused.run_events(events_path, limit=4)
        paused = WindowEngine(spec, self.state_dir("paused"))
        paused.run_events(events_path)
        self.assertEqual(expected, paused.confirmed_view())
        self.assertEqual(full.tentative_view(), paused.tentative_view())

    def test_impact_log_sums_to_revision_diff(self):
        spec = make_spec(self.tmp)
        engine = WindowEngine(spec, self.state_dir())
        engine.apply_event(ev(1, "a", "2026-01-01T00:00:10Z", "c1", 100))
        engine.apply_event(ev(2, "b", "2026-01-01T00:00:20Z", "c1", 40))
        engine.advance_watermark("2026-01-01T00:01:00Z")
        engine.apply_event(ev(3, "a", "2026-01-01T00:00:10Z", "c1", 70))
        engine.apply_event({"op": "retract", "id": "b", "seq": 4})
        rev_dir = os.path.join(self.state_dir(), "revisions")
        changes = diff_revisions(os.path.join(rev_dir, "rev-00001.jsonl"),
                                 os.path.join(rev_dir, "rev-00003.jsonl"))
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0]["deltas"]["total"]["delta"], -70)
        # 逐笔影响合计 = 修订差额
        impacts = read_jsonl(engine.impact_path)
        per_event = [d["deltas"]["total"]["delta"]
                     for impact in impacts
                     for d in impact.get("window_deltas", {}).values()
                     if "total" in d["deltas"] and d["confirmed"]]
        self.assertEqual(sum(per_event), -70)


if __name__ == "__main__":
    unittest.main()

