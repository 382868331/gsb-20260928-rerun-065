"""事件时间窗口流引擎测试：乱序、更正/撤回、修订、连接放大、暂停续传。"""

from __future__ import annotations

import json
import os
import random
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dtsys import StreamEngine, WindowSpec, CancellationToken


def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False))
            fh.write("\n")


def read_jsonl(path):
    with open(path, "r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def reference_windows(events, spec_dict, right_rows):
    """纯 Python 参考模型：去重 -> 筛选 -> 连接 -> 事件时间窗口求和。"""
    size = spec_dict["window_size"]
    group_by = spec_dict.get("group_by", [])
    amount_field = spec_dict.get("amount_field", "amount")
    latest = {}
    for ev in events:
        key = (ev["source"], ev["id"])
        if ev["seq"] > latest.get(key, (0,))[0]:
            latest[key] = (ev["seq"], ev)
    windows = {}
    for _, ev in latest.values():
        if ev["op"] == "retract":
            continue
        rec = ev["record"]
        if spec_dict.get("filter") and not eval(spec_dict["filter"], {}, rec):
            continue
        matched = [rec]
        if spec_dict.get("join"):
            right_on = spec_dict["join"].get("right_on", spec_dict["join"]["left_on"])
            matched = []
            for right in right_rows:
                if repr(right.get(right_on)) == repr(rec.get(spec_dict["join"]["left_on"])):
                    merged = dict(rec)
                    for k, v in right.items():
                        if k != right_on and k not in merged:
                            merged[k] = v
                    matched.append(merged)
        for row in matched:
            start = row[spec_dict["time_field"]] - (row[spec_dict["time_field"]] % size)
            gkey = tuple(row.get(k) for k in group_by)
            windows[(start, gkey)] = windows.get((start, gkey), 0) + row[amount_field]
    return windows


def engine_windows(engine):
    return {(w["window_start"], tuple(w["group"].get(k) for k in engine.spec.group_by)): w["amount"]
            for w in engine.confirmed_windows() + engine.tentative_windows()
            if w["amount"] != 0}  # 全额撤空的窗口与参考模型一样不再出现


BASE_EVENTS = [
    {"source": "orders", "id": "o1", "seq": 1, "op": "upsert",
     "record": {"cust": "A", "ts": 5, "amount": 100}},
    {"source": "orders", "id": "o2", "seq": 1, "op": "upsert",
     "record": {"cust": "A", "ts": 15, "amount": 50}},
    {"source": "orders", "id": "o3", "seq": 1, "op": "upsert",
     "record": {"cust": "B", "ts": 25, "amount": 70}},
    {"source": "orders", "id": "o4", "seq": 1, "op": "upsert",
     "record": {"cust": "A", "ts": 35, "amount": 200}},
]

SPEC_DICT = {"stream_id": "s", "time_field": "ts", "window_size": 10,
             "group_by": ["cust"], "amount_field": "amount"}


class StreamWindowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.spec = WindowSpec.from_dict({**SPEC_DICT, "stream_id": "s"})

    def _engine(self, name="s", **overrides):
        spec = WindowSpec.from_dict({**SPEC_DICT, "stream_id": name, **overrides})
        return StreamEngine(os.path.join(self.tmp, "store"), spec)

    def _events_file(self, events, name="events.jsonl"):
        path = os.path.join(self.tmp, name)
        write_jsonl(path, events)
        return path

    def test_out_of_order_arrival_same_result(self):
        shuffled = list(BASE_EVENTS)
        random.Random(42).shuffle(shuffled)
        ordered_engine = self._engine("ordered")
        ordered_engine.ingest(self._events_file(BASE_EVENTS, "a.jsonl"))
        shuffled_engine = self._engine("shuffled")
        shuffled_engine.ingest(self._events_file(shuffled, "b.jsonl"))
        self.assertEqual(engine_windows(ordered_engine),
                         engine_windows(shuffled_engine))
        expected = reference_windows(BASE_EVENTS, SPEC_DICT, [])
        self.assertEqual(engine_windows(ordered_engine), expected)

    def test_tentative_then_confirmed(self):
        engine = self._engine()
        engine.ingest(self._events_file(BASE_EVENTS[:2]))  # 最大 ts=15
        # 水位线=15：窗口 [0,10) 已确认，[10,20) 仍暂定
        self.assertEqual([w["window_start"] for w in engine.confirmed_windows()], [0])
        tentative = engine.tentative_windows()
        self.assertEqual([w["window_start"] for w in tentative], [10])
        engine.ingest(self._events_file(BASE_EVENTS[2:], "more.jsonl"))  # ts 到 35
        confirmed = {w["window_start"] for w in engine.confirmed_windows()}
        self.assertIn(0, confirmed)   # [0,10) 已确认
        self.assertIn(10, confirmed)  # [10,20) 已确认
        self.assertIn(20, confirmed)  # [20,30) 已确认
        self.assertEqual([w["window_start"] for w in engine.tentative_windows()], [30])

    def test_cross_window_correction_creates_revision(self):
        engine = self._engine()
        engine.ingest(self._events_file(BASE_EVENTS))
        pub1 = engine.publication_dir(1)
        win_before = read_jsonl(os.path.join(pub1, "windows.jsonl"))
        # 更正 o1：从窗口 [0,10) 挪到 [20,30)，金额 100 -> 120；两窗口均已确认
        fix = [{"source": "orders", "id": "o1", "seq": 2, "op": "upsert",
                "record": {"cust": "A", "ts": 25, "amount": 120}}]
        engine.ingest(self._events_file(fix, "fix.jsonl"))
        pub2 = engine.publication_dir(2)
        revisions = read_jsonl(os.path.join(pub2, "revisions.jsonl"))
        self.assertEqual(len(revisions), 2)
        by_start = {r["window_start"]: r for r in revisions}
        # 逐笔核对旧版/新版/差额
        self.assertEqual(by_start[0]["old_amount"], 100)
        self.assertEqual(by_start[0]["new_amount"], 0)
        self.assertEqual(by_start[0]["delta"], -100)
        self.assertEqual(by_start[20]["old_amount"], 0)
        self.assertEqual(by_start[20]["new_amount"], 120)
        self.assertEqual(by_start[20]["delta"], 120)
        for r in revisions:
            self.assertEqual(r["cause"]["id"], "o1")
            self.assertEqual(r["revision"], 1)
        # 旧发布文件未被改写
        self.assertEqual(read_jsonl(os.path.join(pub1, "windows.jsonl")), win_before)
        # 当前确认窗口反映修订后的值
        current = {(w["window_start"], w["group"]["cust"]): w["amount"]
                   for w in engine.confirmed_windows()}
        self.assertEqual(current[(0, "A")], 0)
        self.assertEqual(current[(20, "A")], 120)
        self.assertEqual(current[(20, "B")], 70)
        self.assertEqual(engine_windows(engine),
                         reference_windows(BASE_EVENTS + fix, SPEC_DICT, []))

    def test_retract_removes_contribution(self):
        engine = self._engine()
        pacer = {"source": "orders", "id": "pacer", "seq": 1, "op": "upsert",
                 "record": {"cust": "Z", "ts": 45, "amount": 1}}
        engine.ingest(self._events_file(BASE_EVENTS + [pacer]))  # 窗口 30 已确认
        retract = [{"source": "orders", "id": "o4", "seq": 2, "op": "retract"}]
        engine.ingest(self._events_file(retract, "retract.jsonl"))
        revisions = read_jsonl(os.path.join(engine.publication_dir(2), "revisions.jsonl"))
        self.assertEqual(len(revisions), 1)
        self.assertEqual(revisions[0]["window_start"], 30)
        self.assertEqual(revisions[0]["delta"], -200)
        self.assertEqual(engine_windows(engine),
                         reference_windows(BASE_EVENTS + [pacer] + retract,
                                           SPEC_DICT, []))

    def test_replay_not_counted_twice(self):
        engine = self._engine()
        path = self._events_file(BASE_EVENTS)
        engine.ingest(path)
        engine.ingest(path)  # 整体重放
        engine.ingest(path)
        self.assertEqual(engine_windows(engine),
                         reference_windows(BASE_EVENTS, SPEC_DICT, []))
        manifest = json.load(open(os.path.join(
            engine.publication_dir(3), "manifest.json"), encoding="utf-8"))
        self.assertEqual(manifest["events_applied"], 0)
        self.assertEqual(manifest["events_skipped_replay"], len(BASE_EVENTS))

    def test_empty_input(self):
        engine = self._engine()
        manifest = engine.ingest(self._events_file([]))
        self.assertEqual(manifest["events_applied"], 0)
        self.assertEqual(engine.confirmed_windows(), [])
        self.assertEqual(engine.tentative_windows(), [])
        for name in ("windows.jsonl", "tentative.jsonl", "revisions.jsonl"):
            self.assertEqual(read_jsonl(os.path.join(
                engine.publication_dir(1), name)), [])

    def test_join_amplification_and_correction_report(self):
        right = [{"cust": "A", "seg": "vip"}, {"cust": "A", "seg": "std"}]
        right_path = os.path.join(self.tmp, "dim.jsonl")
        write_jsonl(right_path, right)
        spec = {**SPEC_DICT, "stream_id": "j",
                "join": {"right_source": right_path, "left_on": "cust"}}
        engine = self._engine("j", join=spec["join"])
        engine.ingest(self._events_file(BASE_EVENTS))
        # A 的记录每条匹配 2 个右行：合法重复都计入
        amounts = {start: amt for (start, _), amt in engine_windows(engine).items()}
        self.assertEqual(amounts[0], 200)   # 100 × 2
        self.assertEqual(amounts[10], 100)  # 50 × 2
        self.assertNotIn(20, amounts)       # B 无匹配 -> inner 连接掉
        self.assertEqual(amounts[30], 400)  # 200 × 2
        # 更正 o2 的金额：修订应报告影响到的两个连接结果
        fix = [{"source": "orders", "id": "o2", "seq": 2, "op": "upsert",
                "record": {"cust": "A", "ts": 15, "amount": 80}}]
        engine.ingest(self._events_file(fix, "jfix.jsonl"))
        revisions = read_jsonl(os.path.join(engine.publication_dir(2), "revisions.jsonl"))
        self.assertEqual(len(revisions), 4)  # 撤 2 份旧贡献 + 上 2 份新贡献
        deltas = sorted(r["delta"] for r in revisions)
        self.assertEqual(deltas, [-50, -50, 80, 80])
        join_keys = {r["join_key"] for r in revisions}
        self.assertEqual(join_keys, {"A"})
        self.assertEqual(engine_windows(engine),
                         reference_windows(BASE_EVENTS + fix, spec, right))

    def test_filter_composes_with_window(self):
        engine = self._engine("f", filter="amount >= 100")
        engine.ingest(self._events_file(BASE_EVENTS))
        amounts = {start: amt for (start, _), amt in engine_windows(engine).items()}
        self.assertEqual(amounts, {0: 100, 30: 200})  # 50/70 被筛选掉

    def test_pause_and_resume_matches_uninterrupted(self):
        events = list(BASE_EVENTS) + [
            {"source": "orders", "id": f"x{i}", "seq": 1, "op": "upsert",
             "record": {"cust": "C", "ts": 40 + i, "amount": i}}
            for i in range(20)
        ]
        path = self._events_file(events, "long.jsonl")
        paused = self._engine("paused")
        m1 = paused.ingest(path, max_events=7)   # 模拟资源受限/用户暂停
        self.assertTrue(m1["paused"])
        paused.ingest(path, max_events=5)        # 续传：已应用位置被跳过
        paused.ingest(path)                      # 收尾
        whole = self._engine("whole")
        whole.ingest(path)
        self.assertEqual(engine_windows(paused), engine_windows(whole))
        self.assertEqual(engine_windows(paused),
                         reference_windows(events, SPEC_DICT, []))
        # 暂停后未完成窗口仍在（续传前可查）
        self.assertTrue(paused.confirmed_windows())

    def test_cancel_token_checkpoints_and_resumes(self):
        path = self._events_file(BASE_EVENTS)
        engine = self._engine("cancelled")
        token = CancellationToken()
        original = StreamEngine._apply_event
        calls = {"n": 0}

        def cancel_after_two(self, event, revisions):
            result = original(self, event, revisions)
            calls["n"] += 1
            if calls["n"] >= 2:
                token.cancel()  # 下一循环迭代检测到取消 -> 落盘后抛出
            return result

        StreamEngine._apply_event = cancel_after_two
        try:
            with self.assertRaises(RuntimeError):
                engine.ingest(path, token=token)
        finally:
            StreamEngine._apply_event = original
        # 取消后状态已落盘：新实例从已接收位置继续
        resumed = self._engine("cancelled")
        resumed.ingest(path)
        whole = self._engine("whole2")
        whole.ingest(path)
        self.assertEqual(engine_windows(resumed), engine_windows(whole))

    def test_publications_never_overwritten(self):
        engine = self._engine()
        engine.ingest(self._events_file(BASE_EVENTS))
        snapshots = {}
        for name in ("windows.jsonl", "tentative.jsonl", "revisions.jsonl",
                     "manifest.json"):
            path = os.path.join(engine.publication_dir(1), name)
            with open(path, "rb") as fh:
                snapshots[name] = fh.read()
        fix = [{"source": "orders", "id": "o1", "seq": 2, "op": "upsert",
                "record": {"cust": "A", "ts": 5, "amount": 999}}]
        engine.ingest(self._events_file(fix, "fix2.jsonl"))
        for name, content in snapshots.items():
            with open(os.path.join(engine.publication_dir(1), name), "rb") as fh:
                self.assertEqual(fh.read(), content, name)
        self.assertTrue(os.path.isdir(engine.publication_dir(2)))


if __name__ == "__main__":
    unittest.main()
