"""多来源窗口测试：停发/恢复、独立编号、连接完整性、修订依据与保存点。"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dtsys.windowing import WindowEngine


def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False))
            fh.write("\n")


def read_jsonl(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def upsert(source, seq, rid, ts, cust, amount=None, **extra):
    event = {"source": source, "op": "upsert", "id": rid, "seq": seq,
             "event_time": ts, "cust_id": cust}
    if amount is not None:
        event["amount"] = amount
    event.update(extra)
    return event


def wm(source, ts):
    return {"source": source, "op": "watermark", "ts": ts}


def spec(tmp, stages=None, source_files=None,
         stream_sources=None, fact_sources=None):
    source_files = source_files or {}
    paths = {}
    for name, rows in source_files.items():
        p = os.path.join(tmp, name + ".jsonl")
        write_jsonl(p, rows)
        paths[name] = p
    stages = stages or []
    stream_sources = stream_sources if stream_sources is not None else [
        "orders", "payments"] + [
            s["right_source"] for s in stages
            if s.get("op") == "join" and s["right_source"] not in paths]
    fact_sources = fact_sources if fact_sources is not None else \
        ["orders", "payments", "facts", "main"]
    return {
        "sources": paths,
        "fact_sources": fact_sources,
        "stream_sources": list(set(stream_sources)),
        "stages": stages,
        "window": {"time_field": "event_time", "size_seconds": 60,
                   "group_by": ["cust_id"],
                   "aggregates": {"total": "sum(amount)", "n": "count()"}},
    }


class TestMultiSource(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def engine(self, stages=None, dirname="state",
               stream_sources=None, fact_sources=None):
        return WindowEngine(spec(self.tmp, stages,
                                stream_sources=stream_sources,
                                fact_sources=fact_sources),
                            os.path.join(self.tmp, dirname))

    def test_silence_is_not_end_and_holds_name_source(self):
        e = self.engine()
        e.apply_event(upsert("orders", 1, "o1",
                             "2026-01-01T00:00:10Z", "c1", 100))
        e.advance_watermark("2026-01-01T00:05:00Z", source="orders")
        # payments 一条数据都没到：不能自动当作结束
        self.assertEqual(e.confirmed_view(), [])
        holds = e.window_holds()
        self.assertEqual(len(holds), 1)
        waiting = {w["source"] for w in holds[0]["waiting_for"]}
        self.assertIn("payments", waiting)
        # 显式停发 payments 后才能发布
        result = e.exclude_source("payments", reason="对方接口暂停")
        self.assertIsNotNone(result["revision"])
        confirmed = {(r["window_start"], r["cust_id"]): r
                     for r in e.confirmed_view()}
        self.assertEqual(
            confirmed[("2026-01-01T00:00:00Z", "c1")]["total"], 100)

    def test_publish_preview_changes_nothing(self):
        e = self.engine()
        e.apply_event(upsert("orders", 1, "o1",
                             "2026-01-01T00:00:10Z", "c1", 100))
        e.advance_watermark("2026-01-01T00:05:00Z", source="orders")
        preview = e.publish_preview(["payments"])
        self.assertEqual(len(preview["windows"]), 1)
        self.assertEqual(e.confirmed_view(), [])  # 预览不发布
        self.assertTrue(e.source_progress()["payments"]["active"])

    def test_resume_source_late_record_makes_based_revision(self):
        e = self.engine(dirname="s")
        e.apply_event(upsert("orders", 1, "o1",
                             "2026-01-01T00:00:10Z", "c1", 100))
        e.advance_watermark("2026-01-01T00:05:00Z", source="orders")
        r1 = e.exclude_source("payments")["revision"]
        rev_dir = os.path.join(self.tmp, "s", "revisions")
        first = os.path.join(rev_dir, "rev-%05d.jsonl" % r1)
        content_before = open(first, encoding="utf-8").read()

        e.resume_source("payments")
        impact = e.apply_event(upsert("payments", 1, "p1",
                                      "2026-01-01T00:00:20Z",
                                      "c1", 30))
        e.advance_watermark("2026-01-01T00:05:00Z", source="payments")
        # 旧修订文件没有被改写
        self.assertEqual(open(first, encoding="utf-8").read(),
                         content_before)
        self.assertIsNotNone(impact["revision"])
        basis = json.load(open(os.path.join(
            rev_dir, "rev-%05d.basis.json" % impact["revision"]),
            encoding="utf-8"))
        self.assertEqual(basis["basis_source"], "payments")
        self.assertEqual(basis["trigger"], "late_change")
        row = [r for r in e.confirmed_view()
               if r["window_start"] == "2026-01-01T00:00:00Z"][0]
        self.assertEqual(row["total"], 130)

    def test_same_id_in_different_sources_are_distinct(self):
        e = self.engine()
        e.apply_event(upsert("orders", 1, "x",
                             "2026-01-01T00:00:10Z", "c1", 100))
        e.apply_event(upsert("payments", 1, "x",
                             "2026-01-01T00:00:11Z", "c1", 20))
        # 撤回 orders.x 不能影响 payments.x
        e.apply_event({"source": "orders", "op": "retract",
                       "id": "x", "seq": 2})
        windows = {(r["window_start"], r["cust_id"]): r
                   for r in e.tentative_view()}
        self.assertEqual(
            windows[("2026-01-01T00:00:00Z", "c1")]["total"], 20)
        # payments.x 的重放被跳过
        replay = e.apply_event(upsert("payments", 1, "x",
                                      "2026-01-01T00:00:11Z",
                                      "c1", 20))
        self.assertEqual(replay.get("skipped"), "replay")

    def test_retract_then_arrive_after_exclusion(self):
        e = self.engine()
        e.apply_event(upsert("orders", 1, "o1",
                             "2026-01-01T00:00:10Z", "c1", 100))
        e.apply_event(upsert("orders", 2, "o2",
                             "2026-01-01T00:00:20Z", "c1", 40))
        e.apply_event({"source": "orders", "op": "retract",
                       "id": "o2", "seq": 3})
        e.advance_watermark("2026-01-01T00:05:00Z", source="orders")
        e.exclude_source("payments")
        row = [r for r in e.confirmed_view()
               if r["cust_id"] == "c1"][0]
        self.assertEqual(row["total"], 100)
        # o2 先撤回，之后补到一条更正重放语义：新 seq 生效
        e.resume_source("payments")
        e.apply_event(upsert("orders", 4, "o2",
                             "2026-01-01T00:00:20Z", "c1", 45))
        e.advance_watermark("2026-01-01T00:05:00Z", source="payments")
        row = [r for r in e.confirmed_view()
               if r["cust_id"] == "c1"][0]
        self.assertEqual(row["total"], 145)

    def test_two_stream_join_waits_for_slow_side(self):
        # 右侧 dims 是事件流（spec 中没有静态文件），窗口必须等其进度
        stages = [{"op": "join", "right_source": "dims",
                   "left_on": "cust_id", "how": "inner"}]
        e = self.engine(stages, stream_sources=["facts", "dims"], fact_sources=["facts"])
        e.apply_event(upsert("facts", 1, "f1",
                             "2026-01-01T00:00:10Z", "c1", 100))
        e.advance_watermark("2026-01-01T00:05:00Z", source="facts")
        e.advance_watermark("2026-01-01T00:05:00Z", source="dims")
        # 右来源水位线到了但没有合法匹配：inner join 结果为空，窗口不出现
        self.assertEqual(e.confirmed_view(), [])
        # 新引擎实例验证状态一致

    def test_two_stream_join_one_to_many_completeness(self):
        stages = [{"op": "join", "right_source": "dims",
                   "left_on": "cust_id", "how": "inner"}]
        e = self.engine(stages, dirname="join",
                        stream_sources=["facts", "dims"],
                        fact_sources=["facts"])
        e.apply_event(upsert("facts", 1, "f1",
                             "2026-01-01T00:00:10Z", "c1", 100))
        e.advance_watermark("2026-01-01T00:05:00Z", source="facts")
        # 快的一侧水位线再高也不能提前确认
        holds = e.window_holds()
        sources = {w["source"] for h in holds for w in h["waiting_for"]}
        self.assertIn("dims", sources)
        # 右来源先到一条合法重复
        e.apply_event(upsert("dims", 1, "d1",
                             "2026-01-01T00:00:05Z", "c1", 0, seg="A"))
        # 右来源水位线仍落后窗口结束：另一条匹配可能后到
        e.advance_watermark("2026-01-01T00:00:30Z", source="dims")
        self.assertEqual(e.confirmed_view(), [])
        e.apply_event(upsert("dims", 2, "d2",
                             "2026-01-01T00:00:06Z", "c1", 0, seg="B"))
        e.advance_watermark("2026-01-01T00:05:00Z", source="dims")
        row = [r for r in e.confirmed_view()
               if r["cust_id"] == "c1"][0]
        self.assertEqual(row["n"], 2)
        self.assertEqual(row["total"], 200)

    def test_savepoint_restores_positions_and_decisions(self):
        orders_path = os.path.join(self.tmp, "orders.jsonl")
        write_jsonl(orders_path, [
            upsert("orders", 1, "o1",
                   "2026-01-01T00:00:10Z", "c1", 100),
            wm("orders", "2026-01-01T00:05:00Z"),
        ])
        e = self.engine(dirname="cp")
        e.run_events(orders_path, limit=1)
        e.exclude_source("payments", reason="暂停")
        positions = e.accepted_positions()
        savepoint = e.export_savepoint(note="中段")

        e2 = WindowEngine(spec(self.tmp), os.path.join(self.tmp, "cp"))
        self.assertTrue(e2.sources["payments"]["excluded"])
        self.assertEqual(e2.accepted_positions(), positions)
        # 从接受位置续跑，只补剩余 1 条 watermark
        self.assertEqual(e2.run_events(orders_path), 1)
        sp = json.load(open(savepoint, encoding="utf-8"))
        self.assertEqual(sp["sources"]["payments"]["excluded"], True)
        self.assertEqual(
            sp["sources"]["orders"]["accepted_records"], 1)
        row = [r for r in e2.confirmed_view()
               if r["cust_id"] == "c1"][0]
        self.assertEqual(row["total"], 100)

    def test_impact_amounts_sum_to_published_diff(self):
        e = self.engine(dirname="imp")
        e.apply_event(upsert("orders", 1, "o1",
                             "2026-01-01T00:00:10Z", "c1", 100))
        e.advance_watermark("2026-01-01T00:05:00Z", source="orders")
        e.exclude_source("payments")
        e.resume_source("payments")
        e.apply_event(upsert("payments", 1, "p1",
                             "2026-01-01T00:00:20Z", "c1", 30))
        e.advance_watermark("2026-01-01T00:05:00Z", source="payments")
        impacts = read_jsonl(e.impact_path)
        total_delta = sum(
            d["deltas"]["total"]["delta"]
            for impact in impacts
            for d in impact.get("window_deltas", {}).values()
            if "total" in d["deltas"] and d.get("confirmed"))
        self.assertEqual(total_delta, 30)


if __name__ == "__main__":
    unittest.main()
