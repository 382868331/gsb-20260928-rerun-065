"""窗口引擎演示：乱序 + 跨窗口更正 + 连接放大 + 暂停续跑。

运行：python demo/run_window_demo.py
"""

import json
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dtsys.windowing import WindowEngine, diff_revisions

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "window_out")


def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False))
            fh.write("\n")


def show(title, rows):
    print("== %s ==" % title)
    for row in rows:
        print("  " + json.dumps(row, ensure_ascii=False, sort_keys=True))
    if not rows:
        print("  (空)")


def main():
    shutil.rmtree(ROOT, ignore_errors=True)
    customers = os.path.join(ROOT, "customers.jsonl")
    write_jsonl(customers, [{"cust_id": "c1", "seg": "VIP"},
                            {"cust_id": "c2", "seg": "STD"}])
    spec = {
        "sources": {"customers": customers},
        "stages": [
            {"op": "filter", "expr": "amount >= 10"},
            {"op": "join", "right_source": "customers", "left_on": "cust_id"},
        ],
        "window": {"time_field": "event_time", "size_seconds": 60,
                   "group_by": ["cust_id"],
                   "aggregates": {"total": "sum(amount)", "n": "count()"}},
    }

    # 乱序事件流：先到 00:02 的，再到 00:00 的
    events = [
        {"op": "upsert", "id": "e1", "seq": 1,
         "event_time": "2026-01-01T00:02:05Z", "cust_id": "c2", "amount": 20},
        {"op": "upsert", "id": "e2", "seq": 2,
         "event_time": "2026-01-01T00:00:10Z", "cust_id": "c1", "amount": 100},
        {"op": "upsert", "id": "e3", "seq": 3,
         "event_time": "2026-01-01T00:00:40Z", "cust_id": "c1", "amount": 50},
        {"op": "watermark", "ts": "2026-01-01T00:01:00Z"},
        # 已确认窗口被迟到更正：e2 改到 00:01 窗口、金额 80
        {"op": "upsert", "id": "e2", "seq": 4,
         "event_time": "2026-01-01T00:01:10Z", "cust_id": "c1", "amount": 80},
        # e3 被撤回
        {"op": "retract", "id": "e3", "seq": 5},
        {"op": "watermark", "ts": "2026-01-01T00:03:00Z"},
    ]
    events_path = os.path.join(ROOT, "events.jsonl")
    write_jsonl(events_path, events)

    state = os.path.join(ROOT, "state")
    engine = WindowEngine(spec, state)
    engine.run_events(events_path, limit=3)          # 处理受限：先跑 3 条
    print("已接收偏移:", engine.offset)
    show("暂定窗口（前 3 条后）", engine.tentative_view())

    engine = WindowEngine(spec, state)               # 模拟重开，继续消费
    engine.run_events(events_path)
    show("暂定窗口（全部处理后）", engine.tentative_view())
    show("已确认窗口", engine.confirmed_view())

    rev_dir = os.path.join(state, "revisions")
    revs = sorted(os.listdir(rev_dir))
    print("== 修订文件 ==")
    for name in revs:
        print("  " + name)
    for i in range(1, len(revs)):
        changes = diff_revisions(os.path.join(rev_dir, revs[i - 1]),
                                 os.path.join(rev_dir, revs[i]))
        show("差额 %s -> %s" % (revs[i - 1], revs[i]), changes)

    print("== 逐笔影响（impact.jsonl）==")
    with open(engine.impact_path, encoding="utf-8") as fh:
        for line in fh:
            impact = json.loads(line)
            if impact.get("op") == "watermark":
                continue
            print("  id=%s op=%s 连接增/删=%d/%d 窗口差额=%s 修订=%s" % (
                impact["id"], impact["op"],
                len(impact["join_added"]), len(impact["join_removed"]),
                json.dumps({k: v["deltas"] for k, v in
                            impact["window_deltas"].items()},
                           ensure_ascii=False),
                impact["revision"]))


if __name__ == "__main__":
    main()

