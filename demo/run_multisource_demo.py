"""多来源窗口演示：独立水位线、停发后恢复、双流连接与保存点续跑。

运行：python demo/run_multisource_demo.py
"""

import json
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dtsys.windowing import WindowEngine, diff_revisions

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                    "multisource_out")


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
    spec = {
        "streams": ["orders", "payments"],
        "primary_source": "orders",
        "sources": {},
        "stages": [],
        "window": {"time_field": "event_time", "size_seconds": 60,
                   "group_by": ["cust_id"],
                   "aggregates": {"total": "sum(amount)", "n": "count()"}},
    }
    orders = os.path.join(ROOT, "orders.jsonl")
    payments = os.path.join(ROOT, "payments.jsonl")
    write_jsonl(orders, [
        {"op": "upsert", "id": "o1", "seq": 1,
         "event_time": "2026-01-01T00:00:10Z", "cust_id": "c1", "amount": 100},
        {"op": "upsert", "id": "o2", "seq": 2,
         "event_time": "2026-01-01T00:00:40Z", "cust_id": "c1", "amount": 50},
        {"op": "watermark", "ts": "2026-01-01T00:05:00Z"},
        # 恢复后迟到的更正：o1 金额 100 -> 130（窗口已发布）
        {"op": "upsert", "id": "o1", "seq": 3,
         "event_time": "2026-01-01T00:00:10Z", "cust_id": "c1", "amount": 130},
    ])
    write_jsonl(payments, [
        {"op": "watermark", "ts": "2026-01-01T00:00:30Z"},
    ])

    state = os.path.join(ROOT, "state")
    engine = WindowEngine(spec, state)
    engine.run_events(orders, source="orders", limit=3)   # 先收 orders 三条
    engine.run_events(payments, source="payments")
    print("-- orders 已推进到 00:05，payments 只到 00:30 秒 --")
    show("暂定窗口（waiting_on 指出在等谁）", engine.tentative_view())
    show("各来源状态", [dict(source=s, **v)
                      for s, v in engine.source_status().items()])

    print("-- 显式停发 payments：暂时视为不再推进 --")
    impact = engine.set_stalled("payments", True)
    print("  新确认窗口:", impact["newly_confirmed"], "修订:", impact["revision"])
    show("已确认窗口", engine.confirmed_view())

    print("-- 重开进程：保存点还原各来源偏移与停发决定 --")
    engine = WindowEngine(spec, state)
    show("各来源状态", [dict(source=s, **v)
                      for s, v in engine.source_status().items()])

    print("-- payments 恢复推进，orders 迟到更正到达 --")
    engine.set_stalled("payments", False)
    engine.apply_event({"op": "watermark", "source": "payments",
                        "ts": "2026-01-01T00:05:00Z"})
    engine.run_events(orders, source="orders")            # 从偏移续跑
    show("已确认窗口（迟到更正后）", engine.confirmed_view())

    rev_dir = os.path.join(state, "revisions")
    revs = sorted(os.listdir(rev_dir))
    print("== 修订文件（旧文件永不改写）==")
    for name in revs:
        print("  " + name)
    for i in range(1, len(revs)):
        changes = diff_revisions(os.path.join(rev_dir, revs[i - 1]),
                                 os.path.join(rev_dir, revs[i]))
        if changes:
            show("差额 %s -> %s" % (revs[i - 1], revs[i]), changes)


if __name__ == "__main__":
    main()
