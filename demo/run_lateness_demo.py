"""宽松迟到策略与来源代际演示。

场景：orders 允许迟到 60 秒；payments 全程静默。先停发 payments
让 W0 发布，然后：
1. 超时事件被丢弃并记录；对历史范围建立修订研究后重新落窗；
2. orders 重启换代（序号从 1 重开），旧代际重放/水位线被隔离；
3. 重开进程后历史修订不被覆盖，结果一致。

运行：python demo/run_lateness_demo.py
"""

import json
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dtsys.windowing import WindowEngine, diff_revisions

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                    "lateness_out")


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
        "lateness": {"orders": 60},      # orders 允许迟到 60 秒
        "window": {"time_field": "event_time", "size_seconds": 60,
                   "group_by": ["cust_id"],
                   "aggregates": {"total": "sum(amount)", "n": "count()"}},
    }
    state = os.path.join(ROOT, "state")
    engine = WindowEngine(spec, state)

    print("-- payments 全程静默：W0 等不到它，只能暂定 --")
    engine.apply_event({"op": "upsert", "source": "orders", "id": "o1",
                        "seq": 1, "event_time": "2026-01-01T00:00:10Z",
                        "cust_id": "c1", "amount": 100})
    engine.apply_event({"op": "watermark", "source": "orders",
                        "ts": "2026-01-01T00:05:00Z"})
    show("暂定窗口（waiting_on 指出在等谁）", engine.tentative_view())

    print("-- 显式停发 payments：W0 发布为 rev-00001 --")
    engine.set_stalled("payments", True)
    show("已确认窗口", engine.confirmed_view())

    print("-- 迟到 280 秒的事件超过允许迟到 60 秒：被丢弃并记录 --")
    impact = engine.apply_event({"op": "upsert", "source": "orders",
                                 "id": "o2", "seq": 2,
                                 "event_time": "2026-01-01T00:00:30Z",
                                 "cust_id": "c1", "amount": 9})
    print("  dropped:", impact.get("dropped"),
          "lateness:", impact.get("lateness_seconds"))

    print("-- 对历史范围 [00:00, 00:01) 建立修订研究：放宽到 3600 秒 --")
    impact = engine.set_lateness("orders", 3600, scope="revision",
                                 window_range=("2026-01-01T00:00:00Z",
                                               "2026-01-01T00:01:00Z"))
    print("  重新落窗:", impact["reapplied"], "修订:", impact["revision"])
    show("修订依据（哪些来源与规则变化导致）",
         engine.revision_causes()[impact["revision"]])
    show("已确认窗口（修订研究后）", engine.confirmed_view())

    print("-- orders 重启换代：序号从 1 重新开始 --")
    engine.restart_source("orders")
    impact = engine.apply_event({"op": "upsert", "source": "orders",
                                 "id": "o3", "seq": 1,
                                 "event_time": "2026-01-01T00:00:20Z",
                                 "cust_id": "c1", "amount": 70})
    print("  新代际迟到记录 -> 修订:", impact["revision"])
    replay = engine.apply_event({"op": "upsert", "source": "orders",
                                 "id": "o1", "seq": 1, "generation": 0,
                                 "event_time": "2026-01-01T00:00:10Z",
                                 "cust_id": "c1", "amount": 100})
    print("  旧代际重放:", replay.get("skipped"))
    ignored = engine.apply_event({"op": "watermark", "source": "orders",
                                  "ts": "2026-01-01T00:09:00Z",
                                  "generation": 0})
    print("  旧代际水位线:", ignored.get("ignored"))

    print("-- 重开进程：历史修订不被覆盖，结果一致 --")
    engine = WindowEngine(spec, state)
    show("各来源状态", [dict(source=s, **v)
                      for s, v in engine.source_status().items()])
    show("已确认窗口", engine.confirmed_view())

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
