"""多来源窗口演示。

覆盖：停发后恢复、先撤回后补到、跨窗口更正、两路流连接放大，
以及保存点还原接受位置与停发决定。

运行：python demo/run_multi_source_window_demo.py
"""

from __future__ import annotations

import json
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dtsys.windowing import WindowEngine

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                    "multi_source_window_out")


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
    os.makedirs(ROOT, exist_ok=True)
    dims_path = os.path.join(ROOT, "dims_seed.jsonl")
    write_jsonl(dims_path, [{"cust_id": "c1", "seg": "SEED"}])

    spec = {
        "sources": {"dims_seed": dims_path},
        "stream_sources": ["orders", "payments"],
        "fact_sources": ["orders", "payments"],
        "stages": [
            {"op": "filter", "expr": "amount >= 0"},
        ],
        "window": {"time_field": "event_time", "size_seconds": 60,
                   "group_by": ["cust_id"],
                   "aggregates": {"total": "sum(amount)", "n": "count()"}},
    }
    state = os.path.join(ROOT, "state")
    e = WindowEngine(spec, state)

    # 1) orders 推进很快，payments 没有任何数据 -> 窗口只能暂定
    e.apply_event({"source": "orders", "op": "upsert", "id": "o1", "seq": 1,
                   "event_time": "2026-01-01T00:00:10Z",
                   "cust_id": "c1", "amount": 100})
    e.apply_event({"source": "orders", "op": "upsert", "id": "o2", "seq": 2,
                   "event_time": "2026-01-01T00:00:20Z",
                   "cust_id": "c1", "amount": 40})
    e.advance_watermark("2026-01-01T00:05:00Z", source="orders")
    show("暂定窗口（payments 无声）", e.window_holds())

    # 2) 显式把 payments 暂时视为不推进 -> 可发布；先预览再执行
    preview = e.publish_preview(["payments"])
    show("停发预览", preview["windows"])
    e.exclude_source("payments", reason="对方接口维护")
    show("首次发布", e.confirmed_view())
    savepoint = e.export_savepoint(note="payments 停发中")

    # 3) 先撤回 o2，再补到同 id 的新 seq（不同语义版本）
    e.apply_event({"source": "orders", "op": "retract",
                   "id": "o2", "seq": 3})

    # 4) payments 恢复，迟到记录改动已发布窗口 -> 新修订，旧文件不动
    e.resume_source("payments")
    late = e.apply_event(
        {"source": "payments", "op": "upsert", "id": "p1", "seq": 1,
         "event_time": "2026-01-01T00:00:30Z",
         "cust_id": "c1", "amount": 25})
    e.apply_event(
        {"source": "orders", "op": "upsert", "id": "o2", "seq": 4,
         "event_time": "2026-01-01T00:00:20Z",
         "cust_id": "c1", "amount": 45})
    e.advance_watermark("2026-01-01T00:05:00Z", source="payments")
    print("迟到修订号:", late["revision"], "-> watermark 后形成新发布")
    show("恢复后已确认窗口", e.confirmed_view())

    # 5) 跨窗口更正：o1 挪到 00:01 窗口
    e.apply_event({"source": "orders", "op": "upsert", "id": "o1", "seq": 5,
                   "event_time": "2026-01-01T00:01:10Z",
                   "cust_id": "c1", "amount": 100})
    show("跨窗口更正后", e.confirmed_view())

    # 6) 保存点恢复：新进程重建，接受位置与停发历史仍可核对
    e2 = WindowEngine(spec, state)
    print("== 保存点 ==")
    print("  ", savepoint)
    print("  当前来源进度:",
          json.dumps(e2.source_progress(), ensure_ascii=False, sort_keys=True))
    print("  接受位置:",
          json.dumps(e2.accepted_positions(), ensure_ascii=False, sort_keys=True))
    rev_dir = os.path.join(state, "revisions")
    print("  修订文件:", sorted(os.listdir(rev_dir)))


if __name__ == "__main__":
    main()
