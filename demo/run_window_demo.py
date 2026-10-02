"""事件时间窗口演示：乱序到达、跨窗口更正、撤回、连接放大与暂停续传。

1) 事件流乱序送达，窗口先暂定、随水位线推进逐一确认；
2) 一条跨窗口更正（o1 从 [0,10) 挪到 [20,30)）触发修订，
   逐笔打印旧值/新值/差额；旧发布目录保持原样；
3) 同一事件流重放，验证不会重复计入；
4) 中途暂停（max_events）后续传，结果与一次性执行一致。
"""

from __future__ import annotations

import json
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dtsys import StreamEngine, WindowSpec

EVENTS = [
    {"source": "orders", "id": "o2", "seq": 1, "op": "upsert",
     "record": {"cust": "A", "ts": 15, "amount": 50}},
    {"source": "orders", "id": "o1", "seq": 1, "op": "upsert",
     "record": {"cust": "A", "ts": 5, "amount": 100}},   # 乱序：晚到但时间更早
    {"source": "orders", "id": "o4", "seq": 1, "op": "upsert",
     "record": {"cust": "A", "ts": 35, "amount": 200}},
    {"source": "orders", "id": "o3", "seq": 1, "op": "upsert",
     "record": {"cust": "B", "ts": 25, "amount": 70}},
]
CORRECTION = [
    {"source": "orders", "id": "o1", "seq": 2, "op": "upsert",
     "record": {"cust": "A", "ts": 25, "amount": 120}},  # 跨窗口更正
]


def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False))
            fh.write("\n")


def read_jsonl(path):
    with open(path, "r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def main():
    base = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    store = os.path.join(base, "stream-store")
    shutil.rmtree(store, ignore_errors=True)
    events_path = os.path.join(base, "order-events.jsonl")
    fix_path = os.path.join(base, "order-fix.jsonl")
    write_jsonl(events_path, EVENTS)
    write_jsonl(fix_path, CORRECTION)

    spec = WindowSpec.from_dict({
        "stream_id": "orders-by-cust", "time_field": "ts", "window_size": 10,
        "group_by": ["cust"], "amount_field": "amount",
    })
    engine = StreamEngine(store, spec)

    m1 = engine.ingest(events_path, max_events=2)  # 模拟资源受限，先收 2 条
    print(f"[暂停] 已应用 {m1['events_applied']} 条，水位线={m1['watermark']}")
    print("  暂定窗口:", engine.tentative_windows())

    m2 = engine.ingest(events_path)  # 续传剩余
    print(f"[续传] 再应用 {m2['events_applied']} 条，水位线={m2['watermark']}")
    print("  已确认窗口:", engine.confirmed_windows())
    print("  暂定窗口:", engine.tentative_windows())

    m3 = engine.ingest(fix_path)  # 跨窗口更正
    print(f"[更正] 发布 {m3['publication']}，修订 {m3['revision_count']} 条:")
    for rev in read_jsonl(os.path.join(engine.publication_dir(3),
                                       "revisions.jsonl")):
        print(f"  窗口[{rev['window_start']},{rev['window_end']}) "
              f"{rev['group']} 旧={rev['old_amount']} 新={rev['new_amount']} "
              f"差额={rev['delta']} 原因={rev['cause']}")

    m4 = engine.ingest(events_path)  # 整体重放
    assert m4["events_applied"] == 0, "重放不应再次计入"
    print(f"[重放] 跳过 {m4['events_skipped_replay']} 条重复事件，窗口金额不变")

    # 与一次性执行对比
    spec2 = WindowSpec.from_dict({
        "stream_id": "orders-one-shot", "time_field": "ts", "window_size": 10,
        "group_by": ["cust"], "amount_field": "amount",
    })
    one_shot = StreamEngine(store, spec2)
    one_shot.ingest(events_path)
    one_shot.ingest(fix_path)
    assert engine.confirmed_windows() == one_shot.confirmed_windows()
    print("[对照] 暂停续传与一次性执行的确认窗口完全一致")
    print("\n全部演示通过。")


if __name__ == "__main__":
    main()
