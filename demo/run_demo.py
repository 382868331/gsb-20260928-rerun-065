"""端到端演示：筛选 -> 字段推导 -> 两份数据连接。

1) 小数据直接计算出期望结果（reference）；
2) 正常内存预算执行同一计划；
3) 强制极小内存预算执行同一计划（触发分块与临时落盘）；
4) 三者按多重集对比，必须完全一致。
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dtsys import OperatorSpec, Runner, RunConfig, TransformPlan


ORDERS = [
    {"order_id": "o1", "cust_id": "c1", "amount": 120.0},
    {"order_id": "o2", "cust_id": "c2", "amount": 30.0},   # 被筛选掉
    {"order_id": "o3", "cust_id": "c1", "amount": 200.0},   # c1 重复
    {"order_id": "o4", "cust_id": "c3", "amount": 88.5},
    {"order_id": "o5", "cust_id": "c4", "amount": 500.0},   # 无匹配客户
]

CUSTOMERS = [
    {"cust_id": "c1", "name": "Alice", "level": "VIP"},
    {"cust_id": "c1", "name": "Alice-Old", "level": "STD"},  # 右表重复键
    {"cust_id": "c2", "name": "Bob", "level": "STD"},
    {"cust_id": "c3", "name": "Cara", "level": "VIP"},
]


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False))
            fh.write("\n")


def reference_result():
    """Python 直接计算的期望值。"""
    rows = []
    for order in ORDERS:
        if order["amount"] < 100:
            continue
        enriched = dict(order)
        enriched["taxed"] = round(order["amount"] * 1.1, 2)
        enriched["tier"] = "高价值" if order["amount"] >= 200 else "普通"
        matched = False
        for cust in CUSTOMERS:
            if cust["cust_id"] == order["cust_id"]:
                merged = dict(enriched)
                merged["name"] = cust["name"]
                merged["level"] = cust["level"]
                rows.append(merged)
                matched = True
        if not matched:
            continue
    return rows


def read_jsonl(path):
    with open(path, "r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]

def build_plan(orders_path, customers_path):
    return TransformPlan(
        plan_id="orders-enrichment",
        name="订单筛选+推导+客户信息连接",
        version=1,
        primary_source="orders",
        sources={"orders": orders_path, "customers": customers_path},
        stages=[
            OperatorSpec("filter", {"expr": "amount >= 100"}),
            OperatorSpec("derive", {
                "fields": {
                    "taxed": "round(amount * 1.1, 2)",
                    "tier": "'高价值' if amount >= 200 else '普通'",
                },
            }),
            OperatorSpec("join", {
                "right_source": "customers",
                "left_on": "cust_id",
                "right_on": "cust_id",
                "how": "inner",
            }),
        ],
    )


def multiset(rows):
    return sorted(json.dumps(r, ensure_ascii=False, sort_keys=True) for r in rows)


def main():
    base = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(base, exist_ok=True)
    orders_path = os.path.join(base, "orders.jsonl")
    customers_path = os.path.join(base, "customers.jsonl")
    write_jsonl(orders_path, ORDERS)
    write_jsonl(customers_path, CUSTOMERS)

    store = os.path.join(base, "store")
    if os.path.exists(store):
        import shutil
        shutil.rmtree(store)
    runner = Runner(store)
    plan = build_plan(orders_path, customers_path)
    runner.save_plan(plan)

    expected = reference_result()
    print(f"[reference] 期望结果 {len(expected)} 行")

    normal = runner.execute(plan, RunConfig(run_id="run-normal",
                                            memory_bytes=1 << 20))
    normal_rows = read_jsonl(
        os.path.join(runner.run_dir(plan.plan_id, "run-normal"),
                     "output", "result.jsonl"))
    assert multiset(normal_rows) == multiset(expected), "正常内存结果不符"
    print(f"[normal]    {len(normal_rows)} 行，状态={normal['status']}")

    tiny = runner.execute(plan, RunConfig(run_id="run-tiny-memory",
                                          memory_bytes=256,
                                          chunk_bytes=128))
    tiny_rows = read_jsonl(
        os.path.join(runner.run_dir(plan.plan_id, "run-tiny-memory"),
                     "output", "result.jsonl"))
    assert multiset(tiny_rows) == multiset(expected), "小内存结果不符"
    print(f"[tiny-mem]  {len(tiny_rows)} 行，状态={tiny['status']} "
          f"(预算 256 字节，单批 128 字节)")

    published = read_jsonl(runner.published_result_path(plan.plan_id, 1))
    assert multiset(published) == multiset(expected)
    print("[publish]   已发布版本结果一致")
    print("\n结果明细:")
    for row in normal_rows:
        print(" ", row)
    print("\n全部校验通过：直接计算 / 正常内存 / 强制小内存 结果一致。")


if __name__ == "__main__":
    main()
