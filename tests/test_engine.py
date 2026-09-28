from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dataconv.engine import Cancellation, ExecutionEngine
from dataconv.models import Plan, Stage
from dataconv.storage import RunStore


def read_jsonl(path):
    return [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines()]


def write_jsonl(path, rows):
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def make_plan(root, version):
    orders = root / "orders.jsonl"
    customers = root / "customers.jsonl"
    write_jsonl(
        orders,
        [
            {"order_id": "o1", "customer_id": "c1", "amount": 100, "status": "active"},
            {"order_id": "o2", "customer_id": "c2", "amount": 50, "status": "inactive"},
            {"order_id": "o3", "customer_id": "c1", "amount": 20, "status": "active"},
            {"order_id": "o4", "customer_id": "missing", "amount": 9, "status": "active"},
        ],
    )
    write_jsonl(
        customers,
        [
            {"customer_id": "c1", "name": "Ada", "tier": "silver"},
            {"customer_id": "c1", "name": "Ada", "tier": "gold"},
            {"customer_id": "c2", "name": "Bo", "tier": "bronze"},
        ],
    )
    return Plan(
        id="orders-demo",
        version=version,
        sources={"orders": str(orders), "customers": str(customers)},
        stages=[
            Stage(
                id="active-orders",
                type="filter",
                output="active_orders",
                params={"expression": "status == 'active'"},
                inputs={"input": "orders"},
            ),
            Stage(
                id="with-total",
                type="derive",
                output="orders_with_total",
                params={"fields": {"total": "amount * 2"}},
                inputs={"input": "active_orders"},
            ),
            Stage(
                id="enrich-customers",
                type="join",
                output="enriched_orders",
                params={"keys": ["customer_id"], "how": "inner"},
                inputs={"left": "orders_with_total", "right": "customers"},
            ),
        ],
        publish=["enriched_orders"],
    )


class ExecutionEngineTest(unittest.TestCase):
    def test_normal_and_forced_small_memory_match(self):
        root = Path(self.id().replace(".", "/"))
        import tempfile

        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            normal_store = base / "normal"
            tiny_store = base / "tiny"

            normal_run = ExecutionEngine(normal_store).execute(make_plan(base, 1))
            tiny_run = ExecutionEngine(tiny_store).execute(make_plan(base, 2), memory_bytes=64)

            normal_rows = read_jsonl(normal_run.result["enriched_orders"])
            tiny_rows = read_jsonl(tiny_run.result["enriched_orders"])
            self.assertEqual(normal_rows, tiny_rows)
            self.assertEqual([row["order_id"] for row in normal_rows], ["o1", "o1", "o3", "o3"])
            self.assertEqual([row["tier"] for row in normal_rows], ["silver", "gold", "silver", "gold"])
            self.assertNotIn("o4", [row["order_id"] for row in normal_rows])

    def test_failure_and_cancellation_clean_temp_and_keep_release(self):
        import tempfile

        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            store = base / "store"
            engine = ExecutionEngine(store)
            run = engine.execute(make_plan(base, 1))
            published = Path(run.result["enriched_orders"])
            published_before = published.read_text(encoding="utf-8")

            cancellation = Cancellation(store)
            cancellation.request("cancelled-run")
            cancelled_engine = ExecutionEngine(store, cancellation=cancellation)
            with self.assertRaises(Exception):
                cancelled_engine.execute(make_plan(base, 2), run_id="cancelled-run")

            bad_plan = Plan(
                id="bad",
                version=1,
                sources={"orders": str(base / "orders.jsonl")},
                stages=[
                    Stage(
                        id="bad-stage",
                        type="missing",
                        output="bad",
                        params={},
                        inputs={"input": "orders"},
                    )
                ],
                publish=["bad"],
            )
            with self.assertRaises(ValueError):
                ExecutionEngine(store).execute(bad_plan, run_id="failed-run")

            self.assertFalse(RunStore(store).temp_dir("cancelled-run").exists())
            self.assertFalse(RunStore(store).temp_dir("failed-run").exists())
            self.assertEqual(published.read_text(encoding="utf-8"), published_before)
            self.assertEqual(RunStore(store).load("cancelled-run").status, "cancelled")
            self.assertEqual(RunStore(store).load("failed-run").status, "failed")


if __name__ == "__main__":
    unittest.main()
