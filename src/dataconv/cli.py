from __future__ import annotations

import argparse
import json
from pathlib import Path

from .engine import ExecutionEngine
from .models import Plan
from .storage import PlanStore, read_json


def main(argv=None):
    parser = argparse.ArgumentParser(prog="dataconv")
    parser.add_argument("--store", default="./data-store")
    sub = parser.add_subparsers(dest="command", required=True)
    save = sub.add_parser("save-plan")
    save.add_argument("plan_file")
    run = sub.add_parser("run")
    run.add_argument("plan_id")
    run.add_argument("--memory-bytes", type=int)
    run.add_argument("--run-id")
    args = parser.parse_args(argv)
    if args.command == "save-plan":
        plan = Plan.from_dict(read_json(Path(args.plan_file)))
        print(PlanStore(args.store).save(plan))
    else:
        plan = PlanStore(args.store).load(args.plan_id)
        run = ExecutionEngine(args.store).execute(plan, memory_bytes=args.memory_bytes, run_id=args.run_id)
        print(json.dumps(run.to_dict(), ensure_ascii=False, indent=2))
