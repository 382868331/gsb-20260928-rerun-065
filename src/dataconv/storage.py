from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .models import Plan, Run


def atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


class PlanStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def path(self, plan_id: str) -> Path:
        return self.root / "plans" / f"{plan_id}.json"

    def save(self, plan: Plan, *, overwrite: bool = False) -> Path:
        target = self.path(plan.id)
        if target.exists() and not overwrite:
            existing = Plan.from_dict(read_json(target))
            if existing.version == plan.version:
                raise FileExistsError(f"plan version already exists: {plan.id} v{plan.version}")
        atomic_write_json(target, plan.to_dict())
        return target

    def load(self, plan_id: str) -> Plan:
        return Plan.from_dict(read_json(self.path(plan_id)))


class RunStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def dir(self, run_id: str) -> Path:
        return self.root / "runs" / run_id

    def path(self, run_id: str) -> Path:
        return self.dir(run_id) / "run.json"

    def temp_dir(self, run_id: str) -> Path:
        return self.dir(run_id) / "tmp"

    def save(self, run: Run) -> Path:
        atomic_write_json(self.path(run.id), run.to_dict())
        return self.path(run.id)

    def load(self, run_id: str) -> Run:
        return Run.from_dict(read_json(self.path(run_id)))

    def cleanup_temp(self, run_id: str) -> None:
        import shutil

        shutil.rmtree(self.temp_dir(run_id), ignore_errors=True)
