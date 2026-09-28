from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path

from .models import Plan, Run, utc_now
from .operators import OPERATORS, ExecutionContext
from .storage import RunStore, atomic_write_json


class ExecutionCancelled(RuntimeError):
    pass


class ExecutionEngine:
    def __init__(self, root, *, cancellation=None):
        self.root = Path(root)
        self.run_store = RunStore(self.root)
        self.cancellation = cancellation

    def execute(self, plan, *, memory_bytes=None, run_id=None):
        run = Run(
            id=run_id or uuid.uuid4().hex,
            plan_id=plan.id,
            plan_version=plan.version,
            memory_bytes=memory_bytes,
        )
        temp_dir = self.run_store.temp_dir(run.id)
        outputs_dir = temp_dir / "outputs"
        temp_dir.mkdir(parents=True, exist_ok=True)
        outputs_dir.mkdir(parents=True, exist_ok=True)
        run.started_at = utc_now()
        run.status = "running"
        self.run_store.save(run)

        try:
            self._validate(plan)
            produced = {}
            for stage in plan.stages:
                self._raise_if_cancelled(run.id)
                inputs = self._resolve_inputs(plan, stage.inputs, produced)
                output = outputs_dir / f"{stage.id}.jsonl"
                OPERATORS[stage.type].execute(
                    stage.params,
                    inputs,
                    output,
                    ExecutionContext(run.id, temp_dir, memory_bytes),
                )
                if stage.output:
                    produced[stage.output] = output
            run.result = self._publish(plan, produced)
            run.status = "succeeded"
        except (Exception, KeyboardInterrupt) as exc:
            run.status = "cancelled" if isinstance(exc, (ExecutionCancelled, KeyboardInterrupt)) else "failed"
            run.error = str(exc)
            raise
        finally:
            run.finished_at = utc_now()
            self.run_store.save(run)
            self.run_store.cleanup_temp(run.id)
        return run

    def _validate(self, plan):
        declared = set(plan.sources)
        for stage in plan.stages:
            if stage.type not in OPERATORS:
                raise ValueError(f"unknown operator: {stage.type}")
            for value in stage.inputs.values():
                if value not in declared:
                    raise ValueError(f"stage {stage.id} has unresolved input {value}")
            if stage.output:
                declared.add(stage.output)
        for name in plan.publish:
            if name not in declared:
                raise ValueError(f"cannot publish unknown dataset: {name}")

    def _resolve_inputs(self, plan, inputs, produced):
        resolved = {}
        for name, ref in inputs.items():
            if ref in produced:
                resolved[name] = produced[ref]
            elif ref in plan.sources:
                resolved[name] = Path(plan.sources[ref])
            else:
                raise ValueError(f"unknown dataset: {ref}")
        return resolved

    def _publish(self, plan, produced):
        release_dir = self.root / "releases" / plan.id / f"v{plan.version}"
        if release_dir.exists() and any(release_dir.iterdir()):
            raise FileExistsError(f"published version already exists: {plan.id} v{plan.version}")
        release_dir.mkdir(parents=True, exist_ok=True)
        result = {}
        for name in plan.publish:
            source = produced[name]
            staged = release_dir / f".{name}.jsonl.tmp"
            try:
                shutil.copyfile(source, staged)
                os.replace(staged, release_dir / f"{name}.jsonl")
                result[name] = str(release_dir / f"{name}.jsonl")
            except Exception:
                shutil.rmtree(release_dir, ignore_errors=True)
                raise
        try:
            atomic_write_json(release_dir / "manifest.json", {"plan": plan.to_dict(), "datasets": result})
        except Exception:
            shutil.rmtree(release_dir, ignore_errors=True)
            raise
        self._update_current(plan.id, release_dir, result)
        return result

    def _update_current(self, plan_id, release_dir, result):
        plan_release_root = self.root / "releases" / plan_id
        atomic_write_json(
            plan_release_root / ".current.tmp",
            {"version_dir": str(release_dir), "datasets": result},
        )
        os.replace(plan_release_root / ".current.tmp", plan_release_root / "current.json")

    def _raise_if_cancelled(self, run_id):
        if self.cancellation is not None and self.cancellation.is_cancelled(run_id):
            raise ExecutionCancelled("run cancelled")


class Cancellation:
    def __init__(self, root):
        self.root = Path(root) / "cancellations"

    def request(self, run_id):
        (self.root / run_id).mkdir(parents=True, exist_ok=True)
        (self.root / run_id / "cancel").touch()

    def is_cancelled(self, run_id):
        return (self.root / run_id / "cancel").exists()
