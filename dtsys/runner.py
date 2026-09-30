"""执行器：把转换计划跑成运行实例。

存储约定（root 为工作仓库根目录）：
  plans/<plan_id>/v<version>.json      计划各版本（不可变）
  plans/<plan_id>/current.json         当前版本指针
  runs/<plan_id>/<run_id>/instance.json 运行实例（与计划分开保存）
  runs/<plan_id>/<run_id>/tmp/         本次临时分块（失败/取消即清理）
  runs/<plan_id>/<run_id>/output/      成功后原子发布的结果，不再改动
  published/<plan_id>/v<version>.json  已发布版本指针，发布后永不覆盖
"""

from __future__ import annotations

import enum
import json
import os
import shutil
import time
from dataclasses import dataclass

from .batches import Batch, jsonl_batches, merge_columns, write_jsonl, read_jsonl
from .operators import registry
from .operators.base import OperatorContext
from .plan import TransformPlan


class RunStatus(str, enum.Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PUBLISHED = "published"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class RunConfig:
    run_id: str
    memory_bytes: int = 64 * 1024 * 1024
    chunk_bytes: int | None = None  # 单批目标大小，缺省取内存预算的 1/4


class CancellationToken:
    def __init__(self):
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    @property
    def cancelled(self) -> bool:
        return self._cancelled


class Runner:
    def __init__(self, store_root: str):
        self.root = store_root
        self.plans_dir = os.path.join(store_root, "plans")
        self.runs_dir = os.path.join(store_root, "runs")
        self.published_dir = os.path.join(store_root, "published")
        for path in (self.plans_dir, self.runs_dir, self.published_dir):
            os.makedirs(path, exist_ok=True)

    def run_dir(self, plan_id: str, run_id: str) -> str:
        return os.path.join(self.runs_dir, plan_id, run_id)

    def save_plan(self, plan: TransformPlan) -> str:
        """按版本不可变地保存计划，并更新 current 指针。"""
        plan_dir = os.path.join(self.plans_dir, plan.plan_id)
        os.makedirs(plan_dir, exist_ok=True)
        version_path = os.path.join(plan_dir, f"v{plan.version}.json")
        if os.path.exists(version_path):
            raise FileExistsError(f"计划版本已存在，不可覆盖: {version_path}")
        data = plan.to_dict()
        tmp = f"{version_path}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, version_path)
        current_path = os.path.join(plan_dir, "current.json")
        with open(f"{current_path}.tmp", "w", encoding="utf-8") as fh:
            json.dump({"version": plan.version}, fh, ensure_ascii=False, indent=2)
        os.replace(f"{current_path}.tmp", current_path)
        return version_path

    def load_plan(self, plan_id: str, version: int | None = None) -> TransformPlan:
        plan_dir = os.path.join(self.plans_dir, plan_id)
        if version is None:
            with open(os.path.join(plan_dir, "current.json"),
                      "r", encoding="utf-8") as fh:
                version = json.load(fh)["version"]
        with open(os.path.join(plan_dir, f"v{version}.json"),
                  "r", encoding="utf-8") as fh:
            return TransformPlan.from_dict(json.load(fh))

    def _load_instance(self, path: str) -> dict:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def _save_instance(self, path: str, instance: dict) -> None:
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(instance, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, path)

    def get_instance(self, plan_id: str, run_id: str) -> dict:
        return self._load_instance(
            os.path.join(self.run_dir(plan_id, run_id), "instance.json")
        )

    def execute(self, plan: TransformPlan, config: RunConfig,
                token: CancellationToken | None = None) -> dict:
        token = token or CancellationToken()
        run_dir = self.run_dir(plan.plan_id, config.run_id)
        temp_dir = os.path.join(run_dir, "tmp")
        staging_dir = os.path.join(run_dir, "output.staging")
        instance_path = os.path.join(run_dir, "instance.json")
        os.makedirs(temp_dir, exist_ok=True)

        chunk_bytes = config.chunk_bytes or max(1024, config.memory_bytes // 4)
        instance = {
            "run_id": config.run_id,
            "plan_id": plan.plan_id,
            "plan_version": plan.version,
            "status": RunStatus.RUNNING.value,
            "memory_bytes": config.memory_bytes,
            "chunk_bytes": chunk_bytes,
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "finished_at": None,
            "output": None,
            "row_count": 0,
            "error": None,
            "peak_inflight_bytes": 0,
        }
        self._save_instance(instance_path, instance)

        try:
            result_file, row_count, columns, stats = self._run_pipeline(
                plan, temp_dir, config.memory_bytes, chunk_bytes, token,
            )
            os.makedirs(staging_dir, exist_ok=True)
            final_output = os.path.join(staging_dir, "result.jsonl")
            shutil.move(result_file, final_output)
            with open(os.path.join(staging_dir, "manifest.json"),
                      "w", encoding="utf-8") as fh:
                json.dump(
                    {
                        "run_id": config.run_id,
                        "plan_id": plan.plan_id,
                        "plan_version": plan.version,
                        "row_count": row_count,
                        "columns": columns,
                        "result": "result.jsonl",
                    },
                    fh, ensure_ascii=False, indent=2,
                )
            os.replace(staging_dir, os.path.join(run_dir, "output"))
            instance.update(
                status=RunStatus.SUCCEEDED.value,
                finished_at=time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                output="output/result.jsonl",
                row_count=row_count,
                peak_inflight_bytes=stats.get("join_peak_inflight_bytes", 0),
            )
            self._save_instance(instance_path, instance)
            self._publish(plan, config.run_id)
            instance["status"] = RunStatus.PUBLISHED.value
            self._save_instance(instance_path, instance)
            shutil.rmtree(temp_dir, ignore_errors=True)
            return instance
        except Exception as exc:
            status = (RunStatus.CANCELLED if token.cancelled
                      else RunStatus.FAILED)
            instance.update(
                status=status.value,
                finished_at=time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                error=f"{type(exc).__name__}: {exc}",
            )
            self._save_instance(instance_path, instance)
            shutil.rmtree(temp_dir, ignore_errors=True)
            shutil.rmtree(staging_dir, ignore_errors=True)
            if token.cancelled:
                raise
            raise

    def _publish(self, plan: TransformPlan, run_id: str) -> None:
        pointer = os.path.join(
            self.published_dir, plan.plan_id, f"v{plan.version}.json"
        )
        os.makedirs(os.path.dirname(pointer), exist_ok=True)
        if os.path.exists(pointer):
            return  # 已发布版本不可变，绝不覆盖
        tmp = f"{pointer}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(
                {"plan_id": plan.plan_id, "version": plan.version,
                 "run_id": run_id},
                fh, ensure_ascii=False, indent=2,
            )
        os.replace(tmp, pointer)

    def published_result_path(self, plan_id: str, version: int) -> str:
        pointer = os.path.join(self.published_dir, plan_id, f"v{version}.json")
        with open(pointer, "r", encoding="utf-8") as fh:
            ref = json.load(fh)
        return os.path.join(
            self.run_dir(plan_id, ref["run_id"]), "output", "result.jsonl"
        )

    def _run_pipeline(self, plan, temp_dir, memory_bytes, chunk_bytes, token):
        stats: dict = {}

        def resolve_source(name: str) -> str:
            if name not in plan.sources:
                raise KeyError(f"计划缺少数据源: {name}")
            return plan.sources[name]

        ctx = OperatorContext(
            run_dir=os.path.dirname(temp_dir),
            temp_dir=temp_dir,
            work_bytes=memory_bytes,
            resolve_source=resolve_source,
            is_cancelled=lambda: token.cancelled,
            stats=stats,
        )

        current_files: list[str] = [plan.sources[plan.primary_source]]
        columns = self._scan_columns(current_files)
        stage_no = 0

        for spec in plan.stages:
            stage_no += 1
            operator = registry.build(spec.op, spec.params, ctx)
            stage_dir = os.path.join(temp_dir, f"stage-{stage_no}-{spec.op}")
            os.makedirs(stage_dir, exist_ok=True)
            if getattr(operator, "streaming", True):
                out_files = []
                new_columns: list[str] = []
                part = 0
                for path in current_files:
                    for batch in jsonl_batches(path, chunk_bytes):
                        if token.cancelled:
                            raise RuntimeError("运行已取消")
                        result = operator.process_batch(batch)
                        new_columns = merge_columns(new_columns, result.rows)
                        if not new_columns:
                            new_columns = result.columns
                        part += 1
                        out_path = os.path.join(stage_dir, f"part-{part:05d}.jsonl")
                        write_jsonl(out_path, result.rows, result.columns)
                        out_files.append(out_path)
                columns = new_columns or columns
                current_files = out_files
            else:
                current_files, columns = operator.run(current_files, columns)

        final_dir = os.path.join(temp_dir, "final")
        os.makedirs(final_dir, exist_ok=True)
        final_path = os.path.join(final_dir, "result.jsonl")
        row_count = 0
        with open(final_path + ".tmp", "w", encoding="utf-8") as out_fh:
            for path in current_files:
                with open(path, "r", encoding="utf-8") as fh:
                    for line in fh:
                        if line.strip():
                            if not line.endswith("\n"):
                                line += "\n"
                            out_fh.write(line)
                            row_count += 1
        os.replace(final_path + ".tmp", final_path)
        return final_path, row_count, columns, stats

    @staticmethod
    def _scan_columns(paths) -> list[str]:
        columns: list[str] = []
        for path in paths:
            for batch in jsonl_batches(path, 4096):
                columns = merge_columns(columns, batch.rows)
        return columns
