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

from .batches import Batch, jsonl_batches, merge_columns, write_jsonl, row_size
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
    resume: bool = False  # 从同一 run_id 已取消的保存点继续


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
        checkpoint_dir = os.path.join(run_dir, "checkpoints")
        temp_dir = os.path.join(run_dir, "tmp")
        staging_dir = os.path.join(run_dir, "output.staging")
        instance_path = os.path.join(run_dir, "instance.json")
        resume_from = None

        if config.resume:
            instance = self._load_instance(instance_path)
            if instance["status"] != RunStatus.CANCELLED.value:
                raise RuntimeError("只能从 cancelled 运行的保存点继续")
            if instance["plan_id"] != plan.plan_id or instance["plan_version"] != plan.version:
                raise RuntimeError("保存点与计划版本不一致")
            resume_from = instance.get("checkpoint", {}).get("completed_stages", 0)
            os.makedirs(checkpoint_dir, exist_ok=True)
            incomplete = os.path.join(checkpoint_dir, f"stage-{resume_from + 1:03d}")
            shutil.rmtree(incomplete, ignore_errors=True)
            shutil.rmtree(temp_dir, ignore_errors=True)
        elif os.path.exists(run_dir):
            raise FileExistsError(f"运行已存在，继续请使用 resume=True: {run_dir}")
        else:
            os.makedirs(run_dir, exist_ok=True)
        os.makedirs(temp_dir, exist_ok=True)

        chunk_bytes = config.chunk_bytes or max(1024, config.memory_bytes // 4)
        new_instance = {
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
            "buffer_budget": "resident row/state JSON bytes; staged chunk files are durable queues",
            "checkpoint": {"completed_stages": 0},
        }
        if config.resume:
            instance.update(new_instance)
            instance["status"] = RunStatus.RUNNING.value
            instance["error"] = None
            instance["finished_at"] = None
            instance["checkpoint"] = {"completed_stages": resume_from}
        else:
            instance = new_instance
            self._save_instance(instance_path, instance)

        try:
            result_file, row_count, columns, stats = self._run_pipeline(
                plan, temp_dir, checkpoint_dir, config.memory_bytes, chunk_bytes,
                token, resume_from or 0, instance_path,
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
                peak_inflight_bytes=max(
                    instance.get("peak_inflight_bytes", 0),
                    stats.get("join_peak_inflight_bytes", 0),
                    stats.get("aggregate_peak_inflight_bytes", 0),
                    stats.get("streaming_peak_inflight_bytes", 0),
                ),
            )
            self._save_instance(instance_path, instance)
            self._publish(plan, config.run_id)
            instance["status"] = RunStatus.PUBLISHED.value
            self._save_instance(instance_path, instance)
            shutil.rmtree(temp_dir, ignore_errors=True)
            shutil.rmtree(checkpoint_dir, ignore_errors=True)
            return instance
        except Exception as exc:
            status = (RunStatus.CANCELLED if token.cancelled
                      else RunStatus.FAILED)
            instance.update(
                status=status.value,
                finished_at=time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                error=f"{type(exc).__name__}: {exc}",
            )
            if os.path.exists(instance_path):
                persisted = self._load_instance(instance_path)
                instance["checkpoint"] = persisted.get(
                    "checkpoint", {"completed_stages": 0}
                )
                instance["peak_inflight_bytes"] = persisted.get(
                    "peak_inflight_bytes", instance.get("peak_inflight_bytes", 0)
                )
            elif "checkpoint" not in instance:
                instance["checkpoint"] = {"completed_stages": 0}
            self._save_instance(instance_path, instance)
            shutil.rmtree(temp_dir, ignore_errors=True)
            shutil.rmtree(staging_dir, ignore_errors=True)
            if status == RunStatus.FAILED:
                shutil.rmtree(checkpoint_dir, ignore_errors=True)
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

    def _run_pipeline(self, plan, temp_dir, checkpoint_dir, memory_bytes, chunk_bytes,
                      token, start_stage, instance_path):
        stats: dict = {}

        def resolve_source(name: str) -> str:
            if name not in plan.sources:
                raise KeyError(f"计划缺少数据源: {name}")
            return plan.sources[name]

        if start_stage >= len(plan.stages):
            manifest_path = os.path.join(
                checkpoint_dir, f"stage-{len(plan.stages):03d}", "manifest.json"
            )
            with open(manifest_path, "r", encoding="utf-8") as fh:
                manifest = json.load(fh)
            current_files = [
                os.path.join(os.path.dirname(manifest_path), path)
                for path in manifest["files"]
            ]
            columns = manifest["columns"]
        else:
            current_files = [plan.sources[plan.primary_source]]
            columns = self._scan_columns(current_files)

        for stage_no, spec in enumerate(plan.stages, start=1):
            if stage_no <= start_stage:
                stage_dir = os.path.join(checkpoint_dir, f"stage-{stage_no:03d}")
                with open(os.path.join(stage_dir, "manifest.json"),
                          "r", encoding="utf-8") as fh:
                    manifest = json.load(fh)
                current_files = [
                    os.path.join(stage_dir, path) for path in manifest["files"]
                ]
                columns = manifest["columns"]
                continue

            stage_dir = os.path.join(checkpoint_dir, f"stage-{stage_no:03d}")
            if os.path.exists(stage_dir):
                shutil.rmtree(stage_dir)
            os.makedirs(stage_dir, exist_ok=True)
            ctx = OperatorContext(
                run_dir=os.path.dirname(checkpoint_dir),
                temp_dir=stage_dir,
                work_bytes=memory_bytes,
                resolve_source=resolve_source,
                is_cancelled=lambda: token.cancelled,
                stats=stats,
            )
            operator = registry.build(spec.op, spec.params, ctx)
            if getattr(operator, "streaming", True):
                out_files = []
                new_columns: list[str] = []
                part = 0
                peak_batch_bytes = 0
                output_rows: list[dict] = []

                def flush_output_rows():
                    nonlocal part, output_rows
                    if not output_rows:
                        return
                    part += 1
                    rel_path = f"part-{part:05d}.jsonl"
                    write_jsonl(os.path.join(stage_dir, rel_path),
                                output_rows, new_columns)
                    out_files.append(rel_path)
                    output_rows = []

                for path in current_files:
                    for batch in jsonl_batches(path, chunk_bytes):
                        for row in batch.rows:
                            if token.cancelled:
                                raise RuntimeError("运行已取消")
                            input_batch = Batch([row], batch.columns)
                            output_bytes = sum(
                                row_size(item) for item in output_rows
                            )
                            peak_batch_bytes = max(
                                peak_batch_bytes,
                                input_batch.size + output_bytes,
                            )
                            result = operator.process_batch(input_batch)
                            result_bytes = result.size
                            if output_rows and output_bytes + result_bytes >= chunk_bytes:
                                flush_output_rows()
                                output_bytes = 0
                            new_columns = merge_columns(new_columns, result.rows)
                            if not new_columns:
                                new_columns = result.columns
                            output_rows.extend(result.rows)
                            if sum(row_size(item) for item in output_rows) >= chunk_bytes:
                                flush_output_rows()
                            peak_batch_bytes = max(
                                peak_batch_bytes,
                                input_batch.size + sum(
                                    row_size(item) for item in output_rows
                                ),
                            )
                flush_output_rows()
                columns = new_columns or columns
                stats["streaming_peak_inflight_bytes"] = max(
                    stats.get("streaming_peak_inflight_bytes", 0),
                    peak_batch_bytes,
                )
            else:
                produced_files, columns = operator.run(current_files, columns)
                out_files = []
                for index, path in enumerate(produced_files, start=1):
                    if os.path.dirname(os.path.abspath(path)) != os.path.abspath(stage_dir):
                        rel_path = f"blocked-{index:05d}.jsonl"
                        shutil.move(path, os.path.join(stage_dir, rel_path))
                    else:
                        rel_path = os.path.relpath(path, stage_dir)
                    out_files.append(rel_path)

            if token.cancelled:
                raise RuntimeError("运行已取消")
            current_files = [os.path.join(stage_dir, path) for path in out_files]
            with open(os.path.join(stage_dir, "manifest.json.tmp"),
                      "w", encoding="utf-8") as fh:
                json.dump({"op": spec.op, "files": out_files,
                           "columns": columns}, fh,
                          ensure_ascii=False, indent=2)
            os.replace(os.path.join(stage_dir, "manifest.json.tmp"),
                       os.path.join(stage_dir, "manifest.json"))
            instance = self._load_instance(instance_path)
            instance["checkpoint"] = {"completed_stages": stage_no}
            instance["peak_inflight_bytes"] = max(
                instance.get("peak_inflight_bytes", 0),
                stats.get("join_peak_inflight_bytes", 0),
                stats.get("aggregate_peak_inflight_bytes", 0),
                stats.get("streaming_peak_inflight_bytes", 0),
            )
            self._save_instance(instance_path, instance)

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
