"""转换计划：描述数据源与算子阶段，与运行实例分开持久化。"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any


@dataclass
class OperatorSpec:
    op: str
    params: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"op": self.op, **self.params}

    @classmethod
    def from_dict(cls, data: dict) -> "OperatorSpec":
        data = dict(data)
        return cls(op=data.pop("op"), params=data)


@dataclass
class TransformPlan:
    plan_id: str
    name: str
    version: int
    primary_source: str
    sources: dict[str, str]  # 源名称 -> JSONL 文件路径
    stages: list[OperatorSpec]

    def to_dict(self) -> dict:
        return {
            "plan_id": self.plan_id,
            "name": self.name,
            "version": self.version,
            "primary_source": self.primary_source,
            "sources": dict(self.sources),
            "stages": [s.to_dict() for s in self.stages],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TransformPlan":
        return cls(
            plan_id=data["plan_id"],
            name=data["name"],
            version=int(data["version"]),
            primary_source=data["primary_source"],
            sources=dict(data["sources"]),
            stages=[OperatorSpec.from_dict(s) for s in data["stages"]],
        )


class PlanStore:
    """计划仓库：plans/<plan_id>.json，原子写入。"""

    def __init__(self, root: str):
        self.root = root
        os.makedirs(root, exist_ok=True)

    def path_for(self, plan_id: str) -> str:
        return os.path.join(self.root, f"{plan_id}.json")

    def save(self, plan: TransformPlan) -> str:
        path = self.path_for(plan.plan_id)
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(plan.to_dict(), fh, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)
        return path

    def load(self, plan_id: str) -> TransformPlan:
        with open(self.path_for(plan_id), "r", encoding="utf-8") as fh:
            return TransformPlan.from_dict(json.load(fh))
