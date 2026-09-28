from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class Stage:
    id: str
    type: str
    output: str
    params: dict[str, Any] = field(default_factory=dict)
    inputs: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Stage":
        return cls(
            id=value["id"],
            type=value["type"],
            output=value["output"],
            params=dict(value.get("params", {})),
            inputs=dict(value.get("inputs", {})),
        )


@dataclass(frozen=True)
class Plan:
    id: str
    version: int
    sources: dict[str, str]
    stages: list[Stage]
    publish: list[str]

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Plan":
        return cls(
            id=value["id"],
            version=int(value["version"]),
            sources=dict(value["sources"]),
            stages=[Stage.from_dict(item) for item in value["stages"]],
            publish=list(value.get("publish", [])),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Run:
    id: str
    plan_id: str
    plan_version: int
    status: str = "pending"
    memory_bytes: int | None = None
    error: str | None = None
    created_at: str = field(default_factory=utc_now)
    started_at: str | None = None
    finished_at: str | None = None
    result: dict[str, str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Run":
        return cls(**value)
