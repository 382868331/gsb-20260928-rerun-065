"""事件时间分组窗口流引擎：乱序、撤回/更正、暂定/确认窗口与修订发布。

与批式执行器（runner.py）互补：本引擎面向**变更流**（changelog），
事件逐条到达、可乱序、可撤回（retract）或更正（upsert 覆盖旧 seq）。

事件格式（JSONL 每行一条）：
  {"source": "orders", "id": "o1", "seq": 3,
   "op": "upsert" | "retract", "record": {...含事件时间字段...}}

窗口规格（WindowSpec）：
  stream_id         流标识（状态与发布目录名）
  time_field        记录中的事件时间字段（数值，如秒级时间戳）
  window_size       滚动窗口长度（与事件时间同单位）
  group_by          分组字段列表
  amount_field      聚合金额字段（求和）
  allowed_lateness  允许的乱序程度：水位线 = 已见最大事件时间 - 该值
  filter            可选筛选表达式（与 filter 算子同一白名单语法）
  join              可选连接：{"right_source": 路径, "left_on": ..., "right_on": ...}

语义约定：
- 同一 (source, id) 只应用更大的 seq；重放（seq <= 已应用）直接跳过，
  不会因重放再次计入；
- 连接为一对多时，每个匹配都产生一份合法贡献（保留合法重复），
  撤回/更正时按贡献逐份撤销；
- 水位线未越过窗口右端 => 窗口为**暂定**（tentative），每次发布可见；
  水位线越过 => 窗口**确认**（confirmed），进入 windows.jsonl；
- 已确认窗口再受后到/更正影响 => 产生一条**修订**（revision），
  记录旧值/新值/差额与影响到的连接结果；旧发布文件永不改写，
  每次 ingest 生成新的 pub-<n>/ 目录；
- 状态（已应用位置、水位线、未完成窗口、逐记录贡献）全部落盘，
  暂停（max_events 预算或 CancellationToken）后可继续。

存储布局（store_root/streams/<stream_id>/）：
  state/state.json            位置、水位线、窗口与贡献、发布计数
  publications/pub-<n>/windows.jsonl     本次新确认的窗口
  publications/pub-<n>/tentative.jsonl   当前全部暂定窗口快照
  publications/pub-<n>/revisions.jsonl   对已确认窗口的修订
  publications/pub-<n>/manifest.json     本次发布统计
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from typing import Any

from .expr import Expression


def _atomic_write_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _atomic_write_jsonl(path: str, rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, default=str))
            fh.write("\n")
    os.replace(tmp, path)


def _read_jsonl(path: str) -> list[dict]:
    rows = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


@dataclass
class WindowSpec:
    stream_id: str
    time_field: str
    window_size: float
    group_by: list[str] = field(default_factory=list)
    amount_field: str = "amount"
    allowed_lateness: float = 0
    filter: str | None = None
    join: dict | None = None  # right_source/left_on/right_on

    @classmethod
    def from_dict(cls, data: dict) -> "WindowSpec":
        return cls(
            stream_id=data["stream_id"],
            time_field=data["time_field"],
            window_size=data["window_size"],
            group_by=list(data.get("group_by", [])),
            amount_field=data.get("amount_field", "amount"),
            allowed_lateness=data.get("allowed_lateness", 0),
            filter=data.get("filter"),
            join=data.get("join"),
        )


def _window_start(ts: float, size: float) -> float:
    return ts - (ts % size)


class StreamEngine:
    """变更流 -> 事件时间分组窗口 的增量引擎，状态全部可持久化续传。"""

    def __init__(self, store_root: str, spec: WindowSpec):
        self.spec = spec
        self.base_dir = os.path.join(store_root, "streams", spec.stream_id)
        self.state_path = os.path.join(self.base_dir, "state", "state.json")
        self.pubs_dir = os.path.join(self.base_dir, "publications")
        self.filter_expr = Expression(spec.filter) if spec.filter else None
        self.right_rows: list[dict] = []
        self.right_on = None
        self.left_on = None
        if spec.join:
            self.right_rows = _read_jsonl(spec.join["right_source"])
            self.left_on = spec.join["left_on"]
            self.right_on = spec.join.get("right_on", self.left_on)
        self.state = self._load_state()

    # ---------- 状态 ----------

    def _load_state(self) -> dict:
        if os.path.exists(self.state_path):
            with open(self.state_path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        return {
            "positions": {},      # "source/id" -> 已应用的最大 seq
            "max_event_time": None,
            "windows": {},        # win_key -> {start, group, amount, confirmed, revision}
            "contribs": {},       # "source/id" -> [{win, amount, join_key}]
            "pub_count": 0,
        }

    def _checkpoint(self) -> None:
        _atomic_write_json(self.state_path, self.state)

    # ---------- 窗口键 ----------

    def _win_key(self, start: float, group: dict) -> str:
        return json.dumps([start, group], ensure_ascii=False, sort_keys=True)

    # ---------- 主入口 ----------

    def ingest(self, events_path: str, max_events: int | None = None,
               token=None) -> dict:
        """消费一批事件并发布一个新版本目录；可暂停、可续传。

        max_events: 本次最多应用的事件条数（模拟资源受限/用户暂停）；
        token: 带 cancelled 属性的取消令牌，取消时先落盘再抛 RuntimeError。
        返回本次发布的 manifest。
        """
        applied = skipped = 0
        revisions: list[dict] = []
        events = _read_jsonl(events_path)
        for event in events:
            if token is not None and getattr(token, "cancelled", False):
                self._checkpoint()
                raise RuntimeError("运行已取消（状态已保存，可续传）")
            if max_events is not None and applied >= max_events:
                break
            if self._apply_event(event, revisions):
                applied += 1
            else:
                skipped += 1

        watermark = self._watermark()

        newly_confirmed = []
        if watermark is not None:
            for win in self.state["windows"].values():
                end = win["start"] + self.spec.window_size
                if not win["confirmed"] and end <= watermark:
                    win["confirmed"] = True
                    newly_confirmed.append(self._window_row(win))

        tentative = [self._window_row(w) for w in self.state["windows"].values()
                     if not w["confirmed"]]
        newly_confirmed.sort(key=lambda r: (r["window_start"], repr(r["group"])))
        tentative.sort(key=lambda r: (r["window_start"], repr(r["group"])))

        self.state["pub_count"] += 1
        pub_name = f"pub-{self.state['pub_count']:04d}"
        pub_dir = os.path.join(self.pubs_dir, pub_name)
        if os.path.exists(pub_dir):
            raise FileExistsError(f"发布目录已存在，绝不覆盖: {pub_dir}")
        _atomic_write_jsonl(os.path.join(pub_dir, "windows.jsonl"), newly_confirmed)
        _atomic_write_jsonl(os.path.join(pub_dir, "tentative.jsonl"), tentative)
        _atomic_write_jsonl(os.path.join(pub_dir, "revisions.jsonl"), revisions)
        manifest = {
            "stream_id": self.spec.stream_id,
            "publication": pub_name,
            "published_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "events_applied": applied,
            "events_skipped_replay": skipped,
            "watermark": watermark,
            "paused": max_events is not None and applied >= max_events
                      and applied < len(events),
            "confirmed_count": len(newly_confirmed),
            "tentative_count": len(tentative),
            "revision_count": len(revisions),
        }
        _atomic_write_json(os.path.join(pub_dir, "manifest.json"), manifest)
        self._checkpoint()
        return manifest

    # ---------- 单事件应用 ----------

    def _apply_event(self, event: dict, revisions: list[dict]) -> bool:
        source = event["source"]
        rec_id = event["id"]
        seq = event["seq"]
        pos_key = f"{source}/{rec_id}"
        if seq <= self.state["positions"].get(pos_key, 0):
            return False  # 重放/过期事件：不重复计入

        old_contribs = self.state["contribs"].pop(pos_key, [])
        new_contribs: list[dict] = []
        if event["op"] == "upsert":
            new_contribs = self._contributions(event["record"])
        elif event["op"] != "retract":
            raise ValueError(f"未知 op: {event['op']}")

        cause = {"source": source, "id": rec_id, "seq": seq, "op": event["op"]}
        # 先撤销旧贡献，再叠加新贡献（更正 = 撤旧 + 上新）
        for c in old_contribs:
            self._add_delta(c, -c["amount"], cause, revisions)
        for c in new_contribs:
            self._add_delta(c, c["amount"], cause, revisions)
        if new_contribs:
            self.state["contribs"][pos_key] = new_contribs

        self.state["positions"][pos_key] = seq
        if event["op"] == "upsert":
            ts = event["record"][self.spec.time_field]
            cur = self.state["max_event_time"]
            if cur is None or ts > cur:
                self.state["max_event_time"] = ts
        return True

    def _contributions(self, record: dict) -> list[dict]:
        """一条记录经筛选、连接展开后的逐份窗口贡献。"""
        if self.filter_expr is not None and not self.filter_expr(record):
            return []
        matched_rows = []
        if self.spec.join:
            for right in self.right_rows:
                if repr(right.get(self.right_on)) == repr(record.get(self.left_on)):
                    merged = dict(record)
                    for key, value in right.items():
                        if key != self.right_on and key not in merged:
                            merged[key] = value
                    matched_rows.append((merged, right.get(self.right_on)))
        else:
            matched_rows = [(record, None)]
        contribs = []
        for row, join_key in matched_rows:
            ts = row[self.spec.time_field]
            start = _window_start(ts, self.spec.window_size)
            group = {k: row.get(k) for k in self.spec.group_by}
            contribs.append({
                "win": self._win_key(start, group),
                "amount": row[self.spec.amount_field],
                "join_key": join_key,
            })
        return contribs

    def _add_delta(self, contrib: dict, delta: float,
                   cause: dict, revisions: list[dict]) -> None:
        key = contrib["win"]
        win = self.state["windows"].get(key)
        if win is None:
            # 从窗口键还原 start/group（该 (窗口, 分组) 首次出现）
            win = self._rebuild_window(key)
        if not win["confirmed"] and self._watermark() is not None and \
                win["start"] + self.spec.window_size <= self._watermark():
            # 水位线已越过该窗口右端：新出现的分组直接确认，
            # 本次变化属于对已确认结果的修订
            win["confirmed"] = True
        old = win["amount"]
        win["amount"] = old + delta
        if win["confirmed"]:
            win["revision"] += 1
            revisions.append({
                "window_start": win["start"],
                "window_end": win["start"] + self.spec.window_size,
                "group": win["group"],
                "revision": win["revision"],
                "old_amount": old,
                "new_amount": win["amount"],
                "delta": delta,
                "cause": cause,
                "join_key": contrib.get("join_key"),
            })

    def _watermark(self) -> float | None:
        if self.state["max_event_time"] is None:
            return None
        return self.state["max_event_time"] - self.spec.allowed_lateness

    def _rebuild_window(self, key: str) -> dict:
        start, group = json.loads(key)
        win = {"start": start, "group": group, "amount": 0,
               "confirmed": False, "revision": 0}
        self.state["windows"][key] = win
        return win

    def _window_row(self, win: dict) -> dict:
        return {
            "window_start": win["start"],
            "window_end": win["start"] + self.spec.window_size,
            "group": win["group"],
            "amount": win["amount"],
            "revision": win["revision"],
        }

    # ---------- 查询 ----------

    def confirmed_windows(self) -> list[dict]:
        """当前已确认窗口（含被修订后的最新值）。"""
        rows = [self._window_row(w) for w in self.state["windows"].values()
                if w["confirmed"]]
        rows.sort(key=lambda r: (r["window_start"], repr(r["group"])))
        return rows

    def tentative_windows(self) -> list[dict]:
        """当前暂定窗口（水位线未越过窗口右端）。"""
        rows = [self._window_row(w) for w in self.state["windows"].values()
                if not w["confirmed"]]
        rows.sort(key=lambda r: (r["window_start"], repr(r["group"])))
        return rows

    def publication_dir(self, n: int) -> str:
        return os.path.join(self.pubs_dir, f"pub-{n:04d}")
