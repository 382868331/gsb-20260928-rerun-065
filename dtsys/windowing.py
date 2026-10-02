"""事件时间分组窗口引擎：乱序、撤回/更正、暂定/确认视图与修订发布。

与批式执行器（runner.py）互补：本引擎消费**变更流**（changelog），
逐条维护分组窗口聚合，并把确认结果以不可变修订文件发布。

输入事件（JSONL，每行一条）：
  {"op": "upsert",  "id": "e1", "seq": 1, "event_time": "...", ...业务字段}
  {"op": "retract", "id": "e1", "seq": 2}
  {"op": "watermark", "ts": "..."}          # 推进水位线，确认结束的窗口

语义约定：
- 同一 id 的 upsert 覆盖先前值（更正）；retract 撤回整条记录；
- 每条事件带单调递增的 seq，已应用的 seq 重放时直接跳过，
  保证"同一来源记录不会因重放再次计入"；
- 窗口按 event_time 分组（乱序到达不影响最终结果）；
- 窗口结束时间 <= 水位线即确认；确认后又被迟到事件改动的，
  生成新的修订文件 rev-<n>.jsonl，旧修订文件永不改写；
- 每条事件的影响（连接结果增删、窗口金额差额）记入 impact.jsonl；
- 全部状态持久化在 state_dir，暂停/受限后可用同一目录继续，
  run_events 从已接收偏移处续跑。

窗口规格（spec）：
{
  "sources": {"events": "...", "customers": "..."},
  "stages": [filter/derive/join ...],   # 与批式计划同构，逐条应用
  "window": {
    "time_field": "event_time",
    "size_seconds": 60,
    "group_by": ["cust_id"],
    "aggregates": {"total": "sum(amount)", "n": "count()"}
  }
}
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone

from .expr import Expression

_AGG_RE = re.compile(r"^(sum|count|min|max)\(\s*([A-Za-z_][A-Za-z0-9_]*)?\s*\)$")


def parse_ts(value) -> float:
    """ISO 时间字符串或 epoch 秒 -> epoch 秒（UTC）。"""
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def fmt_ts(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def _window_key(row, time_field, size, group_by):
    start = int(parse_ts(row[time_field]) // size) * size
    return (start, tuple(row.get(g) for g in group_by))


def _window_label(key, size, group_by):
    start, groups = key
    label = {"window_start": fmt_ts(start), "window_end": fmt_ts(start + size)}
    label.update(dict(zip(group_by, groups)))
    return label


class WindowEngine:
    """增量分组窗口引擎。状态全部落在 state_dir，可随时中断续跑。"""

    def __init__(self, spec: dict, state_dir: str):
        self.spec = spec
        self.state_dir = state_dir
        self.revisions_dir = os.path.join(state_dir, "revisions")
        os.makedirs(self.revisions_dir, exist_ok=True)

        win = spec["window"]
        self.time_field = win["time_field"]
        self.size = int(win["size_seconds"])
        self.group_by = list(win.get("group_by", []))
        self.aggregates = {}
        for name, expr in win.get("aggregates", {"n": "count()"}).items():
            match = _AGG_RE.match(expr.strip())
            if not match:
                raise ValueError(f"不支持的聚合表达式: {expr!r}")
            func, field = match.group(1), match.group(2)
            if func != "count" and not field:
                raise ValueError(f"聚合缺少字段: {expr!r}")
            self.aggregates[name] = (func, field)

        self._stages = self._compile_stages(spec.get("stages", []),
                                            spec.get("sources", {}))

        # 持久化状态
        self.live = {}          # id -> 当前生效的输出行（连接放大后为多行）
        self.last_seq = {}      # id -> 已应用的最大 seq
        self.watermark = None
        self.revision = 0
        self.confirmed = {}     # 序列化 win_key -> 最近发布的确认行
        self.offset = 0         # 输入文件已消费字节偏移
        self.impact_path = os.path.join(state_dir, "impact.jsonl")
        self._load_state()

    # ---- 阶段编译：filter / derive / join（与批式算子同语义，逐条应用）----

    def _compile_stages(self, stages, sources):
        compiled = []
        for stage in stages:
            stage = dict(stage)
            op = stage.pop("op")
            if op == "filter":
                compiled.append(("filter", Expression(stage["expr"])))
            elif op == "derive":
                fields = {k: Expression(v) for k, v in stage["fields"].items()}
                compiled.append(("derive", fields))
            elif op == "join":
                right_path = sources[stage["right_source"]]
                left_on = stage["left_on"]
                right_on = stage.get("right_on", left_on)
                how = stage.get("how", "inner")
                index = {}
                with open(right_path, "r", encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        row = json.loads(line)
                        index.setdefault(row.get(right_on), []).append(row)
                compiled.append(("join", (index, left_on, how)))
            else:
                raise ValueError(f"窗口引擎不支持的算子: {op}")
        return compiled

    def _transform(self, payload):
        """filter/derive/join 逐条应用；连接保留合法重复（一行可放大为多行）。"""
        rows = [dict(payload)]
        for op, arg in self._stages:
            next_rows = []
            if op == "filter":
                next_rows = [r for r in rows if arg(r)]
            elif op == "derive":
                for row in rows:
                    new_row = dict(row)
                    for name, expr in arg.items():
                        new_row[name] = expr(row)
                    next_rows.append(new_row)
            else:  # join
                index, left_on, how = arg
                for row in rows:
                    matches = index.get(row.get(left_on), [])
                    if matches:
                        for right in matches:
                            merged = dict(row)
                            for key, value in right.items():
                                merged[key if key not in merged
                                       else key + "_right"] = value
                            next_rows.append(merged)
                    elif how == "left":
                        next_rows.append(dict(row))
            rows = next_rows
        return rows

    # ---- 聚合 ----

    def _aggregate(self, rows):
        out = {}
        for name, (func, field) in self.aggregates.items():
            values = [r[field] for r in rows] if field else []
            if func == "count":
                out[name] = len(rows)
            elif not values:
                out[name] = 0
            elif func == "sum":
                out[name] = sum(values)
            elif func == "min":
                out[name] = min(values)
            else:
                out[name] = max(values)
        return out

    def _current_windows(self):
        """从全部生效行重算所有窗口 -> {win_key: 完整行}。"""
        buckets = {}
        for rows in self.live.values():
            for row in rows:
                key = _window_key(row, self.time_field, self.size, self.group_by)
                buckets.setdefault(key, []).append(row)
        result = {}
        for key, rows in buckets.items():
            label = _window_label(key, self.size, self.group_by)
            label.update(self._aggregate(rows))
            result[key] = label
        return result

    def _is_confirmed(self, key):
        return self.watermark is not None and key[0] + self.size <= self.watermark

    # ---- 事件应用 ----

    def apply_event(self, event):
        op = event.get("op", "upsert")
        if op == "watermark":
            return self.advance_watermark(event["ts"])

        rec_id = event["id"]
        seq = event.get("seq")
        if seq is not None and rec_id in self.last_seq \
                and seq <= self.last_seq[rec_id]:
            return {"id": rec_id, "op": op, "skipped": "replay",
                    "revision": None, "window_deltas": {}}

        before = self._current_windows()
        old_rows = self.live.get(rec_id, [])
        if op == "retract":
            new_rows = []
            self.live.pop(rec_id, None)
        elif op == "upsert":
            payload = {k: v for k, v in event.items()
                       if k not in ("op", "id", "seq")}
            new_rows = self._transform(payload)
            self.live[rec_id] = new_rows
        else:
            raise ValueError("未知事件 op: %s" % op)
        if seq is not None:
            self.last_seq[rec_id] = seq
        after = self._current_windows()

        deltas = {}
        touched_confirmed = False
        for key in set(before) | set(after):
            old, new = before.get(key), after.get(key)
            if old == new:
                continue
            label = _window_label(key, self.size, self.group_by)
            diff = {}
            for name in self.aggregates:
                old_v = old.get(name, 0) if old else 0
                new_v = new.get(name, 0) if new else 0
                if old_v != new_v:
                    diff[name] = {"old": old_v, "new": new_v,
                                  "delta": new_v - old_v}
            label["deltas"] = diff
            label["confirmed"] = self._is_confirmed(key)
            deltas[json.dumps(_window_label(key, self.size, self.group_by),
                              sort_keys=True)] = label
            if self._is_confirmed(key):
                touched_confirmed = True

        revision = None
        if touched_confirmed:
            revision = self._publish_revision()

        impact = {
            "id": rec_id, "op": op, "seq": seq,
            "join_removed": old_rows,   # 更正/撤回移除的连接结果
            "join_added": new_rows,     # 更正/新增产生的连接结果
            "window_deltas": deltas,    # 各窗口金额差额
            "revision": revision,
        }
        self._append_impact(impact)
        self._save_state()
        return impact

    def advance_watermark(self, ts):
        mark = parse_ts(ts)
        if self.watermark is None or mark > self.watermark:
            self.watermark = mark
        revision = self._publish_revision()
        self._save_state()
        return {"op": "watermark", "watermark": fmt_ts(self.watermark),
                "revision": revision}

    # ---- 视图 ----

    def tentative_view(self):
        """全部窗口当前值（含未确认的暂定窗口）。"""
        rows = []
        for key, row in self._current_windows().items():
            row = dict(row)
            row["status"] = "confirmed" if self._is_confirmed(key) else "tentative"
            rows.append(row)
        return sorted(rows, key=lambda r: (r["window_start"],
                                           json.dumps(r, sort_keys=True)))

    def confirmed_view(self):
        """已确认窗口的最新发布值。"""
        return [dict(v) for v in
                sorted(self.confirmed.values(),
                       key=lambda r: (r["window_start"],
                                      json.dumps(r, sort_keys=True)))]

    # ---- 修订发布（旧文件永不改写）----

    def _publish_revision(self):
        windows = self._current_windows()
        self.confirmed = {}
        for key, row in windows.items():
            if self._is_confirmed(key):
                row = dict(row)
                row["status"] = "confirmed"
                self.confirmed[json.dumps(key)] = row
        self.revision += 1
        path = os.path.join(self.revisions_dir,
                            "rev-%05d.jsonl" % self.revision)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            for row in self.confirmed_view():
                fh.write(json.dumps(row, ensure_ascii=False, default=str))
                fh.write("\n")
        os.replace(tmp, path)
        return self.revision

    # ---- 输入文件续跑 ----

    def run_events(self, events_path, limit=None):
        """从已接收偏移继续消费事件文件；返回本次处理条数。

        暂停/受限后直接再次调用（或换进程重建引擎）即可续跑，
        已确认的窗口与偏移都会保留。
        """
        applied = 0
        with open(events_path, "r", encoding="utf-8") as fh:
            fh.seek(self.offset)
            while limit is None or applied < limit:
                line = fh.readline()
                if not line:
                    break
                self.offset = fh.tell()
                if line.strip():
                    self.apply_event(json.loads(line))
                    applied += 1
        self._save_state()
        return applied

    # ---- 状态持久化 ----

    def _state_path(self):
        return os.path.join(self.state_dir, "state.json")

    def _save_state(self):
        data = {
            "live": self.live,
            "last_seq": self.last_seq,
            "watermark": self.watermark,
            "revision": self.revision,
            "confirmed": self.confirmed,
            "offset": self.offset,
        }
        tmp = self._state_path() + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, default=str)
        os.replace(tmp, self._state_path())

    def _load_state(self):
        path = self._state_path()
        if not os.path.exists(path):
            return
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        self.live = {k: list(v) for k, v in data["live"].items()}
        self.last_seq = dict(data["last_seq"])
        self.watermark = data["watermark"]
        self.revision = data["revision"]
        self.confirmed = data["confirmed"]
        self.offset = data["offset"]

    def _append_impact(self, impact):
        with open(self.impact_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(impact, ensure_ascii=False, default=str))
            fh.write("\n")


def diff_revisions(path_old, path_new):
    """逐窗口核对两个修订文件：返回新增/删除/金额变化明细。"""
    def load(path):
        rows = {}
        if not os.path.exists(path):
            return rows
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    row = json.loads(line)
                    group = tuple(sorted(
                        (k, str(v)) for k, v in row.items()
                        if not k.startswith("window_") and k != "status"
                        and not isinstance(v, (int, float))))
                    rows[(row["window_start"], group)] = row
        return rows

    old, new = load(path_old), load(path_new)
    changes = []
    for key in sorted(set(old) | set(new)):
        old_row, new_row = old.get(key), new.get(key)
        if old_row == new_row:
            continue
        entry = {"window_start": key[0], "group": dict(key[1])}
        if old_row is None:
            entry["change"] = "added"
            entry["new"] = new_row
        elif new_row is None:
            entry["change"] = "removed"
            entry["old"] = old_row
        else:
            deltas = {}
            for field in new_row:
                if isinstance(new_row.get(field), (int, float)) and \
                        isinstance(old_row.get(field), (int, float)):
                    if new_row[field] != old_row[field]:
                        deltas[field] = {"old": old_row[field],
                                         "new": new_row[field],
                                         "delta": new_row[field] - old_row[field]}
            entry["change"] = "modified"
            entry["deltas"] = deltas
        changes.append(entry)
    return changes

