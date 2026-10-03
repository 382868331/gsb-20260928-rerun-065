"""多来源事件时间分组窗口引擎。

在乱序、撤回/更正、暂定/确认视图与不可变修订发布之上，本模块支持
多个相互独立的来源：每个来源各自维护记录编号、事件时间水位线、
已接收位置（偏移/序号）以及"暂时停发（排除）"决定。

核心约定：
- 没有数据到达不会自动被解释为来源结束；只有显式 exclude_source
  才会把来源暂时排除在水位线之外；
- 被排除来源用 resume_source 恢复后，迟到记录照常进入；
  若改动了已发布窗口，生成带明确依据（basis）的新修订，
  旧修订文件永不改写；
- 不同来源允许使用相同记录编号：身份键是 (source, id)，
  重放/更正/撤回只作用于对应来源；
- 连接两路资料时，窗口同时受两侧来源进度与合法一对多匹配约束，
  不能只按较快一侧提前宣布完整；
- 保存点保存各来源的接受位置与停发决定，可在新进程中完整还原。

输入事件（JSONL，每行一条，"source" 缺省为 "main"）：

    {"source": "orders", "op": "upsert", "id": "e1", "seq": 1,
     "event_time": "...", ...业务字段}
    {"source": "orders", "op": "retract", "id": "e1", "seq": 2}
    {"source": "orders", "op": "watermark", "ts": "..."}

管理事件同样可以混入事件流（或直接调用方法）：

    {"op": "exclude_source", "source": "customers", "reason": "对方暂停"}
    {"op": "resume_source",  "source": "customers"}
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone

from .expr import Expression

_AGG_RE = re.compile(r"^(sum|count|min|max)\(\s*([A-Za-z_][A-Za-z0-9_]*)?\s*\)$")
DEFAULT_SOURCE = "main"


def data_has_explicit_sources(spec):
    if spec.get("stream_sources") or spec.get("fact_sources"):
        return True
    return any(
        stage.get("op") == "join"
        and not spec.get("sources", {}).get(stage["right_source"])
        for stage in spec.get("stages", []))


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


def fmt_ts(epoch) -> str:
    if epoch is None:
        return None
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
    """多来源增量分组窗口引擎，状态全部落在 state_dir，可中断续跑。"""

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
                raise ValueError("不支持的聚合表达式: %r" % expr)
            func, field = match.group(1), match.group(2)
            if func != "count" and not field:
                raise ValueError("聚合缺少字段: %r" % expr)
            self.aggregates[name] = (func, field)

        self.source_paths = dict(spec.get("sources", {}))
        self.fact_sources = spec.get("fact_sources")
        self._stages = self._compile_stages(spec.get("stages", []))

        # ---- 持久化状态 ----
        # records[(source, id)] = {"payload": 原始upsert, "seq": int,
        #                          "is_fact": 是否带时间字段}
        self.records = {}
        # 每个 join 右侧来源名（可能是动态流来源）
        self.join_right_sources = []
        for stage in spec.get("stages", []):
            if stage.get("op") == "join":
                right = stage["right_source"]
                self.join_right_sources.append(right)
        # sources[name] = {"watermark": float|None, "excluded": bool,
        #                  "excluded_reason": str|None, "offsets": {path:int},
        #                  "accepted": int, "resume_seq": int}
        self.sources = {}
        self.watermark = None          # 最近一次发布修订时的有效水位线
        self.revision = 0
        self.confirmed = {}            # 序列化 win_key -> 最近发布行
        self.impact_path = os.path.join(state_dir, "impact.jsonl")
        self.decisions_path = os.path.join(state_dir, "decisions.jsonl")
        self._load_state()
        # 显式声明的事件流来源 + 没有静态文件支撑的 join 右来源，
        # 都参与多来源水位线；纯静态维表文件不约束窗口。
        for name in spec.get("stream_sources", []):
            self._ensure_source(name)
        for name in self.join_right_sources:
            if not self.source_paths.get(name):
                self._ensure_source(name)
        # 旧单来源接口：没有任何显式流来源时，缺省 main 才参与约束。
        self._implicit_main = (
            len([s for s in self.sources if s != DEFAULT_SOURCE]) == 0
            and not data_has_explicit_sources(spec))
        if self._implicit_main:
            self._ensure_source(DEFAULT_SOURCE)

    # ---- 来源管理 ----

    def _ensure_source(self, name):
        if name not in self.sources:
            self.sources[name] = {
                "watermark": None,
                "excluded": False,
                "excluded_reason": None,
                "offsets": {},
                "accepted": 0,
                "resume_seq": 0,
            }
        return self.sources[name]

    def source_progress(self):
        """返回各来源的进度与停发状态（不含数据，只含可读快照）。"""
        out = {}
        for name, st in self.sources.items():
            out[name] = {
                "watermark": fmt_ts(st["watermark"]),
                "active": not st["excluded"],
                "excluded": st["excluded"],
                "excluded_reason": st["excluded_reason"],
                "accepted_records": st["accepted"],
                "offsets": dict(st["offsets"]),
                "resume_seq": st["resume_seq"],
            }
        return out

    def _active_sources(self):
        return [n for n, st in self.sources.items() if not st["excluded"]]

    def effective_watermark(self, extra_excluded=None):
        """有效水位线 = 所有活跃来源水位线的最小值。

        extra_excluded 里的来源按"假设已停发"处理（用于 publish_preview），
        不改变真实状态。从未报过水位线的活跃来源会把结果压成 None，
        即：没有进度就不能确认任何窗口。
        """
        extra = set(extra_excluded or ())
        marks = []
        for name, st in self.sources.items():
            if st["excluded"] or name in extra:
                continue
            if st["watermark"] is None:
                return None, [s for s in self._active_sources()
                              if s not in extra
                              and self.sources[s]["watermark"] is None]
            marks.append(st["watermark"])
        if not marks:
            return None, list(self._active_sources())
        return min(marks), []

    def exclude_source(self, name, reason=""):
        """显式把某来源视为暂时不再推进；不删除其已接受的数据。"""
        st = self._ensure_source(name)
        st["excluded"] = True
        st["excluded_reason"] = reason or None
        self._append_decision({"op": "exclude_source", "source": name,
                               "reason": reason or None})
        revision = self._publish_revision(
            trigger="exclude_source", basis_source=name)
        self._save_state()
        return {"op": "exclude_source", "source": name,
                "reason": reason or None, "revision": revision}

    def resume_source(self, name, reset_watermark=True):
        """恢复被停发的来源。

        恢复后该来源的水位线约束重新生效：此前借停发发布的窗口保持
        已发布状态，只有该来源新到记录实际改动窗口时才产生新修订。
        默认把该来源水位线降为未报告（恢复 != 声称无迟到数据），
        后续 watermark 事件再重新推进。
        """
        st = self._ensure_source(name)
        st["excluded"] = False
        st["excluded_reason"] = None
        st["resume_seq"] += 1
        if reset_watermark:
            st["watermark"] = None
        self._append_decision({"op": "resume_source", "source": name})
        revision = self._publish_revision(
            trigger="resume_source", basis_source=name)
        self._save_state()
        return {"op": "resume_source", "source": name, "revision": revision}

    def publish_preview(self, assume_excluded):
        """预览：若把 assume_excluded 中的来源暂时停发，能发布哪些窗口。

        不改变任何状态，也不写修订文件。
        """
        extra = set(assume_excluded or ())
        mark, _ = self.effective_watermark(extra)
        publishable = []
        if mark is not None:
            for key, row in self._current_windows().items():
                if key[0] + self.size <= mark:
                    item = dict(row)
                    item["status"] = "confirmed"
                    publishable.append(item)
        return {
            "assume_excluded": sorted(extra),
            "effective_watermark": fmt_ts(mark),
            "windows": sorted(publishable,
                              key=lambda r: (r["window_start"],
                                             json.dumps(r, sort_keys=True))),
        }

    # ---- 阶段编译与逐条变换 ----

    def _compile_stages(self, stages):
        compiled = []
        for stage in stages:
            op = stage["op"]
            if op == "filter":
                compiled.append(("filter", Expression(stage["expr"])))
            elif op == "derive":
                fields = {k: Expression(v) for k, v in stage["fields"].items()}
                compiled.append(("derive", fields))
            elif op == "join":
                right_source = stage["right_source"]
                path = self.source_paths.get(right_source)
                seed = {}
                if path:
                    right_on = stage.get("right_on", stage["left_on"])
                    with open(path, "r", encoding="utf-8") as fh:
                        for line in fh:
                            line = line.strip()
                            if not line:
                                continue
                            row = json.loads(line)
                            seed.setdefault(str(row.get(right_on)),
                                            []).append(row)
                compiled.append(("join", {
                    "right_source": right_source,
                    "left_on": stage["left_on"],
                    "right_on": stage.get("right_on", stage["left_on"]),
                    "how": stage.get("how", "inner"),
                    "seed": seed,
                }))
            else:
                raise ValueError("窗口引擎不支持的算子: %s" % op)
        return compiled

    def _right_index(self, stage):
        """某 join 右侧的当前完整索引：静态种子 + 右侧来源生效中的记录。"""
        index = {k: list(v) for k, v in stage["seed"].items()}
        right_source = stage["right_source"]
        for (src, _rid), rec in self.records.items():
            if src != right_source:
                continue
            payload = rec["payload"]
            index.setdefault(str(payload.get(stage["right_on"])),
                             []).append(payload)
        return index

    def _transform(self, payload):
        """filter/derive/join 逐条应用；连接保留合法重复（一行可放大多行）。"""
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
                index = self._right_index(arg)
                for row in rows:
                    matches = index.get(str(row.get(arg["left_on"])), [])
                    if matches:
                        for right in matches:
                            merged = dict(row)
                            for key, value in right.items():
                                merged[key if key not in merged
                                       else key + "_right"] = value
                            next_rows.append(merged)
                    elif arg["how"] == "left":
                        next_rows.append(dict(row))
            rows = next_rows
        return rows

    def _is_fact(self, payload):
        if self.time_field not in payload:
            return False
        try:
            parse_ts(payload[self.time_field])
        except (TypeError, ValueError):
            return False
        return True

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

    def _materialize_record(self, rec, source=None):
        """一条 upsert 经流水线后的全部生效行；非事实（如纯维表）行返回 []。"""
        payload = rec["payload"]
        if self.fact_sources is not None and source not in self.fact_sources:
            return []
        if not self._is_fact(payload):
            return []
        return self._transform(payload)

    def _current_windows(self):
        buckets = {}
        for (src, rid), rec in self.records.items():
            for row in self._materialize_record(rec, source=src):
                row = dict(row)
                row.setdefault("_src", src)
                row.setdefault("_src_id", rid)
                key = _window_key(row, self.time_field, self.size,
                                  self.group_by)
                buckets.setdefault(key, []).append(row)
        result = {}
        for key, rows in buckets.items():
            label = _window_label(key, self.size, self.group_by)
            label.update(self._aggregate(
                [{k: v for k, v in r.items()
                  if not k.startswith("_")} for r in rows]))
            result[key] = label
        return result

    def _is_confirmed_at(self, key, mark):
        return mark is not None and key[0] + self.size <= mark

    @staticmethod
    def _ser_key(key):
        return json.dumps([key[0], list(key[1])])

    @staticmethod
    def _parse_key(serial):
        raw = json.loads(serial)
        return raw[0], tuple(raw[1])

    def window_holds(self):
        """解释每个暂定窗口为何不能发布、正在等哪个来源。

        返回 [{window..., status, effective_watermark, waiting_for:[
        {"source", "reason": "no_progress"|"excluded"|...,
         "source_watermark", "joined": bool}]}]。
        joined=False 表示该窗口内有事实尚未在右侧来源取得任何匹配，
        且右侧来源水位线还没越过窗口结束（匹配可能后到）。
        """
        mark, no_progress = self.effective_watermark()
        windows = self._current_windows()
        # inner join 尚匹不上时，窗口不会出现在聚合结果里；
        # 从待匹配事实补充这些"尚未成形"的窗口。
        for pending_key, pending in self._pending_join_keys().items():
            if pending_key not in windows:
                windows[pending_key] = _window_label(
                    pending_key, self.size, self.group_by)
        result = []
        for key, row in sorted(windows.items()):
            if self._is_confirmed_at(key, mark):
                continue
            waiting = []
            end_ts = key[0] + self.size
            for name in no_progress:
                waiting.append({
                    "source": name,
                    "reason": "no_watermark",
                    "source_watermark": None,
                })
            # 已有水位线但没越过窗口结束的来源
            if mark is None or end_ts > mark:
                for name, st in self.sources.items():
                    if st["excluded"] or name in no_progress:
                        continue
                    wm = st["watermark"]
                    if wm is not None and wm < end_ts:
                        waiting.append({
                            "source": name,
                            "reason": "watermark_behind_window",
                            "source_watermark": fmt_ts(wm),
                        })
            # 连接完整性：右来源是否可能补到匹配
            join_wait = self._join_waits_for_window(key)
            waiting.extend(join_wait)
            item = dict(row)
            item["status"] = "tentative"
            item["effective_watermark"] = fmt_ts(mark)
            item["waiting_for"] = waiting
            result.append(item)
        return result

    def _pending_join_keys(self):
        """返回存在事实但被某个 inner join 卡住、尚未成形的窗口键。"""
        join_stages = [arg for op, arg in self._stages if op == "join"]
        pending = {}
        if not join_stages:
            return pending
        indexes = {id(s): self._right_index(s) for s in join_stages}
        for (src, rid), rec in self.records.items():
            payload = rec["payload"]
            if self.fact_sources is not None \
                    and src not in self.fact_sources:
                continue
            if not self._is_fact(payload):
                continue
            try:
                wkey = _window_key(payload, self.time_field, self.size,
                                   self.group_by)
            except (TypeError, ValueError):
                continue
            blocked = False
            for stage in join_stages:
                left_rows = self._pre_join_rows(payload, stage)
                index = indexes[id(stage)]
                if stage["how"] != "inner":
                    continue
                if any(not index.get(str(r.get(stage["left_on"])), [])
                       for r in left_rows):
                    blocked = True
            if blocked:
                pending[wkey] = True
        return pending

    def window_holds_map(self):
        return {(h["window_start"], tuple(h.get(g) for g in self.group_by)): h
                for h in self.window_holds()}

    def _join_waits_for_window(self, key):
        """窗口内事实若未匹配上右来源，且右来源进度未越过窗口 -> 等待。"""
        waits = []
        start_ts = key[0]
        end_ts = start_ts + self.size
        group_vals = key[1]
        join_stages = [arg for op, arg in self._stages if op == "join"]
        if not join_stages:
            return waits
        for stage in join_stages:
            right = stage["right_source"]
            st = self.sources.get(right)
            if st is None or st["excluded"]:
                continue
            index = self._right_index(stage)
            unmatched = False
            for (src, rid), rec in self.records.items():
                payload = rec["payload"]
                if self.fact_sources is not None \
                        and src not in self.fact_sources:
                    continue
                if not self._is_fact(payload):
                    continue
                try:
                    rkey = _window_key(payload, self.time_field, self.size,
                                       self.group_by)
                except (TypeError, ValueError):
                    continue
                if rkey != key:
                    continue
                # 事实在 join 前的 filter/derive 与本 join 的左行判定
                left_rows = self._pre_join_rows(payload, stage)
                for left in left_rows:
                    if not index.get(str(left.get(stage["left_on"])), []):
                        if stage["how"] != "left":
                            unmatched = True
            if unmatched:
                wm = st["watermark"]
                if wm is None or wm < end_ts:
                    waits.append({
                        "source": right,
                        "reason": "join_match_pending",
                        "source_watermark": fmt_ts(wm),
                    })
        return waits

    def _pre_join_rows(self, payload, target_stage):
        rows = [dict(payload)]
        for op, arg in self._stages:
            if arg is target_stage:
                break
            if op == "filter":
                rows = [r for r in rows if arg(r)]
            elif op == "derive":
                for row in rows:
                    for name, expr in arg.items():
                        row[name] = expr(row)
        return rows

    # ---- 事件应用 ----

    def apply_event(self, event):
        op = event.get("op", "upsert")
        if op == "exclude_source":
            return self.exclude_source(event["source"],
                                       event.get("reason", ""))
        if op == "resume_source":
            return self.resume_source(event["source"],
                                      event.get("reset_watermark", True))
        source = event.get("source", DEFAULT_SOURCE)
        self._ensure_source(source)
        if op == "watermark":
            return self.advance_watermark(event["ts"], source=source)

        rec_id = event["id"]
        key = (source, rec_id)
        seq = event.get("seq")
        st = self.sources[source]
        # seq 仅在同一来源内比较；重放（含恢复后的旧消息）直接跳过
        if seq is not None:
            prev_seq = self.records.get(key, {}).get("seq")
            if prev_seq is not None and seq <= prev_seq:
                return {"source": source, "id": rec_id, "op": op,
                        "skipped": "replay", "revision": None,
                        "window_deltas": {}}

        before = self._current_windows()
        old = self.records.get(key)
        old_rows = self._materialize_record(old, source=source) if old else []
        # 右侧维表变更也会影响其他来源事实的连接结果，故窗口整体重算
        if op == "retract":
            self.records.pop(key, None)
            new_rows = []
        elif op == "upsert":
            payload = {k: v for k, v in event.items()
                       if k not in ("op", "id", "seq", "source")}
            rec = {"payload": payload, "seq": seq}
            self.records[key] = rec
            new_rows = self._materialize_record(rec, source=source)
        else:
            raise ValueError("未知事件 op: %s" % op)
        st["accepted"] += 1
        after = self._current_windows()
        mark, _ = self.effective_watermark()

        deltas = {}
        touched_confirmed = False
        for wkey in set(before) | set(after):
            old_w, new_w = before.get(wkey), after.get(wkey)
            if old_w == new_w:
                continue
            label = _window_label(wkey, self.size, self.group_by)
            diff = {}
            for name in self.aggregates:
                old_v = old_w.get(name, 0) if old_w else 0
                new_v = new_w.get(name, 0) if new_w else 0
                if old_v != new_v:
                    diff[name] = {"old": old_v, "new": new_v,
                                  "delta": new_v - old_v}
            label["deltas"] = diff
            label["confirmed"] = (
                self._is_confirmed_at(wkey, mark)
                or self._ser_key(wkey) in self.confirmed)
            deltas[json.dumps(
                _window_label(wkey, self.size, self.group_by),
                sort_keys=True)] = label
            if label["confirmed"]:
                touched_confirmed = True

        changed_published = []
        for wkey in set(before) | set(after):
            serial = self._ser_key(wkey)
            if serial in self.confirmed:
                changed_published.append(wkey)

        revision = None
        if touched_confirmed or changed_published:
            revision = self._publish_revision(
                trigger="late_change", basis_source=source,
                changed_keys=set(changed_published))

        impact = {
            "source": source, "id": rec_id, "op": op, "seq": seq,
            "join_removed": old_rows,
            "join_added": new_rows,
            "window_deltas": deltas,
            "revision": revision,
        }
        self._append_impact(impact)
        self._save_state()
        return impact

    def advance_watermark(self, ts, source=DEFAULT_SOURCE):
        self._ensure_source(source)
        st = self.sources[source]
        mark = parse_ts(ts)
        if st["watermark"] is None or mark > st["watermark"]:
            st["watermark"] = mark
        effective, _ = self.effective_watermark()
        revision = self._publish_revision(
            trigger="watermark", basis_source=source)
        self.watermark = effective
        self._save_state()
        return {"op": "watermark", "source": source,
                "source_watermark": fmt_ts(st["watermark"]),
                "effective_watermark": fmt_ts(effective),
                "revision": revision}

    # ---- 视图 ----

    def tentative_view(self):
        """全部窗口当前值；暂定窗口附带 waiting_for 等待原因。"""
        mark, _ = self.effective_watermark()
        holds = self.window_holds_map()
        rows = []
        for key, row in self._current_windows().items():
            item = dict(row)
            if self._is_confirmed_at(key, mark):
                item["status"] = "confirmed"
            else:
                item["status"] = "tentative"
                item["effective_watermark"] = fmt_ts(mark)
                item["waiting_for"] = holds.get(key, {}).get("waiting_for", [])
            rows.append(item)
        return sorted(rows, key=lambda r: (r["window_start"],
                                           json.dumps(r, sort_keys=True)))

    def confirmed_view(self):
        return [dict(v) for v in
                sorted(self.confirmed.values(),
                       key=lambda r: (r["window_start"],
                                      json.dumps(r, sort_keys=True)))]

    # ---- 不可变修订发布 ----

    def _publish_revision(self, trigger=None, basis_source=None,
                          changed_keys=None):
        mark, no_progress = self.effective_watermark()
        windows = self._current_windows()
        changed_keys = changed_keys or set()
        snapshot = dict(self.confirmed)
        for serial in list(snapshot.keys()):
            key = self._parse_key(serial)
            row = windows.get(key)
            if self._is_confirmed_at(key, mark) or key in changed_keys:
                if row is None:
                    snapshot.pop(serial, None)
                else:
                    item = dict(row)
                    item["status"] = "confirmed"
                    snapshot[serial] = item
        for key, row in windows.items():
            if self._is_confirmed_at(key, mark):
                item = dict(row)
                item["status"] = "confirmed"
                snapshot[self._ser_key(key)] = item
        if snapshot == self.confirmed and self.revision > 0:
            return self.revision
        self.confirmed = snapshot
        self.watermark = mark
        self.revision += 1
        rev_id = self.revision
        path = os.path.join(self.revisions_dir,
                            "rev-%05d.jsonl" % rev_id)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            for row in self.confirmed_view():
                fh.write(json.dumps(row, ensure_ascii=False, default=str))
                fh.write("\n")
        os.replace(tmp, path)
        # 修订依据 sidecar：明确说明该修订凭什么发布
        basis = {
            "revision": rev_id,
            "trigger": trigger,
            "basis_source": basis_source,
            "effective_watermark": fmt_ts(mark),
            "sources": self.source_progress(),
            "waiting_sources": no_progress,
        }
        bpath = os.path.join(self.revisions_dir,
                             "rev-%05d.basis.json" % rev_id)
        with open(bpath + ".tmp", "w", encoding="utf-8") as fh:
            json.dump(basis, fh, ensure_ascii=False, default=str, indent=2)
        os.replace(bpath + ".tmp", bpath)
        return rev_id

    # ---- 多来源文件消费与保存点 ----

    def run_events(self, events_path, limit=None, source=None):
        """从某来源已接收偏移继续消费事件文件；返回本次处理条数。

        source 缺省取文件 basename 去扩展名，再缺省为 main。
        同一路径被不同来源复用时各自记录偏移。
        """
        if source is None:
            if getattr(self, "_implicit_main", True):
                source = DEFAULT_SOURCE
            else:
                source = os.path.splitext(
                    os.path.basename(events_path))[0]
        self._ensure_source(source)
        st = self.sources[source]
        offsets = st.setdefault("offsets", {})
        offset = offsets.get(events_path, 0)
        applied = 0
        with open(events_path, "r", encoding="utf-8") as fh:
            fh.seek(offset)
            while limit is None or applied < limit:
                line = fh.readline()
                if not line:
                    break
                offset = fh.tell()
                if line.strip():
                    event = json.loads(line)
                    event.setdefault("source", source)
                    self.apply_event(event)
                    applied += 1
                offsets[events_path] = offset
        offsets[events_path] = offset
        self._save_state()
        return applied

    def export_savepoint(self, note=""):
        """导出保存点：各来源接受位置 + 水位线 + 停发决定。"""
        data = {
            "note": note,
            "sources": self.source_progress(),
            "revision": self.revision,
        }
        path = os.path.join(self.state_dir,
                            "savepoint-%05d.json" % self.revision)
        with open(path + ".tmp", "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, default=str, indent=2)
        os.replace(path + ".tmp", path)
        return path

    def accepted_positions(self):
        """各来源已接受位置（记录数与每文件字节偏移）。"""
        return {name: {"accepted_records": st["accepted"],
                       "offsets": dict(st["offsets"])}
                for name, st in self.sources.items()}

    @property
    def offset(self):
        """旧单来源接口兼容：main 来源唯一事件文件的字节偏移。"""
        st = self.sources.get(DEFAULT_SOURCE, {})
        offsets = st.get("offsets", {})
        if len(offsets) == 1:
            return next(iter(offsets.values()))
        return 0

    # ---- 持久化 ----

    def _state_path(self):
        return os.path.join(self.state_dir, "state.json")

    def _save_state(self):
        data = {
            "version": 2,
            "records": [[list(k), v] for k, v in self.records.items()],
            "sources": self.sources,
            "watermark": self.watermark,
            "revision": self.revision,
            "confirmed": self.confirmed,
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
        if data.get("version", 1) < 2:
            # 兼容旧单来源状态：live[id] -> records[(main,id)]
            self.records = {}
            for rid, rows in data.get("live", {}).items():
                payload = rows[0] if rows else {}
                self.records[(DEFAULT_SOURCE, rid)] = {
                    "payload": {k: v for k, v in payload.items()
                                if not k.startswith("_")},
                    "seq": data.get("last_seq", {}).get(rid),
                }
            self._ensure_source(DEFAULT_SOURCE)
            self.sources[DEFAULT_SOURCE]["watermark"] = data.get("watermark")
        else:
            self.records = {tuple(k): v for k, v in data["records"]}
            self.sources = data["sources"]
            for st in self.sources.values():
                st.setdefault("resume_seq", 0)
                st.setdefault("accepted", 0)
                st.setdefault("offsets", {})
        self.watermark = data["watermark"]
        self.revision = data["revision"]
        self.confirmed = data["confirmed"]

    def _append_impact(self, impact):
        with open(self.impact_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(impact, ensure_ascii=False, default=str))
            fh.write("\n")

    def _append_decision(self, decision):
        with open(self.decisions_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(decision, ensure_ascii=False, default=str))
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
