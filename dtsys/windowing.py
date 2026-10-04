"""多来源事件时间分组窗口引擎：乱序、撤回/更正、停发/恢复、
暂定/确认视图与不可变修订发布。

与批式执行器（runner.py）互补：本引擎消费**变更流**（changelog），
逐条维护分组窗口聚合，并把确认结果以不可变修订文件发布。

输入事件（JSONL，每行一条）：
  {"op": "upsert",  "source": "orders", "id": "e1", "seq": 1,
   "event_time": "...", ...业务字段}
  {"op": "retract", "source": "orders", "id": "e1", "seq": 2}
  {"op": "watermark", "source": "orders", "ts": "..."}   # 推进该来源水位线

多来源语义：
- 每个来源独立维护：已接收偏移（offset）、事件时间进度（watermark）、
  停发标记（stalled）与逐 id 的 seq 去重；不同来源使用同样的记录编号
  仍是不同事件，重放/更正/撤回只作用于对应来源；
- 窗口确认需要**全部未停发来源**的水位线都越过窗口结束时间
  （有效水位线 = 各未停发来源水位线的最小值）；某来源没有数据到达
  本身不能解释为该来源已结束，它的进度未知时会一直阻塞确认；
- set_stalled(source, True) 把某来源**暂时**视为不再推进：该来源从
  有效水位线中剔除，因此可确认的窗口立即发布为新修订；这是显式的
  人工决定并持久化，而不是"没数据"的自动推断；
- 被停发的来源再次活跃（set_stalled(source, False) 或继续到达事件）时，
  后到的记录可能影响已发布窗口，按迟到的不同源事件生成有明确依据的
- 每个来源可单独设置**允许迟到时间**（set_lateness）：超过来源水位线
  减去允许迟到时间的事件被丢弃并记录在案（dropped），不再改动窗口；
  缺省不限制（保持向后兼容）。策略调整分两种作用域：
    * confirmation_policy="on_watermark"（默认）：窗口结束时间一到有效
      水位即确认，后续迟到事件以修订形式接纳；
    * confirmation_policy="after_lateness"：等待所有未停发来源的水位线
      越过"窗口结束 + 各自允许迟到时间"后再确认；某来源未设有限期时
      继续等待。每个窗口首次见到数据时固化口径与期限，运行中 future
      改策略不改已有窗口；历史范围重研需显式传 confirm=True；
    * scope="future"：只影响尚未确认的窗口，已发布窗口沿用确认时
      快照的旧策略，不会被新的宽松策略悄悄当作从未关闭；
    * scope="revision" + window_range=(start, end)：对指定历史范围
      建立修订研究——更新该范围内已确认窗口的策略快照，重新评估此前
      被丢弃的事件，确实改变的窗口发布新修订，并在 revision_causes
      中列明是哪些来源与哪条规则变化导致的；没有实际数据变化的
      窗口不制造重复修订；
- 来源可**重启换代**（restart_source 或 {"op": "restart"} 事件）：
  代际号递增，seq 按 (来源, 代际) 各自计数——旧代际的重放不会堵住
  新代际从 1 重新开始的序号；旧代际的水位线事件被忽略，旧实例的
  迟到记录不能推进新实例的水位；重启后该来源进度重置为未知，
  重新参与阻塞确认（waiting_on 中可见）；
- tentative_view() 每行带 waiting_on：仍在阻塞该窗口确认的来源列表，
  让用户看见窗口为何仍暂定、在等哪个来源；
- 连接两路流式资料时（join 的 right_source 是另一个流式来源），
  窗口确认同时受两侧来源进度约束，不能只按较快一侧提前宣布完整；
  合法一对多匹配保留放大，右侧维流更正会重算连接结果并按需发布修订。

语义约定：
- 同一 (source, id) 的 upsert 覆盖先前值（更正）；retract 撤回整条记录；
- 每条事件带单调递增的 seq（按来源各自计数），已应用的 seq 重放时
  直接跳过，保证"同一来源记录不会因重放再次计入"；
- 窗口按 event_time 分组（乱序到达不影响最终结果）；
- 窗口结束时间 <= 有效水位线即确认；确认后又被迟到事件改动的，
  生成新的修订文件 rev-<n>.jsonl，旧修订文件永不改写；
- 每条事件的影响（连接结果增删、窗口金额差额）记入 impact.jsonl；
- 全部状态（含各来源偏移与停发决定）持久化在 state_dir，
  暂停/受限后可用同一目录继续，run_events 按来源从已接收偏移续跑，
  尚未完成的来源范围在 source_status() 中清楚保留。

窗口规格（spec）：
{
  "streams": ["orders", "payments"],   # 流式输入来源，缺省 ["events"]
  "primary_source": "orders",          # 产生窗口行的主来源，缺省首个流
  "sources": {"customers": "..."},     # 静态维表文件（非流式）
  "stages": [filter/derive/join ...],   # 与批式计划同构，逐条应用
  "window": {
    "time_field": "event_time",
    "size_seconds": 60,
    "group_by": ["cust_id"],
    "aggregates": {"total": "sum(amount)", "n": "count()"}
  },
  "lateness": {"orders": 120},       # 各来源允许迟到秒数，缺省不限
  "confirmation_policy": "on_watermark"  # 或 after_lateness
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

        # 流式来源：主来源产生窗口行，其余流可作为 join 的右侧维流
        self.streams = list(spec.get("streams") or ["events"])
        self.primary = spec.get("primary_source", self.streams[0])
        if self.primary not in self.streams:
            raise ValueError("primary_source 不在 streams 中: %s" % self.primary)

        self._stages = self._compile_stages(spec.get("stages", []),
                                            spec.get("sources", {}))

        # 持久化状态（全部按来源分开；live/last_seq 再按代际细分）
        self.live = {s: {} for s in self.streams}      # src -> gen -> id -> 负载
        self.last_seq = {s: {} for s in self.streams}  # src -> gen -> id -> seq
        self.watermarks = {s: None for s in self.streams}
        self.stalled = {s: False for s in self.streams}
        self.offsets = {s: 0 for s in self.streams}    # src -> 已消费字节偏移
        self.generations = {s: 0 for s in self.streams}  # src -> 当前代际号
        # 各来源允许迟到秒数：None 表示不限制（默认，兼容旧行为）
        self.lateness = {s: (spec.get("lateness") or {}).get(s)
                         for s in self.streams}
        self.confirmation_policy = spec.get("confirmation_policy",
                                            "on_watermark")
        if self.confirmation_policy not in ("on_watermark", "after_lateness"):
            raise ValueError("未知确认口径: %s" % self.confirmation_policy)
        self.window_policy = {}   # 窗口起点秒(str) -> {source: 确认时快照的策略}
        self.window_confirmation = {}
        self.dropped = []         # 因超过允许迟到时间被丢弃的事件（供修订研究）
        self.rule_changes = []    # 迟到策略调整记录（修订研究的依据）
        self.rev_causes = {}      # 修订号(str) -> 导致该修订的来源与规则变化
        self._pending_causes = []
        self.revision = 0
        self.confirmed = {}     # 序列化 win_key -> 最近发布的确认行
        self.impact_path = os.path.join(state_dir, "impact.jsonl")
        self._load_state()

    # 兼容单来源视角
    @property
    def offset(self):
        return self.offsets[self.primary]

    @property
    def watermark(self):
        return self.effective_watermark()

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
                right_name = stage["right_source"]
                left_on = stage["left_on"]
                right_on = stage.get("right_on", left_on)
                how = stage.get("how", "inner")
                if right_name in self.streams:
                    # 流式右侧：连接索引随该来源事件动态变化
                    compiled.append(("join", ("stream", right_name,
                                              left_on, right_on, how)))
                    continue
                index = {}
                with open(sources[right_name], "r", encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        row = json.loads(line)
                        index.setdefault(row.get(right_on), []).append(row)
                compiled.append(("join", ("static", index, left_on, how)))
            else:
                raise ValueError(f"窗口引擎不支持的算子: {op}")
        return compiled

    def _live_records(self, source):
        """该来源全部代际当前生效的原始负载。"""
        for generation in self.live[source].values():
            for payload in generation.values():
                yield payload

    def _stream_index(self, source, right_on):
        """由维流当前生效的原始负载构建连接索引（保留合法重复）。"""
        index = {}
        for payload in self._live_records(source):
            index.setdefault(payload.get(right_on), []).append(payload)
        return index

    def _join_matches(self, join_arg, key_value):
        kind = join_arg[0]
        if kind == "static":
            return join_arg[1].get(key_value, [])
        _, source, _left_on, right_on, _how = join_arg
        return self._stream_index(source, right_on).get(key_value, [])

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
                how = arg[-1]
                left_on = arg[2]
                for row in rows:
                    matches = self._join_matches(arg, row.get(left_on))
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
        for payload in self._live_records(self.primary):
            for row in self._transform(payload):
                key = _window_key(row, self.time_field, self.size, self.group_by)
                buckets.setdefault(key, []).append(row)
        result = {}
        for key, rows in buckets.items():
            label = _window_label(key, self.size, self.group_by)
            label.update(self._aggregate(rows))
            result[key] = label
        return result

    # ---- 多来源水位线 ----

    def effective_watermark(self):
        """有效水位线 = 全部未停发来源水位线的最小值。

        某来源还没有任何进度（没到过数据/水位线）时返回 None：
        没有数据到达不能自动解释为该来源已结束，它会一直阻塞确认，
        直到被显式停发或真的推进。
        """
        marks = []
        for src in self.streams:
            if self.stalled.get(src):
                continue
            mark = self.watermarks.get(src)
            if mark is None:
                return None
            marks.append(mark)
        return min(marks) if marks else None

    def waiting_on(self, key):
        """仍在阻塞该窗口确认的来源列表（未停发且水位线未越过窗口结束）。"""
        end = key[0] + self.size
        policy = self._confirmation_policy_for(key)
        pending = []
        for src in self.streams:
            if self.stalled.get(src):
                continue
            mark = self.watermarks.get(src)
            deadline = end
            if policy == "after_lateness":
                seconds = self._policy_lateness_for(key, src)
                if seconds is None:
                    pending.append(src)
                    continue
                deadline = end + seconds
            if mark is None or mark < deadline:
                pending.append(src)
        return sorted(pending)

    def _confirmation_policy_for(self, key):
        start = str(key[0])
        snapshot = self.window_confirmation.get(start)
        if snapshot is not None:
            return snapshot["policy"]
        return self.confirmation_policy

    def _policy_lateness_for(self, key, source):
        start = str(key[0])
        snapshot = self.window_confirmation.get(start)
        if snapshot is not None:
            return snapshot.get("lateness_seconds", {}).get(source)
        policy = self.window_policy.get(start)
        if policy is not None and source in policy:
            return policy[source]
        return self.lateness.get(source)

    def _is_confirmed(self, key):
        # 确认是单调的：一旦发布就永远保持已确认（内容可被迟到事件
        # 修订），来源重启换代、水位线重置都不会把旧窗口当作从未关闭
        if json.dumps(key) in self.confirmed:
            return True
        mark = self.effective_watermark()
        if mark is None:
            return False
        end = key[0] + self.size
        if self._confirmation_policy_for(key) == "on_watermark":
            return end <= mark
        horizon = self._lateness_horizon(key)
        return horizon is not None and end <= horizon

    def _lateness_horizon(self, key):
        """延迟确认口径下的有效水位：各来源水位扣除各自允许迟到。"""
        horizons = []
        for src in self.streams:
            if self.stalled.get(src):
                continue
            mark = self.watermarks.get(src)
            seconds = self._policy_lateness_for(key, src)
            if mark is None or seconds is None:
                return None
            horizons.append(mark - seconds)
        return min(horizons) if horizons else None

    def _confirmation_snapshot(self, key):
        end = key[0] + self.size
        seconds = dict(self.lateness)
        deadlines = {}
        latest = None
        if self.confirmation_policy == "after_lateness":
            for src, value in seconds.items():
                if self.stalled.get(src) or value is None:
                    continue
                deadline = end + value
                deadlines[src] = fmt_ts(deadline)
                latest = deadline if latest is None else max(latest, deadline)
        return {
            "policy": self.confirmation_policy,
            "lateness_seconds": seconds,
            "deadline": fmt_ts(latest) if latest is not None else None,
            "deadlines": deadlines,
        }

    def source_status(self):
        """各来源的接受位置、事件时间进度与停发决定（保存点内容的可读视图）。"""
        status = {}
        for src in self.streams:
            mark = self.watermarks.get(src)
            status[src] = {
                "offset": self.offsets[src],
                "watermark": fmt_ts(mark) if mark is not None else None,
                "stalled": self.stalled[src],
                "generation": self.generations[src],
                "lateness_seconds": self.lateness.get(src),
                "confirmation_policy": self.confirmation_policy,
                "dropped_late": sum(1 for d in self.dropped
                                  if d["source"] == src),
                "live_records": sum(len(g)
                                    for g in self.live[src].values()),
                "role": "primary" if src == self.primary else "stream",
            }
        return status

    def set_stalled(self, source, stalled=True):
        """显式把某来源暂时视为不再推进（或恢复推进）。

        停发后该来源从有效水位线中剔除，因此可确认的窗口立即发布为
        新修订；恢复后其水位线重新计入，后到的迟到事件照常生成修订。
        决定持久化，暂停续跑后仍然有效。
        """
        if source not in self.streams:
            raise ValueError("未知来源: %s" % source)
        self.stalled[source] = bool(stalled)
        before = set(self.confirmed)
        revision = None
        windows = self._current_windows()
        confirmable = {k: None for k in windows if self._is_confirmed(k)}
        now_confirmable = {json.dumps(k) for k in confirmable}
        if stalled and now_confirmable != before:
            self._pending_causes.append(
                {"type": "stall_source", "source": source})
            revision = self._publish_revision()
        impact = {
            "op": "stall_source" if stalled else "resume_source",
            "source": source,
            "stalled": self.stalled[source],
            "effective_watermark": (
                fmt_ts(self.effective_watermark())
                if self.effective_watermark() is not None else None),
            "newly_confirmed": sorted(
                _window_label(k, self.size, self.group_by)["window_start"]
                for k in confirmable
                if json.dumps(k) in now_confirmable - before),
            "revision": revision,
        }
        self._append_impact(impact)
        self._save_state()
        return impact

    # ---- 事件应用 ----

    def _lateness_for(self, source, ts):
        """该来源对 ts 所在窗口生效的允许迟到秒数（None 表示不限制）。

        已确认窗口沿用确认时快照的策略；未确认窗口用当前策略——
        因此新的宽松策略不会悄悄重开已经发布的旧窗口。
        """
        start = int(ts // self.size) * self.size
        snapshot = self.window_confirmation.get(str(start))
        if snapshot is not None:
            return snapshot.get("lateness_seconds", {}).get(source)
        policy = self.window_policy.get(str(start))
        if policy is not None and source in policy:
            return policy[source]
        return self.lateness.get(source)

    def _adopt_window_policy(self, start):
        """窗口首次见到数据时固化当时口径与允许迟到期限。"""
        self.window_confirmation.setdefault(
            str(start), self._confirmation_snapshot((start, ())))

    def _too_late(self, source, ts):
        lateness = self._lateness_for(source, ts)
        mark = self.watermarks[source]
        if lateness is None or mark is None:
            return None
        return lateness if ts < mark - lateness else None

    def apply_event(self, event):
        op = event.get("op", "upsert")
        if op == "watermark":
            return self.advance_watermark(event["ts"],
                                          source=event.get("source"),
                                          generation=event.get("generation"))
        if op == "restart":
            return self.restart_source(event.get("source", self.primary))

        source = event.get("source", self.primary)
        if source not in self.streams:
            raise ValueError("未知来源: %s" % source)
        rec_id = event["id"]
        generation = int(event.get("generation", self.generations[source]))
        if generation > self.generations[source]:
            # 新代际首次随事件出现即采用（显式 restart 之外的方式）
            self.generations[source] = generation
        seq = event.get("seq")
        last = self.last_seq[source].setdefault(str(generation), {})
        if seq is not None and rec_id in last and seq <= last[rec_id]:
            return {"source": source, "id": rec_id, "op": op,
                    "generation": generation,
                    "skipped": "replay", "revision": None,
                    "window_deltas": {}}

        # 允许迟到时间门槛：超过 来源水位线 - 允许迟到 的事件被丢弃，
        # 但完整记录在案，修订研究（set_lateness scope="revision"）可重估
        ts_raw = event.get(self.time_field)
        if ts_raw is not None:
            ts = parse_ts(ts_raw)
            lateness = self._too_late(source, ts)
            if lateness is not None:
                entry = {"source": source, "generation": generation,
                         "id": rec_id, "seq": seq, "op": op,
                         "event": {k: v for k, v in event.items()},
                         "event_time": fmt_ts(ts),
                         "window_start": int(ts // self.size) * self.size,
                         "lateness_seconds": lateness,
                         "source_watermark": fmt_ts(
                             self.watermarks[source])}
                if not any(d["source"] == source
                           and d["generation"] == generation
                           and d["id"] == rec_id and d["seq"] == seq
                           for d in self.dropped):
                    self.dropped.append(entry)
                impact = {"source": source, "id": rec_id, "op": op,
                          "seq": seq, "generation": generation,
                          "dropped": "too_late",
                          "lateness_seconds": lateness,
                          "revision": None, "window_deltas": {}}
                self._append_impact(impact)
                self._save_state()
                return impact
            self._adopt_window_policy(int(ts // self.size) * self.size)

        before = self._current_windows()
        records = self.live[source].setdefault(str(generation), {})
        old_payload = records.get(rec_id)
        # 主来源事件的连接影响逐笔记录；维流事件的影响体现在窗口差额中
        old_rows = self._transform(old_payload) \
            if source == self.primary and old_payload is not None else []
        if op == "retract":
            records.pop(rec_id, None)
        elif op == "upsert":
            payload = {k: v for k, v in event.items()
                       if k not in ("op", "id", "seq", "source",
                                    "generation")}
            records[rec_id] = payload
        else:
            raise ValueError("未知事件 op: %s" % op)
        new_rows = self._transform(records[rec_id]) \
            if source == self.primary and rec_id in records else []
        if seq is not None:
            last[rec_id] = seq
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
            self._pending_causes.append({
                "type": "late_event", "source": source, "id": rec_id,
                "seq": seq, "generation": generation})
            revision = self._publish_revision()

        impact = {
            "source": source, "id": rec_id, "op": op, "seq": seq,
            "generation": generation,
            "join_removed": old_rows,   # 更正/撤回移除的连接结果
            "join_added": new_rows,     # 更正/新增产生的连接结果
            "window_deltas": deltas,    # 各窗口金额差额
            "revision": revision,
        }
        self._append_impact(impact)
        self._save_state()
        return impact

    def advance_watermark(self, ts, source=None, generation=None):
        source = source or self.primary
        if source not in self.streams:
            raise ValueError("未知来源: %s" % source)
        current = self.generations[source]
        gen = current if generation is None else int(generation)
        if gen < current:
            # 旧代际的水位线不能推进新实例的进度
            impact = {"op": "watermark", "source": source, "generation": gen,
                      "ignored": "old_generation", "revision": None}
            self._append_impact(impact)
            return impact
        mark = parse_ts(ts)
        if self.watermarks[source] is None or mark > self.watermarks[source]:
            self.watermarks[source] = mark
        revision = self._publish_revision()
        self._save_state()
        effective = self.effective_watermark()
        return {"op": "watermark", "source": source, "generation": gen,
                "watermark": fmt_ts(self.watermarks[source]),
                "effective_watermark": (fmt_ts(effective)
                                        if effective is not None else None),
                "revision": revision}

    def restart_source(self, source):
        """来源重启换代：代际号 +1，序号从新一代重新计数。

        旧代际的 seq 去重表、水位线不再约束新实例；旧实例迟到的
        记录仍可按迟到事件落入窗口（生成修订），但不能推进水位线。
        重启后该来源进度重置为未知，重新参与阻塞窗口确认。
        决定持久化，重开进程后代际与进度状态仍然清楚保留。
        """
        if source not in self.streams:
            raise ValueError("未知来源: %s" % source)
        self.generations[source] += 1
        self.watermarks[source] = None
        impact = {"op": "restart", "source": source,
                  "generation": self.generations[source],
                  "effective_watermark": (
                      fmt_ts(self.effective_watermark())
                      if self.effective_watermark() is not None else None),
                  "revision": None}
        self._append_impact(impact)
        self._save_state()
        return impact

    def set_lateness(self, source, seconds, scope="future", window_range=None,
                     confirm=False):
        """运行时调整某来源的允许迟到秒数（None 表示不限制）。

        scope="future"：只影响尚未确认的窗口；已发布窗口沿用确认时
        快照的旧策略，不会被新策略悄悄当作从未关闭。
        scope="revision"：必须显式 confirm=True，对 window_range=(start, end)
        内的历史窗口
        建立修订研究——更新这些窗口的策略快照，重新评估此前因超时
        被丢弃的事件；确实改变的窗口发布新修订，修订依据（来源与
        规则变化）记入 revision_causes；没有实际数据变化则不产生
        新修订。
        """
        if source not in self.streams:
            raise ValueError("未知来源: %s" % source)
        if seconds is not None and seconds < 0:
            raise ValueError("允许迟到时间不能为负: %r" % (seconds,))
        change = {"id": len(self.rule_changes) + 1, "source": source,
                  "lateness_seconds": seconds, "scope": scope,
                  "window_range": (list(window_range)
                                   if window_range else None),
                  "effective_watermark": (
                      fmt_ts(self.effective_watermark())
                      if self.effective_watermark() is not None else None)}
        self.rule_changes.append(change)
        impact = {"op": "set_lateness", "rule_change": change["id"],
                  "source": source, "lateness_seconds": seconds,
                  "scope": scope, "revision": None}
        if scope == "future":
            self.lateness[source] = seconds
            impact["applies_to"] = "unconfirmed_windows"
        elif scope == "revision":
            if not window_range:
                raise ValueError("scope='revision' 需要 window_range")
            if not confirm:
                raise ValueError("历史范围重研必须显式 confirm=True")
            start, end = (parse_ts(window_range[0]),
                          parse_ts(window_range[1]))
            for key in list(self.window_policy):
                if start <= float(key) < end:
                    self.window_policy[key][source] = seconds
                    snapshot = self.window_confirmation.get(key)
                    if snapshot is not None:
                        snapshot.setdefault("lateness_seconds", {})[source] = seconds
            self._pending_causes.append({
                "type": "lateness_rule", "rule_change": change["id"],
                "source": source, "lateness_seconds": seconds,
                "window_range": [fmt_ts(start), fmt_ts(end)]})
            reapplied, remaining = [], []
            for entry in self.dropped:
                if (entry["source"] == source
                        and start <= entry["window_start"] < end
                        and self._too_late(
                            source, parse_ts(entry["event_time"])) is None):
                    reapplied.append(entry)
                else:
                    remaining.append(entry)
            self.dropped = remaining
            revision = None
            for entry in reapplied:
                replay = dict(entry["event"])
                replay.setdefault("source", source)
                result = self.apply_event(replay)
                if result.get("revision"):
                    revision = result["revision"]
            impact["reapplied"] = [{"id": e["id"], "seq": e["seq"],
                                    "generation": e["generation"]}
                                   for e in reapplied]
            impact["revision"] = revision
            if revision is None:
                self._pending_causes.pop()  # 无实际变化，不留悬空依据
        else:
            raise ValueError("未知作用域: %s" % scope)
        self._append_impact(impact)
        self._save_state()
        return impact

    def set_confirmation_policy(self, policy, scope="future",
                                window_range=None, confirm=False):
        """设置确认口径：水位到达即发布，或等待允许迟到期限后再确认。"""
        if policy not in ("on_watermark", "after_lateness"):
            raise ValueError("未知确认口径: %s" % policy)
        change = {
            "id": len(self.rule_changes) + 1,
            "type": "confirmation_policy",
            "confirmation_policy": policy,
            "scope": scope,
            "window_range": (list(window_range)
                             if window_range else None),
            "effective_watermark": (
                fmt_ts(self.effective_watermark())
                if self.effective_watermark() is not None else None),
        }
        impact = {"op": "set_confirmation_policy",
                  "rule_change": change["id"],
                  "confirmation_policy": policy,
                  "scope": scope, "revision": None}
        if scope == "future":
            self.confirmation_policy = policy
            impact["applies_to"] = "unconfirmed_windows"
        elif scope == "revision":
            if not window_range:
                raise ValueError("scope='revision' 需要 window_range")
            if not confirm:
                raise ValueError("历史范围重研必须显式 confirm=True")
            start, end = parse_ts(window_range[0]), parse_ts(window_range[1])
            preserved = []
            for key_str, row in self.confirmed.items():
                key = json.loads(key_str)
                if start <= key[0] < end:
                    preserved.append({
                        "window_start": row["window_start"],
                        "revision_policy_kept":
                            self.window_confirmation[str(key[0])]["policy"],
                    })
            impact["preserved_published_windows"] = preserved
            impact["acknowledged"] = True
        else:
            raise ValueError("未知作用域: %s" % scope)
        self.rule_changes.append(change)
        self._append_impact(impact)
        self._save_state()
        return impact

    def revision_causes(self):
        """各次修订的依据：哪些来源的哪些事件/规则变化导致了结果改变。"""
        return {int(rev): list(causes)
                for rev, causes in self.rev_causes.items()}

    # ---- 视图 ----

    def tentative_view(self):
        """全部窗口当前值（含未确认的暂定窗口及其等待的来源）。"""
        rows = []
        for key, row in self._current_windows().items():
            row = dict(row)
            confirmed = self._is_confirmed(key)
            row["status"] = "confirmed" if confirmed else "tentative"
            row["waiting_on"] = [] if confirmed else self.waiting_on(key)
            row["confirmation_policy"] = self._confirmation_policy_for(key)
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
        confirmed = {}
        for key, row in windows.items():
            if self._is_confirmed(key):
                row = dict(row)
                row["status"] = "confirmed"
                key_str = json.dumps(key)
                old_row = self.confirmed.get(key_str)
                if old_row is not None:
                    for meta in ("confirmed_at", "confirmation_policy",
                                 "confirmed_watermark",
                                 "confirmation_deadline",
                                 "confirmation_deadlines"):
                        if meta in old_row:
                            row[meta] = old_row[meta]
                else:
                    start = key[0]
                    snapshot = self.window_confirmation.get(
                        str(start), self._confirmation_snapshot(key))
                    row["confirmed_at"] = datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%SZ")
                    row["confirmed_watermark"] = fmt_ts(
                        self.effective_watermark())
                    row["confirmation_policy"] = snapshot["policy"]
                    row["confirmation_deadline"] = snapshot["deadline"]
                    row["confirmation_deadlines"] = snapshot["deadlines"]
                confirmed[key_str] = row
        if confirmed == self.confirmed:
            return None     # 没有实际数据变化：不制造重复修订
        # 新确认的窗口快照当前各来源迟到策略：之后的策略调整
        # （scope="future"）不会把这些已发布窗口当作从未关闭
        for key_str in confirmed:
            if key_str not in self.confirmed:
                start = json.loads(key_str)[0]
                self.window_policy.setdefault(str(start),
                                              dict(self.lateness))
                self.window_confirmation.setdefault(
                    str(start), self._confirmation_snapshot(json.loads(key_str)))
        self.confirmed = confirmed
        self.revision += 1
        path = os.path.join(self.revisions_dir,
                            "rev-%05d.jsonl" % self.revision)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            for row in self.confirmed_view():
                fh.write(json.dumps(row, ensure_ascii=False, default=str))
                fh.write("\n")
        os.replace(tmp, path)
        # 记录导致本修订的来源与规则变化，随状态持久化
        self.rev_causes[str(self.revision)] = self._pending_causes
        self._pending_causes = []
        return self.revision

    # ---- 输入文件续跑 ----

    def run_events(self, events_path, limit=None, source=None):
        """按来源从已接收偏移继续消费事件文件；返回本次处理条数。

        source 缺省为主来源；文件内事件若自带 "source" 字段以事件为准，
        但偏移始终记在 source 指定的来源名下（一个文件对应一个来源）。
        暂停/受限后直接再次调用（或换进程重建引擎）即可续跑，
        各来源的已确认窗口、偏移与停发决定都会保留。
        """
        source = source or self.primary
        if source not in self.streams:
            raise ValueError("未知来源: %s" % source)
        applied = 0
        with open(events_path, "r", encoding="utf-8") as fh:
            fh.seek(self.offsets[source])
            while limit is None or applied < limit:
                line = fh.readline()
                if not line:
                    break
                self.offsets[source] = fh.tell()
                if line.strip():
                    event = json.loads(line)
                    event.setdefault("source", source)
                    self.apply_event(event)
                    applied += 1
        self._save_state()
        return applied

    # ---- 状态持久化 ----

    def _state_path(self):
        return os.path.join(self.state_dir, "state.json")

    def _save_state(self):
        data = {
            "state_version": 3,
            "live": self.live,
            "last_seq": self.last_seq,
            "watermarks": self.watermarks,
            "stalled": self.stalled,
            "offsets": self.offsets,
            "generations": self.generations,
            "lateness": self.lateness,
            "confirmation_policy": self.confirmation_policy,
            "window_policy": self.window_policy,
            "window_confirmation": self.window_confirmation,
            "dropped": self.dropped,
            "rule_changes": self.rule_changes,
            "rev_causes": self.rev_causes,
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
        live, seqs = data["live"], data["last_seq"]
        if data.get("state_version", 1) < 2:
            # 旧格式：live/last_seq 未按代际细分，全部归入第 0 代
            live = {s: {"0": live.get(s, {})} for s in self.streams}
            seqs = {s: {"0": seqs.get(s, {})} for s in self.streams}
        self.live = {s: {str(g): dict(ids)
                         for g, ids in live.get(s, {}).items()}
                     for s in self.streams}
        self.last_seq = {s: {str(g): dict(ids)
                             for g, ids in seqs.get(s, {}).items()}
                         for s in self.streams}
        self.watermarks = {s: data["watermarks"].get(s) for s in self.streams}
        self.stalled = {s: bool(data["stalled"].get(s, False))
                        for s in self.streams}
        self.offsets = {s: int(data["offsets"].get(s, 0))
                        for s in self.streams}
        self.generations = {s: int(data.get("generations", {}).get(s, 0))
                            for s in self.streams}
        self.lateness.update(data.get("lateness", {}))
        self.confirmation_policy = data.get("confirmation_policy",
                                           "on_watermark")
        self.window_policy = dict(data.get("window_policy", {}))
        self.window_confirmation = dict(data.get("window_confirmation", {}))
        for key_str, row in data.get("confirmed", {}).items():
            start = str(json.loads(key_str)[0])
            self.window_confirmation.setdefault(start, {
                "policy": row.get("confirmation_policy", "on_watermark"),
                "lateness_seconds": dict(self.window_policy.get(start, {})),
                "deadline": row.get("confirmation_deadline"),
                "deadlines": row.get("confirmation_deadlines", {}),
            })
        self.dropped = list(data.get("dropped", []))
        self.rule_changes = list(data.get("rule_changes", []))
        self.rev_causes = dict(data.get("rev_causes", {}))
        self.revision = data["revision"]
        self.confirmed = data["confirmed"]

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
