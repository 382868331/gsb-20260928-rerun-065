# 数据转换执行系统（dtsys）

纯 Python 标准库实现的数据转换执行系统。核心流程：**筛选 → 字段推导 → 两份数据连接**，
转换计划与运行实例分开持久化；后续新增算子沿用同一套批次与结果约定。

## 运行验证

```bash
python -m pytest tests/ -q      # 5 个测试
python demo/run_demo.py         # 小数据直接计算对照 + 强制小内存重跑
```

演示会：
1. 用 Python 直接计算出期望结果（reference）；
2. 正常内存预算执行计划；
3. 以 256 字节内存预算、128 字节单批大小强制执行同一计划（触发分块与临时落盘）；
4. 以多重集对比三者，结果必须完全一致。

## 存储布局

```
store/
  plans/<plan_id>/v<n>.json        计划版本，保存后不可变
  plans/<plan_id>/current.json     当前版本指针
  runs/<plan_id>/<run_id>/instance.json   运行实例（状态/预算/行数/错误）
  runs/<plan_id>/<run_id>/output/         成功发布的结果（result.jsonl + manifest.json）
  runs/<plan_id>/<run_id>/tmp/            本次临时分块，成功/失败/取消均清理
  published/<plan_id>/v<n>.json           已发布版本 -> run_id 指针，不覆盖
```

关键保证：
- 结果先写入 `output.staging/`，完整后原子 `os.replace` 为 `output/`；
- 取消或失败只清理本次运行的 `tmp/` 与 staging，已发布版本与其他运行不受影响；
- 同一计划版本重复运行不会覆盖首次发布指针。

## 计划格式

```json
{
  "plan_id": "orders-enrichment",
  "version": 1,
  "primary_source": "orders",
  "sources": {"orders": "data/orders.jsonl", "customers": "data/customers.jsonl"},
  "stages": [
    {"op": "filter", "expr": "amount >= 100"},
    {"op": "derive", "fields": {"taxed": "round(amount * 1.1, 2)"}},
    {"op": "join", "right_source": "customers", "left_on": "cust_id", "how": "inner"}
  ]
}
```

表达式为白名单 AST 求值（`dtsys/expr.py`），支持算术/比较/布尔/三元与
`round/abs/len/min/max/int/float/str/bool`，无属性访问与导入能力。

## 批次与结果约定（新增算子照此即可）

- 数据单位是 `Batch(rows: list[dict], columns: list[str])`（`dtsys/batches.py`）；
- 中间数据统一以 JSONL 分块写入本次运行的 `tmp/`，按 `chunk_bytes` 切批；
- **流式算子**：继承 `Operator`，实现 `process_batch(batch) -> Batch`，
  执行器逐批喂入、逐块落盘；
- **阻塞算子**：设置 `streaming = False`，实现
  `run(input_files, columns) -> (output_files, columns)`，自行决定落盘策略；
- 用 `@register_operator("名字")` 注册，并在 `dtsys/operators/__init__.py` 导入。

## 分块连接

`join` 为递归分区哈希连接（`dtsys/operators/join.py`）：
- 两侧按连接键哈希分成 16 个桶落盘，逐桶处理，内存中只保留一个桶；
- 桶仍超预算时按哈希下一位递归；
- 哈希位用尽（深度 16）仍超预算时，是无法再切开的倾斜键（如两侧完全同键），
改用**分块嵌套循环连接**：两侧各切成不超过预算一半的块，左块 × 右块逐对
  配对，内存中始终只驻留一对块；输出行产生一行写一行，不物化笛卡尔积，
  乘积数量一行不少，也不靠无限细分同一个键回避问题；
- 多对多语义：左行 × 右行的每个重复匹配都输出，不去重；支持 `inner` / `left`；
- 运行实例记录 `peak_inflight_bytes`（累计在途数据峰值，行 JSON 字节口径）。

## 取消

`CancellationToken().cancel()` 可在任意批次边界与每个连接分区处被检测到，
运行实例标记为 `cancelled`，临时结果清理后向上抛出。
## 事件时间分组窗口（dtsys/windowing.py）

在批式计划之外提供**增量窗口引擎** WindowEngine，消费变更流（changelog），
支持乱序到达、撤回/更正、暂定/确认视图与不可变修订发布。

事件格式（JSONL 每行一条）：

    {"op": "upsert",  "id": "e1", "seq": 1, "event_time": "...", ...业务字段}
    {"op": "retract", "id": "e1", "seq": 2}
    {"op": "watermark", "ts": "..."}

关键语义：
- 窗口按 event_time 分组，乱序到达不影响最终结果；
- 同一 id 的 upsert 覆盖旧值（更正），retract 撤回整条记录；
- seq 单调递增，已应用的 seq 重放直接跳过，不会重复计入；
- 窗口结束 <= 水位线即确认；迟到事件改动已确认窗口时生成新的
  revisions/rev-<n>.jsonl，旧修订文件永不改写；
- tentative_view() / confirmed_view() 分别查看暂定与已确认窗口；
- 每条事件的影响（连接结果增删 join_added/join_removed、各窗口金额
  差额 window_deltas）逐笔记入 impact.jsonl；
- 状态全部持久化在 state_dir，run_events(path, limit=n) 处理受限或
  暂停后，用同一目录重建引擎即可从已接收偏移续跑，确认窗口不丢失；
- diff_revisions(old, new) 逐窗口核对两个修订的新增/删除/金额差额。

窗口可与 filter / derive / join 组合（spec.stages 与批式计划同构）；
连接保留维表合法重复（一条事实可放大为多行），撤回时全部移除。

    python demo/run_window_demo.py    # 乱序 + 跨窗口更正 + 暂停续跑演示

## 多来源水位线与停发恢复

窗口引擎可接收多个独立来源（spec.streams），各自维护到达位置（offset）、
事件时间进度（watermark）与暂时停发标记（stalled）：

- 事件带 `"source"` 字段；不同来源使用同样的记录编号仍是不同事件，
  重放、更正和撤回只作用于对应来源（seq 按来源各自计数）；
- 有效水位线 = 全部未停发来源水位线的最小值；某来源没有数据到达本身
  不能解释为该来源已结束，它会一直阻塞窗口确认；
- `tentative_view()` 每行带 `waiting_on`，说明窗口为何仍暂定、在等哪个
  来源；`source_status()` 给出各来源的接受位置、进度与停发决定；
- `set_stalled(source, True)` 显式把某来源暂时视为不再推进：它从有效
  水位线中剔除，可确认的窗口立即发布为新修订；该决定持久化，
  重开进程后仍然有效；
- 被停发的来源再次活跃时，后到的记录可能影响已发布窗口，按迟到事件
  生成有明确依据的新修订，旧修订文件永不改写；
- join 的 `right_source` 可以是另一个流式来源：窗口确认同时受两侧
  进度约束，不能只按较快一侧提前宣布完整；合法一对多匹配保留放大，
  维流更正会重算连接结果并按需发布修订；
- 保存点（state.json）还原各来源的接受位置及停发决定，
  `run_events(path, source=...)` 按来源从已接收偏移续跑，
  尚未完成的来源范围在 `source_status()` 中清楚保留。

    python demo/run_multisource_demo.py    # 停发后恢复 + 迟到更正修订演示

## 允许迟到策略与来源重启代际

运行中可调整各来源的允许迟到时间，并区分来源重启前后的数据代际：

- `set_lateness(source, seconds)`：新策略默认只影响**今后的窗口**
  （从当前有效水位线起生效），已经发布的旧窗口保持已确认，不会被
  新策略悄悄当作从未关闭；`effective_from=...` 可显式指定生效起点；
- `set_lateness(source, seconds, restudy=(start, end))`：对指定历史
  范围建立修订研究，只重评窗口起始时间落在 [start, end) 内的窗口；
  结果有实际变化才发布新修订，没有数据变化的窗口不制造重复修订；
- 每条修订附带 `revision_meta/rev-<n>.json`，列出导致本次变化的
  来源与规则变化（事件/水位线/停发/迟到策略调整/重启）及受影响窗口；
- `restart_source(source)`：标记来源重启，进入新数据代际——新代际
  seq 重新计数（与旧代际各自去重）、水位线重置为未知；旧代际迟到的
  数据事件仍按迟到数据修订已发布窗口，但旧代际水位线不能推进新实例；
  事件显式携带更高 `"generation"` 时视为该来源已重启；
- 确认状态单调：已确认过的窗口即使来源重启、水位线重置也保持已确认，
  其发布的永远是最新值；`source_status()` 展示各来源代际与生效中的
  迟到规则，暂停/恢复/重启交错时进度可解释。

    python -m pytest tests/test_lateness_generations.py -q
