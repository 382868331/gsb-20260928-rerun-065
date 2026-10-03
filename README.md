# 数据转换执行系统（dtsys）

纯 Python 标准库实现的数据转换执行系统。核心流程：**筛选 → 字段推导 → 两份数据连接**，
转换计划与运行实例分开持久化；后续新增算子沿用同一套批次与结果约定。

## 运行验证

```bash
python -m pytest tests/ -q      # 25 个测试
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

在批式计划之外提供**多来源增量窗口引擎** WindowEngine，消费变更流（changelog），
支持乱序到达、撤回/更正、暂定/确认视图、不可变修订发布，以及
**多个独立来源各自的到达位置、事件时间水位线与暂时停发决定**。

事件格式（JSONL 每行一条）：

    {"source": "orders", "op": "upsert",  "id": "e1", "seq": 1, "event_time": "...", ...业务字段}
    {"source": "orders", "op": "retract", "id": "e1", "seq": 2}
    {"source": "orders", "op": "watermark", "ts": "..."}
    {"op": "exclude_source", "source": "payments", "reason": "对方暂停"}
    {"op": "resume_source",  "source": "payments"}

关键语义：
- 窗口按 event_time 分组，乱序到达不影响最终结果；
- 身份键是 (source, id)：不同来源可以共用同一个 id，
  upsert/retract/重放只作用于对应来源；seq 也只在来源内比较；
- 同一 (source, id) 的 upsert 覆盖旧值（更正），retract 撤回整条记录；
- seq 单调递增，已应用的 seq 重放直接跳过，不会重复计入；
- 有效水位线是所有活跃来源水位线的最小值；没有数据到达不等于
  来源结束——未报告过水位线的来源会一直阻塞确认；只有显式
  exclude_source(name, reason) 才暂时把来源排除在水位线之外，
  publish_preview 可先预览"若停发可发布哪些窗口"而不改状态；
- resume_source(name) 恢复来源后水位线约束重新生效，迟到数据照常进入；
- 窗口结束 <= 有效水位线即确认；迟到事件改动已发布窗口时生成新的
  revisions/rev-<n>.jsonl，旧修订文件永不改写；
- 每次修订同时写 revisions/rev-<n>.basis.json，记录发布依据
  （触发事件、基准来源、当时各来源水位线与停发状态）；
- window_holds() / tentative_view() 对每个暂定窗口给出 waiting_for：
  正在等待哪个来源（无水位线/水位线未越过窗口/连接匹配未齐）；
- confirmed_view() 查看已确认窗口；
- 每条事件的影响（连接结果增删 join_added/join_removed、各窗口金额
  差额 window_deltas）逐笔记入 impact.jsonl，停发/恢复记入 decisions.jsonl；
- spec 用 stream_sources 声明参与水位线的来源，用 fact_sources
  声明哪些来源产生窗口事实（其余来源可作为流式 join 维表）；
- join 右来源若没有静态文件，就被视为一路流资料：窗口必须同时满足
  两侧来源进度与合法一对多匹配，较快一侧不能单独宣布完整；
- 状态全部持久化在 state_dir，run_events(path, limit=n, source=...)
  按来源记录已接受字节偏移；export_savepoint() 导出各来源接受位置、
  水位线与停发决定，换进程用同一目录重建即可还原续跑；
- diff_revisions(old, new) 逐窗口核对两个修订的新增/删除/金额差额。

窗口可与 filter / derive / join 组合（spec.stages 与批式计划同构）；
连接保留维表合法重复（一条事实可放大为多行），撤回时全部移除。

    python demo/run_window_demo.py               # 单来源乱序/更正/暂停续跑
    python demo/run_multi_source_window_demo.py  # 多来源停发恢复/连接/保存点
