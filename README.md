# 数据转换执行系统（dtsys）

纯 Python 标准库实现的数据转换执行系统。核心流程：**筛选 → 字段推导 → 两份数据连接 → 分组聚合**，
转换计划与运行实例分开持久化；后续新增算子沿用同一套批次与结果约定。

## 运行验证

```bash
python -m pytest tests/ -q      # 9 个测试
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
  runs/<plan_id>/<run_id>/instance.json   运行实例（状态/预算/行数/错误/保存点）
  runs/<plan_id>/<run_id>/checkpoints/    取消后保留的已完成阶段，恢复后删除
  runs/<plan_id>/<run_id>/output/         成功发布的结果（result.jsonl + manifest.json）
  runs/<plan_id>/<run_id>/tmp/            本次临时分块，成功/失败/取消均清理
  published/<plan_id>/v<n>.json           已发布版本 -> run_id 指针，不覆盖
```

关键保证：
- 结果先写入 `output.staging/`，完整后原子 `os.replace` 为 `output/`；
- 每个阶段完整后才写 `checkpoints/stage-nnn/manifest.json`，未提交的下一阶段目录恢复时丢弃；
- 取消或失败清理本次运行的 `tmp/` 与 staging，但保留已提交 checkpoints；成功后删除 checkpoints；
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
    {"op": "join", "right_source": "customers", "left_on": "cust_id", "how": "inner"},
    {"op": "aggregate", "group_by": ["level"],
     "aggregations": {"orders": "count(*)", "amount": "sum(taxed)"}}
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

## 聚合与缓冲预算

`aggregate` 支持 `count(*)`、`count(expr)`、`sum(expr)`、`min(expr)`、`max(expr)`、
`avg(expr)`。分组键与聚合状态超预算时按键递归 16 路分区；每个分区独立聚合，
不去重也不折叠同键重复行。表达式中的空值按 SQL 风格传播为空，`count(expr)`
只统计非空表达式结果。

预算口径是**驻留行/状态的 JSON 字节**，不是磁盘空间，也不计算 Python 对象头和 OS
文件缓冲：
- 流式阶段逐行处理，最多保留一个输入行与一个不超过 `chunk_bytes` 的输出批；
- 阻塞连接最多驻留左右一对预算块，输出行立即写盘，不物化倾斜键乘积；
- 阻塞聚合只驻留当前分区的分组状态，超过预算先分区再读取；
- 单行或单键状态本身超过预算时属于不可再切分例外，实例中的
  `peak_inflight_bytes` 会如实记录实际驻留值。

阶段之间以提交后的分块文件作为持久、有界队列：下游慢时上游只推进到当前阶段保存点，
不会把未来阶段的数据堆进内存；恢复从最近的阶段保存点继续，未提交阶段整体重跑。

## 取消

`CancellationToken().cancel()` 可在逐行边界、连接分区与聚合分区处被检测到。
运行实例标记为 `cancelled`，只保留已提交阶段；用
`RunConfig(..., resume=True)` 和同一 `run_id` 恢复。正式输出只在最终结果和清单完整后
原子发布，因此恢复不会重复发布，`tmp/` 与未提交 checkpoint 也不会成为正式结果。
