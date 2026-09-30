# 数据转换执行系统（dtsys）

纯 Python 标准库实现的数据转换执行系统。核心流程：**筛选 → 字段推导 → 两份数据连接**，
转换计划与运行实例分开持久化；后续新增算子沿用同一套批次与结果约定。

## 运行验证

```bash
python -m pytest tests/ -q      # 10 个测试（含倾斜热键/混合空值/背压取消）
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

## 分块连接（倾斜安全）

`join` 为递归分区哈希连接 + 倾斜兜底（`dtsys/operators/join.py`）：
- 两侧按连接键哈希分成 16 个桶落盘，逐桶处理，内存中只保留一个桶；
- 桶仍超预算时按哈希下一位递归；
- 若子分区几乎没有变小（热键/完全相同键，继续细分同一个键无意义）或哈希位
  用尽，转为**分块嵌套循环**：构建侧每次只持有一个预算内的块，探测侧逐行
  流式扫描，匹配结果**逐行**写出，绝不一次性生成整张乘积表；
- 多对多语义：左行 × 右行的每个重复匹配都输出，乘积数量一行不少，不去重；
- 空值混合（`None` / 键缺失 / 空串）按取值匹配：`None` 与缺失等价，
  空串是独立键；支持 `inner` / `left`。

## 在途数据与背压

所有阻塞结果（连接输出、流式分块落盘、最终汇总）都经过有界结果通道
`dtsys/flow.py` 的 `BoundedResultSink`：
- 按行的 JSON 字节记账，在途数据达到 `memory_bytes` 时生产者阻塞，
  由独立写出线程排空——下游读取/落盘很慢时同样受控，不丢输出；
- 单条超大结果允许独占通道以保证可推进；
- `RunConfig(slow_write_bytes=N)` 可模拟 N 字节/秒的慢速下游用于验证；
- 运行实例 `instance.json` 的 `metrics` 记录 `peak_inflight_bytes`
  （累计在途峰值）、`total_output_bytes`、`peak_join_work_bytes`（连接构建块峰值）。

## 取消

`CancellationToken().cancel()` 可在任意批次边界与每个连接分区处被检测到，
运行实例标记为 `cancelled`，临时结果清理后向上抛出。
