# database-distributed-transaction-recovery-planner

规划两阶段事务协调器故障后的提交和回滚恢复。

## 功能

针对协调者在部分参与者完成 prepare 后失败的场景，根据事务分支、prepare/commit
日志、协调者决策、参与者状态、超时、网络分区和已有 heuristic 结果，计算每个事务
最终应提交还是回滚，并输出：

- 每个分支的恢复动作（`send_commit` / `send_abort` / `wait` / `manual_review` / `blocked`）
- 阻塞资源（未决分支持有的锁）
- 最终决策来源（`commit_log` / `abort_log` / `coordinator_decision` / `heuristic` / `none`）
- 残余风险（跨分片部分提交、heuristic 偏离协调者意图、网络分区待重试等）

## 决策原则

- 已持久化的 commit 决策（commit 日志或协调者持久化决策）必须继续提交；
- 只有明确记录的 abort 才能回滚；
- 决策未知时输出等待协调者恢复（`wait_coordinator`）或人工裁决
  （`manual_review`），绝不自行猜测；未持久化的协调者记忆决策不作为提交依据；
- 参与者回报矛盾状态（如重放后既 committed 又 aborted）时保留证据、阻止重复提交，
  整个事务进入 `blocked`；
- 参与者已有 heuristic 结果时遵循该结果并标记高风险；不同参与者 heuristic 不一致
  时转人工裁决。

## 使用

```python
from recovery_planner import Branch, TransactionInput, plan_recovery

txn = TransactionInput(
    "txn-1",
    [Branch("b1", "shard-a", prepared=True, committed=True),
     Branch("b2", "shard-b", prepared=True)],
    commit_logged=True,
)
plan = plan_recovery([txn])[0]
print(plan.to_dict())
```

## 重要说明

**本工具只生成恢复计划，不连接真实数据库。** 它只消费日志与状态快照并输出计划
字典，实际的 commit/abort 下发、锁释放和人工裁决流程由调用方执行。

## 测试

```bash
python -m unittest discover -s tests -v
```

覆盖场景：协调者崩溃、prepare 后失败、commit 日志丢失、参与者重放、网络分区、
heuristic 结果一致/分歧。兼容 Python 3.8+。
