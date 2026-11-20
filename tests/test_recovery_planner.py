import unittest

from recovery_planner import (
    Branch,
    BranchAction,
    DecisionSource,
    FinalDecision,
    TransactionInput,
    plan_recovery,
    plan_transaction,
)


class CoordinatorCrashTest(unittest.TestCase):
    """协调者在 prepare 阶段崩溃，未留下任何决策记录。"""

    def test_wait_for_coordinator_when_no_decision(self):
        txn = TransactionInput(
            "txn-1",
            [Branch("b1", "shard-a", prepared=True),
             Branch("b2", "shard-b", prepared=True)],
        )
        plan = plan_transaction(txn)
        self.assertEqual(plan.final_decision, FinalDecision.WAIT_COORDINATOR)
        self.assertEqual(plan.decision_source, DecisionSource.NONE)
        self.assertEqual(
            [b.action for b in plan.branch_plans],
            [BranchAction.WAIT, BranchAction.WAIT])
        # 已 prepare 的分支资源保持阻塞
        self.assertEqual(sorted(plan.blocked_resources),
                         ["shard-a:b1", "shard-b:b2"])

    def test_manual_review_after_timeout(self):
        txn = TransactionInput(
            "txn-2",
            [Branch("b1", "shard-a", prepared=True)],
            decision_timed_out=True,
        )
        plan = plan_transaction(txn)
        self.assertEqual(plan.final_decision, FinalDecision.MANUAL_REVIEW)
        self.assertEqual(plan.branch_plans[0].action,
                         BranchAction.MANUAL_REVIEW)

    def test_persisted_coordinator_commit_continues(self):
        txn = TransactionInput(
            "txn-3",
            [Branch("b1", "shard-a", prepared=True),
             Branch("b2", "shard-b", prepared=True)],
            coordinator_decision="commit",
            coordinator_decision_persisted=True,
        )
        plan = plan_transaction(txn)
        self.assertEqual(plan.final_decision, FinalDecision.COMMIT)
        self.assertEqual(plan.decision_source,
                         DecisionSource.COORDINATOR_DECISION)
        self.assertTrue(all(b.action == BranchAction.SEND_COMMIT
                            for b in plan.branch_plans))


class FailureAfterPrepareTest(unittest.TestCase):
    """部分参与者 prepare 完成后协调者失败，commit 决策已落盘。"""

    def test_commit_decision_must_complete(self):
        txn = TransactionInput(
            "txn-10",
            [Branch("b1", "shard-a", prepared=True, committed=True),
             Branch("b2", "shard-b", prepared=True)],
            commit_logged=True,
        )
        plan = plan_transaction(txn)
        self.assertEqual(plan.final_decision, FinalDecision.COMMIT)
        self.assertEqual(plan.decision_source, DecisionSource.COMMIT_LOG)
        actions = {b.branch_id: b.action for b in plan.branch_plans}
        self.assertEqual(actions["b1"], BranchAction.SEND_COMMIT)
        self.assertEqual(actions["b2"], BranchAction.SEND_COMMIT)
        # 跨分片部分提交应被标记为残余风险
        self.assertTrue(any("partial commit" in r
                            for r in plan.residual_risks))

    def test_abort_recorded_rolls_back(self):
        txn = TransactionInput(
            "txn-11",
            [Branch("b1", "shard-a", prepared=True)],
            abort_logged=True,
        )
        plan = plan_transaction(txn)
        self.assertEqual(plan.final_decision, FinalDecision.ABORT)
        self.assertEqual(plan.decision_source, DecisionSource.ABORT_LOG)
        self.assertEqual(plan.branch_plans[0].action, BranchAction.SEND_ABORT)


class CommitLogLostTest(unittest.TestCase):
    """commit 日志丢失：协调者记得决策但未持久化。"""

    def test_unpersisted_decision_is_not_trusted(self):
        txn = TransactionInput(
            "txn-20",
            [Branch("b1", "shard-a", prepared=True),
             Branch("b2", "shard-b", prepared=True)],
            coordinator_decision="commit",
            coordinator_decision_persisted=False,
        )
        plan = plan_transaction(txn)
        # 未持久化的决策不能作为提交依据，必须等待或人工裁决
        self.assertIn(plan.final_decision,
                      (FinalDecision.WAIT_COORDINATOR,
                       FinalDecision.MANUAL_REVIEW))
        self.assertNotEqual(plan.final_decision, FinalDecision.COMMIT)


class ParticipantReplayTest(unittest.TestCase):
    """参与者重放后回报矛盾状态。"""

    def test_inconsistent_participant_blocks_and_preserves_evidence(self):
        txn = TransactionInput(
            "txn-30",
            [Branch("b1", "shard-a", prepared=True,
                    committed=True, aborted=True),
             Branch("b2", "shard-b", prepared=True)],
            commit_logged=True,
        )
        plan = plan_transaction(txn)
        self.assertEqual(plan.final_decision, FinalDecision.BLOCKED)
        self.assertTrue(all(b.action == BranchAction.BLOCKED
                            for b in plan.branch_plans))
        bad = [b for b in plan.branch_plans if b.branch_id == "b1"][0]
        self.assertEqual(bad.evidence["committed"], True)
        self.assertEqual(bad.evidence["aborted"], True)
        self.assertTrue(any("duplicate commit suppressed" in r
                            for r in bad.risks))

    def test_idempotent_replay_of_commit(self):
        txn = TransactionInput(
            "txn-31",
            [Branch("b1", "shard-a", prepared=True, committed=True)],
            commit_logged=True,
        )
        plan = plan_transaction(txn)
        self.assertEqual(plan.final_decision, FinalDecision.COMMIT)
        self.assertTrue(any("idempotent" in r
                            for r in plan.branch_plans[0].risks))


class NetworkPartitionTest(unittest.TestCase):
    """网络分区导致部分参与者不可达。"""

    def test_unreachable_participant_stays_blocked(self):
        txn = TransactionInput(
            "txn-40",
            [Branch("b1", "shard-a", prepared=True, committed=True),
             Branch("b2", "shard-b", prepared=True, reachable=False)],
            commit_logged=True,
        )
        plan = plan_transaction(txn)
        self.assertEqual(plan.final_decision, FinalDecision.COMMIT)
        unreachable = [b for b in plan.branch_plans
                       if b.branch_id == "b2"][0]
        self.assertEqual(unreachable.blocked_resources, ["shard-b:b2"])
        self.assertTrue(any("network partition" in r
                            for r in unreachable.risks))
        self.assertIn("shard-b:b2", plan.blocked_resources)


class HeuristicTest(unittest.TestCase):
    """已有 heuristic 结果的处理。"""

    def test_uniform_heuristic_commit_is_followed_with_risk(self):
        txn = TransactionInput(
            "txn-50",
            [Branch("b1", "shard-a", prepared=True, heuristic="commit"),
             Branch("b2", "shard-b", prepared=True, heuristic="commit")],
        )
        plan = plan_transaction(txn)
        self.assertEqual(plan.final_decision, FinalDecision.COMMIT)
        self.assertEqual(plan.decision_source, DecisionSource.HEURISTIC)
        self.assertTrue(any("heuristic" in r
                            for r in plan.residual_risks))

    def test_divergent_heuristics_require_manual_review(self):
        txn = TransactionInput(
            "txn-51",
            [Branch("b1", "shard-a", prepared=True, heuristic="commit"),
             Branch("b2", "shard-b", prepared=True, heuristic="abort")],
        )
        plan = plan_transaction(txn)
        self.assertEqual(plan.final_decision, FinalDecision.MANUAL_REVIEW)
        self.assertTrue(any("partial commit" in r
                            for r in plan.residual_risks))


class BatchTest(unittest.TestCase):
    def test_plan_recovery_returns_plan_per_transaction(self):
        txns = [
            TransactionInput("t1", [Branch("b1", "s1", prepared=True)],
                             commit_logged=True),
            TransactionInput("t2", [Branch("b2", "s2", prepared=True)]),
        ]
        plans = plan_recovery(txns)
        self.assertEqual(len(plans), 2)
        self.assertEqual(plans[0].final_decision, FinalDecision.COMMIT)
        self.assertEqual(plans[1].final_decision,
                         FinalDecision.WAIT_COORDINATOR)
        # 输出可序列化
        d = plans[0].to_dict()
        self.assertEqual(d["final_decision"], "commit")
        self.assertEqual(d["decision_source"], "commit_log")


if __name__ == "__main__":
    unittest.main()
