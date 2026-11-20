"""Two-phase commit (2PC) failure recovery planner.

Computes recovery plans from logs and observed state only; it never
connects to a real database.

Decision principles:
- A persisted commit decision must be driven to completion.
- Only an explicitly recorded abort may roll back.
- When the decision is unknown, output wait-for-coordinator or
  manual-review; never guess.
- When participants report inconsistent results, preserve evidence and
  block duplicate commits.
"""

from enum import Enum


class FinalDecision(Enum):
    COMMIT = "commit"
    ABORT = "abort"
    WAIT_COORDINATOR = "wait_coordinator"
    MANUAL_REVIEW = "manual_review"
    BLOCKED = "blocked"


class BranchAction(Enum):
    SEND_COMMIT = "send_commit"
    SEND_ABORT = "send_abort"
    WAIT = "wait"
    MANUAL_REVIEW = "manual_review"
    BLOCKED = "blocked"


class DecisionSource(Enum):
    COMMIT_LOG = "commit_log"
    ABORT_LOG = "abort_log"
    COORDINATOR_DECISION = "coordinator_decision"
    HEURISTIC = "heuristic"
    NONE = "none"


class Branch(object):
    """Observed state of one transaction branch (participant side)."""

    def __init__(self, branch_id, participant,
                 prepared=False, committed=False, aborted=False,
                 heuristic=None, reachable=True):
        self.branch_id = branch_id
        self.participant = participant
        self.prepared = prepared
        self.committed = committed
        self.aborted = aborted
        # Heuristic decision already taken by the participant:
        # "commit" / "abort" / None
        self.heuristic = heuristic
        # False when a network partition makes the participant unreachable
        self.reachable = reachable

    def inconsistent(self):
        """Participant reports contradictory state (e.g. after a replay
        it is both committed and aborted)."""
        return self.committed and self.aborted


class TransactionInput(object):
    """All recovery inputs for a single transaction."""

    def __init__(self, txn_id, branches,
                 commit_logged=False, abort_logged=False,
                 coordinator_decision=None, coordinator_decision_persisted=False,
                 decision_timed_out=False):
        self.txn_id = txn_id
        self.branches = list(branches)
        self.commit_logged = commit_logged
        self.abort_logged = abort_logged
        # Coordinator's in-memory decision: "commit" / "abort" / None
        self.coordinator_decision = coordinator_decision
        self.coordinator_decision_persisted = coordinator_decision_persisted
        self.decision_timed_out = decision_timed_out


class BranchPlan(object):
    def __init__(self, branch_id, participant, action, blocked_resources,
                 decision_source, risks, evidence=None):
        self.branch_id = branch_id
        self.participant = participant
        self.action = action
        self.blocked_resources = list(blocked_resources)
        self.decision_source = decision_source
        self.risks = list(risks)
        self.evidence = dict(evidence or {})

    def to_dict(self):
        return {
            "branch_id": self.branch_id,
            "participant": self.participant,
            "action": self.action.value,
            "blocked_resources": self.blocked_resources,
            "decision_source": self.decision_source.value,
            "risks": self.risks,
            "evidence": self.evidence,
        }


class TransactionPlan(object):
    def __init__(self, txn_id, final_decision, decision_source, branch_plans,
                 blocked_resources, residual_risks):
        self.txn_id = txn_id
        self.final_decision = final_decision
        self.decision_source = decision_source
        self.branch_plans = list(branch_plans)
        self.blocked_resources = list(blocked_resources)
        self.residual_risks = list(residual_risks)

    def to_dict(self):
        return {
            "txn_id": self.txn_id,
            "final_decision": self.final_decision.value,
            "decision_source": self.decision_source.value,
            "branches": [b.to_dict() for b in self.branch_plans],
            "blocked_resources": self.blocked_resources,
            "residual_risks": self.residual_risks,
        }


def _locked_resource(branch):
    return "%s:%s" % (branch.participant, branch.branch_id)


def plan_transaction(txn):
    """Compute the recovery plan for a single transaction."""
    # 1. Contradictory participant state: preserve evidence, block retries.
    bad = [b for b in txn.branches if b.inconsistent()]
    if bad:
        branch_plans = []
        for b in txn.branches:
            evidence = {}
            if b in bad:
                evidence = {
                    "committed": b.committed,
                    "aborted": b.aborted,
                    "prepared": b.prepared,
                    "heuristic": b.heuristic,
                }
            branch_plans.append(BranchPlan(
                b.branch_id, b.participant, BranchAction.BLOCKED,
                [_locked_resource(b)], DecisionSource.NONE,
                ["participant reported inconsistent state; "
                 "duplicate commit suppressed"],
                evidence=evidence))
        return TransactionPlan(
            txn.txn_id, FinalDecision.BLOCKED, DecisionSource.NONE,
            branch_plans, [_locked_resource(b) for b in bad],
            ["inconsistent participant reports; manual arbitration "
             "required before any retry"])

    # 2. Persisted commit (log or persisted coordinator decision): must
    #    continue to commit.
    commit_persisted = txn.commit_logged or (
        txn.coordinator_decision == "commit"
        and txn.coordinator_decision_persisted)
    if commit_persisted:
        source = DecisionSource.COMMIT_LOG if txn.commit_logged \
            else DecisionSource.COORDINATOR_DECISION
        return _finalize(txn, FinalDecision.COMMIT, source,
                         BranchAction.SEND_COMMIT, "commit")

    # 3. Explicitly recorded abort: roll back.
    abort_recorded = txn.abort_logged or (
        txn.coordinator_decision == "abort"
        and txn.coordinator_decision_persisted)
    if abort_recorded:
        source = DecisionSource.ABORT_LOG if txn.abort_logged \
            else DecisionSource.COORDINATOR_DECISION
        return _finalize(txn, FinalDecision.ABORT, source,
                         BranchAction.SEND_ABORT, "abort")

    # 4. Existing heuristic outcomes: follow them, flagged as high risk.
    heuristics = {b.heuristic for b in txn.branches if b.heuristic}
    if heuristics:
        if len(heuristics) > 1:
            # Participants took divergent heuristic decisions: a partial
            # commit across shards has already happened.
            branch_plans = [BranchPlan(
                b.branch_id, b.participant, BranchAction.MANUAL_REVIEW,
                [_locked_resource(b)], DecisionSource.HEURISTIC,
                ["divergent heuristic decisions across participants"],
                evidence={"heuristic": b.heuristic}) for b in txn.branches]
            return TransactionPlan(
                txn.txn_id, FinalDecision.MANUAL_REVIEW,
                DecisionSource.HEURISTIC, branch_plans,
                [_locked_resource(b) for b in txn.branches],
                ["mixed heuristic commit/abort: partial commit already "
                 "occurred"])
        decision = heuristics.pop()
        final = FinalDecision.COMMIT if decision == "commit" \
            else FinalDecision.ABORT
        action = BranchAction.SEND_COMMIT if decision == "commit" \
            else BranchAction.SEND_ABORT
        plan = _finalize(txn, final, DecisionSource.HEURISTIC, action,
                         decision)
        plan.residual_risks.append(
            "heuristic decision may diverge from coordinator's "
            "original intent")
        return plan

    # 5. Decision unknown: wait for coordinator recovery or manual
    #    arbitration; never guess.
    if txn.decision_timed_out:
        final, action = FinalDecision.MANUAL_REVIEW, BranchAction.MANUAL_REVIEW
        risk = "decision unknown and timed out; manual arbitration required"
    else:
        final, action = FinalDecision.WAIT_COORDINATOR, BranchAction.WAIT
        risk = "decision unknown; waiting for coordinator recovery"
    branch_plans = []
    blocked = []
    for b in txn.branches:
        resources = [_locked_resource(b)] \
            if b.prepared and not (b.committed or b.aborted) else []
        blocked.extend(resources)
        branch_plans.append(BranchPlan(
            b.branch_id, b.participant, action, resources,
            DecisionSource.NONE, [risk]))
    return TransactionPlan(txn.txn_id, final, DecisionSource.NONE,
                           branch_plans, blocked, [risk])


def _finalize(txn, final, source, action, keyword):
    """Build per-branch actions for a decided outcome, handling
    unreachable participants and replays."""
    branch_plans = []
    blocked = []
    risks = []
    done_attr = "committed" if keyword == "commit" else "aborted"
    for b in txn.branches:
        branch_risks = []
        resources = []
        already = getattr(b, done_attr)
        if not b.reachable and not already:
            # Network partition: action must be retried, resource stays
            # blocked.
            resources.append(_locked_resource(b))
            branch_risks.append(
                "participant unreachable (network partition); %s must be "
                "retried" % keyword)
        if already:
            branch_risks.append(
                "participant already %s; replay of %s is idempotent"
                % (done_attr, keyword))
        blocked.extend(resources)
        risks.extend(branch_risks)
        branch_plans.append(BranchPlan(
            b.branch_id, b.participant, action, resources, source,
            branch_risks))
    states = {getattr(b, done_attr) for b in txn.branches}
    if len(states) > 1:
        risks.append(
            "partial %s observed across shards; remaining branches must "
            "converge" % keyword)
    return TransactionPlan(txn.txn_id, final, source, branch_plans, blocked,
                           risks)


def plan_recovery(transactions):
    """Compute recovery plans for a batch of transactions."""
    return [plan_transaction(t) for t in transactions]
