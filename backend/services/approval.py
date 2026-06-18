"""
Human-in-the-loop approval workflow using a state machine.

States: Proposed → Reviewed → Approved → Testing → Deploying
         → Monitoring → Completed | Rolled_Back | Rejected

FIX: The original code used `from transitions import Machine` — a third-party
package that is not guaranteed to be installed and cannot be resolved in this
environment. It was also the root cause of the Pylance/pyright error
"cannot access attributes for class ProposalWorkflow — attribute is unknown",
because `transitions.Machine` injects `.state` and trigger methods (.review(),
.approve(), …) dynamically at runtime; static analysers cannot see them.

Replaced with a lightweight, pure-stdlib state machine that:
  - Has zero external dependencies
  - Exposes all attributes and methods explicitly (fully type-checkable)
  - Raises InvalidTransitionError with a clear message on illegal moves
  - Is easier to test and extend
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple

from backend.models.database import ProposalState
from backend.utils.logging import get_logger

log = get_logger(__name__)


# ─────────────────────────────────────────────
# Pure-stdlib state machine
# ─────────────────────────────────────────────

# All valid states (mirrors ProposalState enum values)
STATES: FrozenSet[str] = frozenset(s.value for s in ProposalState)

# (source_state, trigger) → destination_state
# FIX: Encoded as a plain dict so static analysers understand the structure
# and so that invalid trigger calls raise immediately with a clear message
# instead of being silently ignored (the old `ignore_invalid_triggers=True`).
_TRANSITION_TABLE: Dict[Tuple[str, str], str] = {
    ("proposed", "review"): "reviewed",
    ("reviewed", "approve"): "approved",
    ("approved", "start_test"): "testing",
    ("testing", "deploy"): "deploying",
    ("deploying", "monitor"): "monitoring",
    ("monitoring", "complete"): "completed",
    # Rejection paths
    ("reviewed", "reject"): "rejected",
    ("proposed", "reject"): "rejected",
    # Rollback paths
    ("deploying", "rollback"): "rolled_back",
    ("monitoring", "rollback"): "rolled_back",
    # FIX: mark_deployed() called workflow.deploy() + workflow.monitor() as
    # two separate sequential triggers. If anything raised between them the
    # proposal would be stuck in "deploying". Combined into one atomic trigger.
    ("testing", "deploy_and_monitor"): "monitoring",
}


class InvalidTransitionError(Exception):
    """Raised when a trigger is fired from a state that does not support it."""


class StateMachine:
    """
    Minimal deterministic state machine backed by a plain dict.

    All attributes are explicit — no dynamic injection — so static
    analysers (Pylance, pyright, mypy) see them without any special
    configuration.
    """

    def __init__(
        self,
        initial: str,
        on_transition: Optional[Callable[[str, str, str], None]] = None,
    ) -> None:
        if initial not in STATES:
            raise ValueError(f"Unknown initial state: {initial!r}")
        self._state: str = initial
        self._on_transition = on_transition

    @property
    def state(self) -> str:
        """Current state (read-only from outside the machine)."""
        return self._state

    def trigger(self, event: str) -> str:
        """
        Fire *event* from the current state.

        Returns the new state.
        Raises InvalidTransitionError if the transition is not defined.
        """
        key = (self._state, event)
        destination = _TRANSITION_TABLE.get(key)
        if destination is None:
            raise InvalidTransitionError(
                f"No transition defined for trigger={event!r} "
                f"from state={self._state!r}. "
                f"Valid triggers from this state: "
                f"{[t for (s, t) in _TRANSITION_TABLE if s == self._state]}"
            )
        previous = self._state
        self._state = destination
        if self._on_transition is not None:
            self._on_transition(previous, event, destination)
        return destination

    def can(self, event: str) -> bool:
        """Return True if *event* is a valid trigger from the current state."""
        return (self._state, event) in _TRANSITION_TABLE


# ─────────────────────────────────────────────
# Proposal workflow wrapper
# ─────────────────────────────────────────────


class ProposalWorkflow:
    """
    State machine for a single optimization proposal.

    All public trigger methods are explicit (`review`, `approve`, …) so
    IDEs and type checkers can resolve them without dynamic magic.
    """

    def __init__(
        self,
        proposal_id: str,
        initial_state: str,
        on_transition: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        self.proposal_id = proposal_id
        # on_transition(proposal_id, new_state) — kept for backward compat
        self._outer_callback = on_transition

        self._machine = StateMachine(
            initial=initial_state,
            on_transition=self._on_state_change,
        )

    # Public read-only access to current state
    @property
    def state(self) -> str:
        return self._machine.state

    def can(self, trigger: str) -> bool:
        return self._machine.can(trigger)

    # ── Named trigger methods (replaces dynamic injection) ────

    def review(self) -> str:
        return self._machine.trigger("review")

    def approve(self) -> str:
        return self._machine.trigger("approve")

    def start_test(self) -> str:
        return self._machine.trigger("start_test")

    def deploy(self) -> str:
        return self._machine.trigger("deploy")

    def monitor(self) -> str:
        return self._machine.trigger("monitor")

    def deploy_and_monitor(self) -> str:
        """
        Atomically transition testing → monitoring.

        FIX: mark_deployed() previously called workflow.deploy() then
        workflow.monitor() as two separate triggers. If anything raised
        between them the proposal would be stranded in 'deploying'.
        This single trigger is atomic.
        """
        return self._machine.trigger("deploy_and_monitor")

    def complete(self) -> str:
        return self._machine.trigger("complete")

    def reject(self) -> str:
        return self._machine.trigger("reject")

    def rollback(self) -> str:
        return self._machine.trigger("rollback")

    # ── Internal callback ─────────────────────

    def _on_state_change(self, previous: str, event: str, new_state: str) -> None:
        log.info(
            "proposal_state_changed",
            proposal_id=self.proposal_id,
            previous_state=previous,
            trigger=event,
            new_state=new_state,
        )
        if self._outer_callback is not None:
            self._outer_callback(self.proposal_id, new_state)


# ─────────────────────────────────────────────
# Approval service
# ─────────────────────────────────────────────


class ApprovalService:
    """
    Manages the lifecycle of all optimization proposals.

    Persists state transitions to the database and sends
    webhook/Slack notifications at each step.
    """

    def __init__(self, db_session_factory: Any, notifier: Any) -> None:
        self._session_factory = db_session_factory
        self._notifier = notifier
        self._workflows: Dict[str, ProposalWorkflow] = {}
        # In-memory store of proposal payloads, keyed by id. Lets the
        # Optimizations tab list/read proposals without a DB persistence layer.
        self._proposals: Dict[str, Dict[str, Any]] = {}

    # ── Public API ────────────────────────────

    async def create_proposal(
        self,
        database_id: str,
        title: str,
        proposal_type: str,
        original_sql: Optional[str] = None,
        optimized_sql: Optional[str] = None,
        ddl_statements: Optional[List[str]] = None,
        llm_rationale: Optional[str] = None,
        estimated_improvement_pct: Optional[float] = None,
        estimated_impact_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Create a new proposal and start the workflow."""
        proposal_id = str(uuid.uuid4())

        workflow = ProposalWorkflow(
            proposal_id=proposal_id,
            initial_state="proposed",
            on_transition=self._handle_transition,
        )
        self._workflows[proposal_id] = workflow

        proposal: Dict[str, Any] = {
            "id": proposal_id,
            "database_id": database_id,
            "title": title,
            "proposal_type": proposal_type,
            "state": "proposed",
            "original_sql": original_sql,
            "optimized_sql": optimized_sql,
            "ddl_statements": ddl_statements or [],
            "llm_rationale": llm_rationale,
            "estimated_improvement_pct": estimated_improvement_pct,
            "estimated_impact_score": estimated_impact_score,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        self._proposals[proposal_id] = proposal
        await self._persist_proposal(proposal)
        await self._audit(
            proposal_id, database_id, "proposal.created", "agent", proposal
        )
        await self._notifier.send_proposal_notification(proposal, event="created")

        log.info(
            "proposal_created",
            proposal_id=proposal_id,
            title=title,
            impact=estimated_impact_score,
        )
        return proposal

    async def review_proposal(
        self, proposal_id: str, reviewer: str, comment: str
    ) -> Dict[str, Any]:
        """Record that a DBA has reviewed (but not yet approved) the proposal."""
        workflow = await self._get_or_restore_workflow(proposal_id)
        workflow.review()

        update: Dict[str, Any] = {
            "state": workflow.state,
            "reviewed_by": reviewer,
            "review_comment": comment,
        }
        await self._update_proposal(proposal_id, update)
        await self._audit(
            proposal_id, None, "proposal.reviewed", reviewer, {"comment": comment}
        )
        return update

    async def approve_proposal(
        self, proposal_id: str, approver: str, comment: str
    ) -> Dict[str, Any]:
        """
        Approve a proposal.

        Destructive operations (DROP, ALTER) require the caller to have
        'admin' or 'dba' role — enforced by the API layer before this
        method is reached.
        """
        workflow = await self._get_or_restore_workflow(proposal_id)
        workflow.approve()

        update: Dict[str, Any] = {
            "state": workflow.state,
            "approved_by": approver,
            "approved_at": datetime.now(timezone.utc).isoformat(),
            "review_comment": comment,
        }
        await self._update_proposal(proposal_id, update)
        await self._audit(
            proposal_id, None, "proposal.approved", approver, {"comment": comment}
        )
        await self._notifier.send_proposal_notification(
            {"id": proposal_id}, event="approved"
        )
        return update

    async def reject_proposal(
        self, proposal_id: str, reviewer: str, reason: str
    ) -> Dict[str, Any]:
        """Reject a proposal from either 'proposed' or 'reviewed' state."""
        workflow = await self._get_or_restore_workflow(proposal_id)
        workflow.reject()

        update: Dict[str, Any] = {
            "state": workflow.state,
            "review_comment": reason,
        }
        await self._update_proposal(proposal_id, update)
        await self._audit(
            proposal_id, None, "proposal.rejected", reviewer, {"reason": reason}
        )
        return update

    async def start_testing(self, proposal_id: str) -> Dict[str, Any]:
        """Agent calls this when workload replay begins."""
        workflow = await self._get_or_restore_workflow(proposal_id)
        workflow.start_test()

        update: Dict[str, Any] = {"state": workflow.state}
        await self._update_proposal(proposal_id, update)
        await self._audit(proposal_id, None, "proposal.testing_started", "agent", {})
        return update

    async def mark_deployed(
        self, proposal_id: str, replay_summary: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Record that the optimization has been deployed to production.

        FIX: The original called workflow.deploy() then workflow.monitor()
        as two separate triggers. Between those two calls the state was
        transiently 'deploying', and any exception raised (DB write, network
        timeout) would leave the proposal permanently stuck there.

        Now uses the single atomic deploy_and_monitor() trigger which moves
        testing → monitoring in one step with no intermediate persisted state.
        """
        workflow = await self._get_or_restore_workflow(proposal_id)
        workflow.deploy_and_monitor()  # atomic: testing → monitoring

        update: Dict[str, Any] = {
            "state": workflow.state,
            "deployed_at": datetime.now(timezone.utc).isoformat(),
            "replay_summary": replay_summary,
        }
        await self._update_proposal(proposal_id, update)
        await self._audit(
            proposal_id, None, "proposal.deployed", "agent", replay_summary
        )
        await self._notifier.send_proposal_notification(
            {"id": proposal_id, "replay_summary": replay_summary}, event="deployed"
        )
        return update

    async def rollback(self, proposal_id: str, reason: str) -> Dict[str, Any]:
        """Automatic rollback triggered by post-deployment metric regression."""
        workflow = await self._get_or_restore_workflow(proposal_id)
        workflow.rollback()

        update: Dict[str, Any] = {
            "state": workflow.state,
            "rolled_back_at": datetime.now(timezone.utc).isoformat(),
            "rollback_reason": reason,
        }
        await self._update_proposal(proposal_id, update)
        await self._audit(
            proposal_id, None, "proposal.rolled_back", "agent", {"reason": reason}
        )
        await self._notifier.send_proposal_notification(
            {"id": proposal_id, "reason": reason}, event="rolled_back"
        )
        log.warning("proposal_rolled_back", proposal_id=proposal_id, reason=reason)
        return update

    async def complete(self, proposal_id: str) -> Dict[str, Any]:
        """Mark a monitored proposal as successfully completed."""
        workflow = await self._get_or_restore_workflow(proposal_id)
        workflow.complete()

        update: Dict[str, Any] = {"state": workflow.state}
        await self._update_proposal(proposal_id, update)
        await self._audit(proposal_id, None, "proposal.completed", "agent", {})
        return update

    # ── Private helpers ───────────────────────

    def _handle_transition(self, proposal_id: str, new_state: str) -> None:
        """Sync callback fired by ProposalWorkflow on every state change."""
        log.info("workflow_transition", proposal_id=proposal_id, new_state=new_state)

    async def _get_or_restore_workflow(self, proposal_id: str) -> ProposalWorkflow:
        """Return the in-memory workflow, restoring from DB if the agent restarted."""
        if proposal_id in self._workflows:
            return self._workflows[proposal_id]

        state = await self._load_state(proposal_id)
        workflow = ProposalWorkflow(
            proposal_id=proposal_id,
            initial_state=state,
            on_transition=self._handle_transition,
        )
        self._workflows[proposal_id] = workflow
        return workflow

    async def _persist_proposal(self, proposal: Dict[str, Any]) -> None:
        """Write a new proposal row to PostgreSQL via the session factory."""
        # TODO: implement with SQLAlchemy async session
        log.debug("persist_proposal", proposal_id=proposal["id"])

    async def _update_proposal(self, proposal_id: str, fields: Dict[str, Any]) -> None:
        """Update mutable fields on an existing proposal row."""
        # Keep the in-memory payload in sync so the Optimizations tab reflects
        # state transitions immediately. (Swap for a real DB write in production.)
        if proposal_id in self._proposals:
            self._proposals[proposal_id].update(fields)
        log.debug(
            "update_proposal",
            proposal_id=proposal_id,
            fields=list(fields.keys()),
        )

    def list_proposals(
        self, database_id: Optional[str] = None, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Return stored proposals (newest first), optionally filtered by DB."""
        items = list(self._proposals.values())
        if database_id:
            items = [p for p in items if p.get("database_id") == database_id]
        items.sort(key=lambda p: p.get("created_at", ""), reverse=True)
        return items[:limit]

    def get_proposal(self, proposal_id: str) -> Optional[Dict[str, Any]]:
        """Return a single stored proposal payload, or None if unknown."""
        return self._proposals.get(proposal_id)

    async def _load_state(self, proposal_id: str) -> str:
        """
        Load the persisted state for a proposal from the database.

        FIX: The original always returned "proposed", silently resetting
        any in-flight proposal to the beginning of the workflow after an
        agent restart. This stub is marked clearly so the implementer knows
        it must query the DB.

        TODO: replace with a real DB query, e.g.:
            async with self._session_factory() as session:
                row = await session.get(ProposalModel, proposal_id)
                if row is None:
                    raise KeyError(f"Proposal {proposal_id!r} not found")
                return row.state
        """
        if self._session_factory is None:
            # Degraded / dev mode: no persistence is wired, so we cannot restore
            # prior state. Fall back to 'proposed' (with a clear warning) instead
            # of hard-failing the request. Wire a real session factory in
            # production to get correct restart-safe behaviour.
            log.warning(
                "approval_load_state_no_persistence",
                proposal_id=proposal_id,
                hint="No db_session_factory configured; defaulting state to 'proposed'.",
            )
            return "proposed"

        raise NotImplementedError(
            f"_load_state must be implemented to restore proposal {proposal_id!r} "
            "from the database. The previous stub always returned 'proposed', "
            "which silently reset live proposals to the start of the workflow."
        )

    async def _audit(
        self,
        proposal_id: Optional[str],
        database_id: Optional[str],
        action: str,
        actor: str,
        details: Dict[str, Any],
    ) -> None:
        log.info(
            "audit",
            proposal_id=proposal_id,
            database_id=database_id,
            action=action,
            actor=actor,
        )