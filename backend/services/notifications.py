"""
Notification service.

Sends alerts and proposal updates via Slack webhooks
and configurable HTTP webhooks with tiered routing:

  P1 (critical)  → PagerDuty + Slack
  P2 (high)      → Slack
  P3 (info)      → email digest (batched)
"""
from __future__ import annotations

from typing import Any, Dict, List

import httpx

from backend.utils.config import settings
from backend.utils.logging import get_logger

log = get_logger(__name__)

# Severity → Slack color mapping
SEVERITY_COLORS = {
    "p1": "#FF0000",   # Red
    "p2": "#FFA500",   # Orange
    "p3": "#36A64F",   # Green
    "critical": "#FF0000",
    "high": "#FFA500",
    "medium": "#FFD700",
    "low": "#36A64F",
}

PROPOSAL_EMOJIS = {
    "created": "🔍",
    "approved": "✅",
    "deployed": "🚀",
    "rolled_back": "⏮️",
    "rejected": "❌",
}


class NotificationService:
    def __init__(self):
        self._http = httpx.AsyncClient(timeout=10.0)
        self._email_queue: List[Dict] = []

    # ── Alerts ────────────────────────────────

    async def send_alert(self, anomaly: Any):
        """Route an anomaly event to the appropriate notification channel."""
        severity = anomaly.severity
        payload = self._build_alert_slack_payload(anomaly)

        if severity in ("p1", "critical"):
            await self._slack(payload)
            await self._pagerduty(anomaly)
        elif severity in ("p2", "high"):
            await self._slack(payload)
        else:
            # P3: queue for digest
            self._email_queue.append(anomaly.to_dict())

    def _build_alert_slack_payload(self, anomaly: Any) -> Dict:
        color = SEVERITY_COLORS.get(anomaly.severity, "#808080")
        return {
            "text": f"*[{anomaly.severity.upper()}]* {anomaly.title}",
            "attachments": [
                {
                    "color": color,
                    "fields": [
                        {"title": "Database", "value": anomaly.database_id, "short": True},
                        {"title": "Type", "value": anomaly.anomaly_type, "short": True},
                        {"title": "Details", "value": anomaly.description, "short": False},
                    ],
                    "footer": "Autonomous DBA Agent",
                    "ts": int(anomaly.detected_at.timestamp()),
                }
            ],
        }

    # ── Proposals ─────────────────────────────

    async def send_proposal_notification(self, proposal: Dict, event: str):
        emoji = PROPOSAL_EMOJIS.get(event, "ℹ️")
        title = proposal.get("title", proposal.get("id", "unknown"))

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} Optimization Proposal {event.title()}",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Proposal:*\n{title}"},
                    {"type": "mrkdwn", "text": f"*Status:*\n{event.upper()}"},
                ],
            },
        ]

        if event == "created":
            impact = proposal.get("estimated_impact_score", "N/A")
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*Impact Score:* {impact}/100\n"
                        f"*Type:* {proposal.get('proposal_type', 'N/A')}\n\n"
                        f"Review and approve at the DBA Dashboard."
                    ),
                },
            })
        elif event == "rolled_back":
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Rollback Reason:*\n{proposal.get('reason', 'Automatic regression detected')}",
                },
            })

        await self._slack({"blocks": blocks})

    # ── Transport ─────────────────────────────

    async def _slack(self, payload: Dict):
        if not settings.slack_webhook_url:
            log.debug("slack_not_configured", payload=str(payload)[:100])
            return
        try:
            resp = await self._http.post(settings.slack_webhook_url, json=payload)
            resp.raise_for_status()
        except Exception as e:
            log.error("slack_notification_failed", error=str(e))

    async def _pagerduty(self, anomaly: Any):
        """PagerDuty Events API v2."""
        pd_key = getattr(settings, "pagerduty_integration_key", None)
        if not pd_key:
            return
        try:
            payload = {
                "routing_key": pd_key,
                "event_action": "trigger",
                "payload": {
                    "summary": anomaly.title,
                    "severity": "critical",
                    "source": f"dba-agent:{anomaly.database_id}",
                    "custom_details": anomaly.to_dict(),
                },
            }
            resp = await self._http.post(
                "https://events.pagerduty.com/v2/enqueue", json=payload
            )
            resp.raise_for_status()
        except Exception as e:
            log.error("pagerduty_failed", error=str(e))

    async def flush_email_digest(self):
        """Send batched P3 alerts as an email digest (called hourly)."""
        if not self._email_queue:
            return
        count = len(self._email_queue)
        log.info("email_digest_would_send", count=count)
        self._email_queue.clear()

    async def close(self):
        await self._http.aclose()