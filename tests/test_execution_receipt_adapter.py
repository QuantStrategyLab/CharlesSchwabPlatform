from __future__ import annotations

from types import SimpleNamespace
import unittest

from application.execution_receipt_adapter import attach_cycle_execution_receipt


REVISION = "a" * 40


def _report() -> dict[str, object]:
    return {
        "platform": "charles_schwab",
        "strategy_profile": "soxl_soxx_trend_income",
        "dry_run": False,
        "runtime_target": {"execution_mode": "live"},
        "runtime_release_receipt": {
            "attestation_state": "self_attested",
            "strategy_release": {"strategy_revision": REVISION},
        },
    }


class ExecutionReceiptAdapterTest(unittest.TestCase):
    def test_accepted_order_remains_pending_reconciliation(self) -> None:
        report = _report()

        attach_cycle_execution_receipt(
            report,
            SimpleNamespace(
                execution={
                    "execution_status": "pending_reconciliation",
                    "broker_submission_done": True,
                    "orders_pending_count": 1,
                },
                submitted_orders=({"status": "accepted"},),
            ),
        )

        self.assertEqual(report["execution_receipt"]["outcome"], "reconciliation_required")
        self.assertEqual(
            report["execution_receipt"]["broker_confirmation"],
            "reconciliation_required",
        )

    def test_unconfirmed_submission_is_not_a_fill(self) -> None:
        report = _report()

        attach_cycle_execution_receipt(
            report,
            SimpleNamespace(execution={"broker_submission_done": True}, submitted_orders=({"status": "accepted"},)),
        )

        self.assertEqual(report["execution_receipt"]["outcome"], "submitted")
        self.assertEqual(report["execution_receipt"]["broker_confirmation"], "not_observed")

    def test_dry_run_never_claims_submission(self) -> None:
        report = _report()
        report["dry_run"] = True

        attach_cycle_execution_receipt(
            report,
            SimpleNamespace(execution={"broker_submission_done": True}, submitted_orders=({"status": "dry_run"},)),
        )

        self.assertEqual(report["execution_receipt"]["outcome"], "no_action")
