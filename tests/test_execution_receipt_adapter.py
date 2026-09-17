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

    def test_strategy_risk_rejection_is_persisted_as_risk_blocked(self) -> None:
        report = _report()

        attach_cycle_execution_receipt(
            report,
            SimpleNamespace(
                execution={
                    "execution_status": "blocked",
                    "no_op_reason": "rejected:too_many_positions",
                },
                submitted_orders=(),
            ),
        )

        self.assertEqual(report["execution_receipt"]["outcome"], "risk_blocked")
        self.assertEqual(report["execution_receipt"]["broker_confirmation"], "not_applicable")

    def test_explicit_no_signal_reason_is_no_signal(self) -> None:
        report = _report()

        attach_cycle_execution_receipt(
            report,
            SimpleNamespace(execution={"no_op_reason": "no_signal"}, submitted_orders=()),
        )

        self.assertEqual(report["execution_receipt"]["outcome"], "no_signal")
        self.assertEqual(report["execution_receipt"]["broker_confirmation"], "not_applicable")

    def test_explicit_no_rebalance_reason_is_no_rebalance(self) -> None:
        report = _report()

        attach_cycle_execution_receipt(
            report,
            SimpleNamespace(
                execution={
                    "execution_status": "no_op",
                    "no_op_reason": "target_diff_below_threshold",
                },
                submitted_orders=(),
            ),
        )

        self.assertEqual(report["execution_receipt"]["outcome"], "no_rebalance")
        self.assertEqual(report["execution_receipt"]["broker_confirmation"], "not_applicable")

    def test_ambiguous_no_op_reason_stays_no_action(self) -> None:
        report = _report()

        attach_cycle_execution_receipt(
            report,
            SimpleNamespace(
                execution={"execution_status": "no_op", "no_op_reason": "market_closed"},
                submitted_orders=(),
            ),
        )

        self.assertEqual(report["execution_receipt"]["outcome"], "no_action")

    def test_dry_run_keeps_no_action_even_with_explicit_reason(self) -> None:
        report = _report()
        report["dry_run"] = True

        attach_cycle_execution_receipt(
            report,
            SimpleNamespace(
                execution={"no_op_reason": "no_signal"},
                submitted_orders=(),
            ),
        )

        self.assertEqual(report["execution_receipt"]["outcome"], "no_action")
