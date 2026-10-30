"""Offline observation coverage for missing account-risk inputs."""

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest

from application.account_observation import build_account_observation


class AccountRiskFactReadinessTest(unittest.TestCase):
    def test_reported_cash_does_not_claim_settled_or_complete_account(self):
        snapshot = SimpleNamespace(as_of=datetime(2026, 1, 1, tzinfo=timezone.utc), positions=(), metadata={
            "account_hash": "synthetic",
            "broker_cash_balance": "100",
            "broker_cash_balance_source": "cashBalance",
        })
        observation = build_account_observation(snapshot)
        receipt = observation["account_risk_fact_readiness"]
        self.assertEqual(observation["cash_balance"], "100")
        self.assertEqual(receipt["settled_cash"], "NOT_PROJECTED_IN_OBSERVATION")
        self.assertEqual(receipt["full_account_positions"], "UNVERIFIED")
        self.assertEqual(receipt["order_observation_watermark"], "MISSING")
        self.assertEqual(receipt["status"], "NOT_READY")
        self.assertEqual(receipt["snapshot_observed_at"], observation["observed_at"])


if __name__ == "__main__":
    unittest.main()
