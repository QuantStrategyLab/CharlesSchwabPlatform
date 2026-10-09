"""N13: Schwab adapter around QPK execution_kernel T1–T3 (ADR-B T1 flag)."""

from __future__ import annotations

import unittest

from application.execution_kernel_adapter import (
    consult_t1_live_submit,
    evaluate_halted_resubmit_guards,
    should_block_halted_resubmit,
    should_block_live_submit_for_t1,
)
from application.runtime_dependencies import SchwabRebalanceConfig


def _minimal_config(**kwargs) -> SchwabRebalanceConfig:
    defaults = dict(
        translator=lambda key, **_kw: key,
        strategy_display_name="test",
        limit_buy_premium=0.0,
        sell_settle_delay_sec=0.0,
    )
    defaults.update(kwargs)
    return SchwabRebalanceConfig(**defaults)


class ExecutionKernelAdapterTests(unittest.TestCase):
    def test_unknown_halt_denies_t2_and_t3(self):
        t2, t3 = evaluate_halted_resubmit_guards(submission_outcome_unknown=True)
        self.assertEqual(t2.decision, "deny")
        self.assertEqual(t2.reason_code, "submission_uncertain")
        self.assertEqual(t3.decision, "deny")
        self.assertEqual(t3.reason_code, "reconcile_required")
        self.assertTrue(should_block_halted_resubmit(submission_outcome_unknown=True))

    def test_terminal_halt_allows_kernel_but_caller_still_halts(self):
        """When halted for a confirmed terminal reason, T2/T3 allow; service still returns False."""
        t2, t3 = evaluate_halted_resubmit_guards(submission_outcome_unknown=False)
        self.assertEqual(t2.decision, "allow")
        self.assertEqual(t3.decision, "allow")
        self.assertFalse(should_block_halted_resubmit(submission_outcome_unknown=False))

    def test_t1_live_without_identity_denies(self):
        decision = consult_t1_live_submit(identity_held=False)
        self.assertEqual(decision.decision, "deny")
        self.assertEqual(decision.reason_code, "missing_durable_identity")
        self.assertFalse(decision.allowed)

    def test_t1_live_with_identity_allows(self):
        decision = consult_t1_live_submit(identity_held=True)
        self.assertEqual(decision.decision, "allow")
        self.assertTrue(decision.allowed)

    def test_t1_dry_run_bypass_allows_without_identity(self):
        decision = consult_t1_live_submit(
            identity_held=False,
            dry_run_bypass=True,
        )
        self.assertEqual(decision.decision, "allow")
        self.assertTrue(decision.allowed)

    def test_t1_dry_run_mode_allows_without_identity(self):
        decision = consult_t1_live_submit(
            identity_held=False,
            dry_run_only=True,
        )
        self.assertEqual(decision.decision, "allow")
        self.assertTrue(decision.allowed)

    def test_flag_off_never_blocks_even_when_t1_would_deny(self):
        """Default OFF: consult may deny but should_block stays False (no behavior change)."""
        self.assertFalse(
            should_block_live_submit_for_t1(
                enforce=False,
                execution_dedup_enabled=True,
                identity_held=False,
            )
        )
        config = _minimal_config(execution_kernel_t1_enforce=False)
        self.assertFalse(config.execution_kernel_t1_enforce)

    def test_flag_on_dedup_on_no_identity_blocks(self):
        self.assertTrue(
            should_block_live_submit_for_t1(
                enforce=True,
                execution_dedup_enabled=True,
                identity_held=False,
            )
        )

    def test_flag_on_dedup_on_with_identity_allows(self):
        self.assertFalse(
            should_block_live_submit_for_t1(
                enforce=True,
                execution_dedup_enabled=True,
                identity_held=True,
            )
        )

    def test_flag_on_dedup_off_never_blocks_and_does_not_forge_identity(self):
        """ADR-B: dedup-off is N/A for enforce; identity_held stays False in consult."""
        decision = consult_t1_live_submit(identity_held=False)  # honest snapshot
        self.assertEqual(decision.decision, "deny")  # consult still sees missing identity
        self.assertFalse(
            should_block_live_submit_for_t1(
                enforce=True,
                execution_dedup_enabled=False,
                identity_held=False,  # must not forge True
            )
        )

    def test_config_default_t1_enforce_is_false(self):
        config = _minimal_config()
        self.assertFalse(config.execution_kernel_t1_enforce)
        self.assertFalse(config.execution_dedup_enabled)


class ExecutionKernelT1EnforceEnvTests(unittest.TestCase):
    def test_env_flag_enables_when_config_false(self):
        from application import rebalance_service

        config = _minimal_config(execution_kernel_t1_enforce=False)
        old = rebalance_service.os.environ.get(
            rebalance_service.EXECUTION_KERNEL_T1_ENFORCE_ENV
        )
        try:
            rebalance_service.os.environ[
                rebalance_service.EXECUTION_KERNEL_T1_ENFORCE_ENV
            ] = "true"
            self.assertTrue(rebalance_service._execution_kernel_t1_enforce_enabled(config))
        finally:
            if old is None:
                rebalance_service.os.environ.pop(
                    rebalance_service.EXECUTION_KERNEL_T1_ENFORCE_ENV, None
                )
            else:
                rebalance_service.os.environ[
                    rebalance_service.EXECUTION_KERNEL_T1_ENFORCE_ENV
                ] = old

    def test_config_true_enables_without_env(self):
        from application import rebalance_service

        config = _minimal_config(execution_kernel_t1_enforce=True)
        old = rebalance_service.os.environ.pop(
            rebalance_service.EXECUTION_KERNEL_T1_ENFORCE_ENV, None
        )
        try:
            self.assertTrue(rebalance_service._execution_kernel_t1_enforce_enabled(config))
        finally:
            if old is not None:
                rebalance_service.os.environ[
                    rebalance_service.EXECUTION_KERNEL_T1_ENFORCE_ENV
                ] = old


if __name__ == "__main__":
    unittest.main()
