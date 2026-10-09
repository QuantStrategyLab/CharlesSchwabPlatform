"""N13: Schwab adapter around QPK execution_kernel T2/T3 (halted resubmit)."""

from __future__ import annotations

import unittest

from application.execution_kernel_adapter import (
    evaluate_halted_resubmit_guards,
    should_block_halted_resubmit,
)


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


if __name__ == "__main__":
    unittest.main()
