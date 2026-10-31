from types import SimpleNamespace
from application.execution_evidence import extract_execution_evidence


def test_accepted_order_is_observed_without_claiming_fills():
    order = {"symbol": "TQQQ", "side": "buy", "status": "accepted", "broker_order_id": "synthetic"}
    result = SimpleNamespace(execution={"broker_submission_done": True}, submitted_orders=(order,))
    assert extract_execution_evidence(result, dry_run=False) == ([order], None)


def test_unknown_unidentified_duplicate_or_dry_run_order_not_claimed_complete():
    known = {"status": "accepted", "broker_order_id": "synthetic"}
    for result, dry in ((SimpleNamespace(execution={"broker_submission_done": False}, submitted_orders=(known,)), False),
                        (SimpleNamespace(execution={"broker_submission_done": True}, submitted_orders=({"status": "accepted"},)), False),
                        (SimpleNamespace(execution={"broker_submission_done": True}, submitted_orders=(known, known)), False),
                        (SimpleNamespace(execution={"broker_submission_done": True}, submitted_orders=(known,)), True),
                        (SimpleNamespace(execution={}, submitted_orders=()), False)):
        assert extract_execution_evidence(result, dry_run=dry) == (None, None)


def test_order_issue_leaves_cycle_order_coverage_unknown():
    order = {"status": "accepted", "broker_order_id": "synthetic"}
    result = SimpleNamespace(execution={"broker_submission_done": True, "notification_order_issue": True}, submitted_orders=(order,))
    assert extract_execution_evidence(result, dry_run=False) == (None, None)
