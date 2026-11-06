"""Evidence export remains independent of failed heartbeat/candidate checks."""
from test_runtime_daily_heartbeat_workflow import (
    _condition, _evaluate, _step_block, _workflow_text,
)


def test_evidence_export_after_attention_uses_authenticated_installed_reader():
    step = _step_block(_workflow_text(), "Publish read-only runtime execution evidence")
    condition = _condition(step)
    # A status function overrides implicit success(), including failed candidate publication.
    assert "!cancelled()" in condition
    assert _evaluate(condition, "", "success", "skipped")
    assert _evaluate(condition, "", "failure", "success")
    for primary, retry in (("failure", "failure"), ("failure", "skipped")):
        assert not _evaluate(condition, "", primary, retry)
    for installed in ("failure", "skipped", "cancelled"):
        assert not _evaluate(condition, "", "success", "skipped", installed=installed)
    assert not _evaluate(condition, "", "success", "skipped", cancelled=True)
    assert "actions/publish-runtime-execution-evidence@" in step
    assert "EXECUTION_EVIDENCE_SYNC_TOKEN:" in step
