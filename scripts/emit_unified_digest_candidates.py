"""Project existing private reports; no service or broker invocation."""
from __future__ import annotations
from contextlib import redirect_stderr, redirect_stdout
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import json
import os
from notifications.unified import CandidateConflictError, CandidateStorageError, emit_completed_candidates  # noqa: E402
from scripts.digest_coverage_warning import warn_coverage_unconfirmed  # noqa: E402


# Match whole local codes only; provider text and object locators stay private.
_ERROR_STAGES = {
    **dict.fromkeys((
        "candidate_bindings_invalid", "candidate_target_enablement_unknown",
        "candidate_store_identity_missing", "candidate_identity_missing",
        "candidate_store_configuration_missing", "notification_store_configuration_missing",
    ), "binding_validation"),
    **dict.fromkeys((
        "candidate_archive_read_failed", "candidate_archive_budget_exceeded",
        "candidate_source_locator_mismatch", "candidate_archive_time_missing",
        "candidate_archive_uri_invalid", "candidate_archive_operation_invalid",
        "candidate_archive_object_too_large",
    ), "archive_read"),
    "candidate_source_absent": "source_selection",
    "candidate_due_source_absent": "schedule_evaluation",
    **dict.fromkeys((
        "candidate_identity_or_time_missing", "candidate_target_invalid",
        "candidate_source_identity_mismatch", "candidate_source_invalid",
        "candidate_source_time_invalid", "candidate_source_incomplete",
        "candidate_run_id_missing", "candidate_duplicate_run",
        "candidate_non_execution_conflict", "candidate_plugin_observations_invalid",
        "notification_source_run_conflict", "notification_observed_run_not_covered",
        "notification_time_timezone_missing",
    ), "projection"),
    **dict.fromkeys((
        "notification_candidate_invalid", "notification_candidate_identity_unknown",
        "notification_candidate_budget_exceeded", "notification_candidate_history_unknown",
        "notification_candidate_scope_conflict", "notification_candidate_source_conflict",
        "notification_event_identity_unknown", "notification_event_conflict",
    ), "candidate_merge"),
}
_STORAGE_CODES = frozenset({
    "permission_denied", "generation_conflict", "timeout_unknown",
    "json_invalid", "serialization_invalid", "storage_unknown",
})


def _failure_diagnostic(error, stage):
    if isinstance(error, CandidateConflictError):
        return {"stage": "candidate_merge", "error_code": "notification_candidate_source_conflict",
                "conflict": error.diagnostic()}
    if isinstance(error, CandidateStorageError):
        stage = error.stage if error.stage in ("candidate_read", "candidate_write") else stage
        code = error.category
        code = code if type(code) is str and code in _STORAGE_CODES else "storage_unknown"
        return {"stage": stage, "error_code": code}
    code = error.args[0] if len(error.args) == 1 and type(error.args[0]) is str else None
    if isinstance(error, (ValueError, RuntimeError)) and code in _ERROR_STAGES:
        return {"stage": _ERROR_STAGES[code], "error_code": code}
    if isinstance(error, json.JSONDecodeError):
        code = "json_invalid"
    elif stage == "output_serialization" and isinstance(error, (TypeError, ValueError)):
        code = "serialization_invalid"
    else:
        code = "unexpected_error"
    return {"stage": stage, "error_code": code}


def main():
    stage = "projection"
    try:
        result = emit_completed_candidates(os.environ)
        stage = "output_serialization"
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception as error:
        print(json.dumps({"status": "blocked", "uploaded": False, "reason": "candidate_projection_failed",
                          **_failure_diagnostic(error, stage)}))
        from scripts.execution_report_heartbeat import _notification_locale, _send_telegram
        # The legacy sender can print provider details. Keep its attempt and
        # marker semantics without exposing those details or masking the cause.
        alert_failed = False
        with open(os.devnull, "w") as sink, redirect_stdout(sink), redirect_stderr(sink):
            try:
                warn_coverage_unconfirmed(_send_telegram, _notification_locale())
            except Exception:
                alert_failed = True
        if alert_failed:
            print(json.dumps({"stage": "coverage_alert", "error_code": "alert_delivery_unconfirmed"}),
                  file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
