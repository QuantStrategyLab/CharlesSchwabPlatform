"""Project existing private reports; no service or broker invocation."""
from __future__ import annotations
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import json
import os
from notifications.unified import emit_completed_candidates  # noqa: E402
from scripts.digest_coverage_warning import warn_coverage_unconfirmed  # noqa: E402


def main():
    try:
        print(json.dumps(emit_completed_candidates(os.environ), sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({"status": "blocked", "uploaded": False, "reason": "candidate_projection_failed"}))
        from scripts.execution_report_heartbeat import _notification_locale, _send_telegram
        warn_coverage_unconfirmed(_send_telegram, _notification_locale())
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
