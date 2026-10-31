"""Coordinate one digest-coverage warning attempt within an Actions job."""
from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile


def warn_coverage_unconfirmed(send, locale):
    """Record an attempt before sending; never claim or infer delivery.

    An unavailable marker retains the original alert with a fixed diagnostic;
    only an existing, completed attempt marker suppresses a duplicate.
    """
    path = os.environ.get("DIGEST_COVERAGE_ALERT_ATTEMPT_PATH")
    if path:
        target = json.dumps([os.environ.get(key, "") for key in (
            "DIGEST_TARGET_BINDINGS_JSON", "DIGEST_TARGET_ID", "DIGEST_OPAQUE_ACCOUNT_UID",
            "DIGEST_STORE_TARGET", "RUNTIME_TARGET_JSON",
        )], separators=(",", ":"))
        scope = hashlib.sha256(target.encode("utf-8")).hexdigest()
        path = f"{path}-{scope}"
        temporary = None
        try:
            fd, temporary = tempfile.mkstemp(prefix=".digest-attempt-", dir=os.path.dirname(path) or ".")
            with os.fdopen(fd, "w", encoding="utf-8") as marker:
                marker.write("attempted\n")
            # Link publishes the complete marker atomically and never replaces a winner.
            os.link(temporary, path)
        except FileExistsError:
            try:
                with open(path, encoding="utf-8") as marker:
                    if marker.read(32) == "attempted\n":
                        return False
            except (OSError, UnicodeError):
                pass
            print("digest_coverage_warning_marker_unavailable", file=sys.stderr)
        except OSError:
            print("digest_coverage_warning_marker_unavailable", file=sys.stderr)
        finally:
            if temporary:
                try:
                    os.unlink(temporary)
                except OSError:
                    pass
    elif os.environ.get("GITHUB_ACTIONS") == "true":
        print("digest_coverage_warning_marker_unavailable", file=sys.stderr)
    message = ("⚠️ Schwab 日报候选记录未确认，日报可能缺少本次记录；请检查运行记录和摘要存储。"
               if str(locale).lower().startswith("zh") else
               "⚠️ Schwab daily digest candidate record is unconfirmed; the digest may omit this record. "
               "Check the run and digest storage.")
    try:
        return send(message) is True
    except Exception:  # noqa: BLE001 -- The attempt remains recorded when delivery is uncertain.
        print("digest_coverage_warning_send_unconfirmed", file=sys.stderr)
        return False
