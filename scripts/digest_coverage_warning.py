"""Coordinate one digest-coverage warning attempt within an Actions job."""
from __future__ import annotations

import hashlib
import json
import os
import re
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
    message = _coverage_message(locale)
    try:
        return send(message) is True
    except Exception:  # noqa: BLE001 -- The attempt remains recorded when delivery is uncertain.
        print("digest_coverage_warning_send_unconfirmed", file=sys.stderr)
        return False


def _coverage_message(locale):
    """Display only; preserve the existing attempted-send marker and callback."""
    from notifications.digest_snapshot.presentation import next_step_text, render_operator_message

    lang = "zh" if str(locale).lower().startswith("zh") else "en"
    zh = lang == "zh"
    link = None
    # Only the active Actions run is a valid source for this warning's run link.
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    if (os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_SERVER_URL") == "https://github.com"
            and re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository)
            and re.fullmatch(r"[0-9]{1,20}", run_id)):
        link = f"https://github.com/{repository}/actions/runs/{run_id}"
    return render_operator_message(
        title="⚠️ Schwab 日报候选确认" if zh else "⚠️ Schwab digest candidate confirmation",
        locale=lang,
        result=["日报候选记录未确认" if zh else "Daily digest candidate record is unconfirmed"],
        scope=["本次候选保存尝试；统计窗口未知" if zh else "This candidate persistence attempt; business window unknown"],
        evidence=["reason_code: candidate_publication_unconfirmed",
                  ("运行记录：" if zh else "Run record: ") + (link or ("未提供运行链接" if zh else "run link not provided"))],
        coverage=["日报可能缺少本次记录；订单：未知；成交：未知" if zh else
                  "The digest may omit this record; orders: unknown; fills: unknown"],
        next_step=[next_step_text(lang, needs_review=True)],
    )
