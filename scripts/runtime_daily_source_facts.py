"""Bounded read-only service/scheduler metadata for the fixed daily caller.

Uses the workflow's existing ADC identity. No report collection, secret-manager
access, scheduler invocation, service request, notification or publication occurs.
Returned facts are private in-memory inputs; only fixed reason codes may be logged.
"""

from __future__ import annotations

import datetime as dt
import json
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from scripts.publish_account_facts_from_reports import PROJECT_ID, REGION, _REVISION
from scripts.publish_runtime_daily_from_reports import TARGET, matured_schedule
from scripts.runtime_heartbeat_policy import cron_matches

MAX_METADATA_BYTES = 64 * 1024
SERVICE_FIELDS = (
    "name,generation,observedGeneration,reconciling,terminalCondition(state),"
    "trafficStatuses(revision,percent,tag),uri"
)
SCHEDULER_FIELDS = "name,state,schedule,timeZone,httpTarget(uri,httpMethod)"
SAFE_REASONS = frozenset(
    {
        "verified",
        "source_configuration_invalid",
        "service_unavailable",
        "service_traffic_unconfirmed",
        "scheduler_unavailable",
        "scheduler_ambiguous",
        "scheduler_paused",
        "scheduler_disabled",
        "scheduler_target_mismatch",
        "scheduler_unevaluable",
    }
)


@dataclass(frozen=True, repr=False)
class SourceFacts:
    reason: str
    runtime_revision: str | None = None
    scheduler_cron: str | None = None
    scheduler_timezone: str | None = None


def _authorized_session() -> Any:
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    credentials, _project = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return AuthorizedSession(credentials, max_refresh_attempts=0, refresh_timeout=15)


def _get_object(session: Any, url: str, fields: str) -> dict[str, Any] | None:
    deadline = time.monotonic() + 15
    with session.get(
        url,
        params={"fields": fields},
        timeout=(5, 15),
        stream=True,
        allow_redirects=False,
    ) as response:
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise ValueError("metadata_unavailable")
        content = bytearray()
        for chunk in response.iter_content(chunk_size=8192):
            if time.monotonic() > deadline or not isinstance(chunk, bytes):
                raise ValueError("metadata_unavailable")
            if len(content) + len(chunk) > MAX_METADATA_BYTES:
                raise ValueError("metadata_unavailable")
            content.extend(chunk)
        value = json.loads(content)
        if not isinstance(value, dict):
            raise ValueError("metadata_unavailable")
        return value


def _serving_revision(service: Mapping[str, Any], resource: str) -> tuple[str, str]:
    if (
        service.get("name") != resource
        or service.get("reconciling", False) is not False
        or not isinstance(service.get("generation"), str)
        or not service["generation"].isdigit()
        or service["generation"] != service.get("observedGeneration")
        or service.get("terminalCondition", {}).get("state") != "CONDITION_SUCCEEDED"
    ):
        raise ValueError("service_unconfirmed")
    traffic = service.get("trafficStatuses")
    if not isinstance(traffic, list) or not traffic:
        raise ValueError("service_unconfirmed")
    serving: dict[str, int] = {}
    total = 0
    for item in traffic:
        if not isinstance(item, dict):
            raise ValueError("service_unconfirmed")
        revision = item.get("revision")
        if isinstance(revision, str) and revision.startswith(resource + "/revisions/"):
            revision = revision.removeprefix(resource + "/revisions/")
        if not isinstance(revision, str) or _REVISION.fullmatch(revision) is None:
            raise ValueError("service_unconfirmed")
        percent = item.get("percent")
        if percent is None and "percent" not in item:
            tag = item.get("tag")
            if (
                not isinstance(tag, str)
                or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", tag) is None
            ):
                raise ValueError("service_unconfirmed")
            percent = 0
        if type(percent) is not int or not 0 <= percent <= 100:
            raise ValueError("service_unconfirmed")
        total += percent
        if percent:
            serving[revision] = serving.get(revision, 0) + percent
    if total != 100 or len(serving) != 1:
        raise ValueError("service_unconfirmed")
    uri = service.get("uri")
    parsed = urlsplit(uri) if isinstance(uri, str) else None
    if (
        parsed is None
        or parsed.scheme != "https"
        or not parsed.hostname
        or not parsed.hostname.endswith(".run.app")
        or parsed.port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("service_unconfirmed")
    return next(iter(serving)), uri


def read_source_facts(
    environ: Mapping[str, str], *, session: Any = None
) -> SourceFacts:
    """GET one fixed service and at most two established scheduler aliases."""
    location = environ.get("RUNTIME_HEARTBEAT_SCHEDULER_LOCATION") or REGION
    if (
        environ.get("GCP_PROJECT_ID") != PROJECT_ID
        or environ.get("GCP_REGION") != REGION
        or not isinstance(location, str)
        or re.fullmatch(r"[a-z][a-z0-9-]{0,62}", location) is None
    ):
        return SourceFacts("source_configuration_invalid")
    resource = f"projects/{PROJECT_ID}/locations/{REGION}/services/{TARGET['service']}"
    owned = session is None
    try:
        if session is None:
            session = _authorized_session()
        try:
            service = _get_object(
                session, "https://run.googleapis.com/v2/" + resource, SERVICE_FIELDS
            )
            if service is None:
                return SourceFacts("service_unavailable")
            revision, uri = _serving_revision(service, resource)
        except Exception:
            return SourceFacts("service_traffic_unconfirmed")
        service_name = TARGET["service"]
        names = [
            service_name + "-scheduler",
            service_name.removesuffix("-service") + "-scheduler",
        ]
        jobs = []
        try:
            for name in dict.fromkeys(names):
                job_resource = f"projects/{PROJECT_ID}/locations/{location}/jobs/{name}"
                job = _get_object(
                    session,
                    "https://cloudscheduler.googleapis.com/v1/" + job_resource,
                    SCHEDULER_FIELDS,
                )
                if job is not None:
                    if job.get("name") != job_resource:
                        return SourceFacts("scheduler_target_mismatch", revision)
                    jobs.append(job)
        except Exception:
            return SourceFacts("scheduler_unavailable", revision)
        if len(jobs) != 1:
            return SourceFacts(
                "scheduler_ambiguous" if jobs else "scheduler_unavailable", revision
            )
        job = jobs[0]
        if job.get("state") != "ENABLED":
            reason = {
                "PAUSED": "scheduler_paused",
                "DISABLED": "scheduler_disabled",
            }.get(job.get("state"), "scheduler_unevaluable")
            return SourceFacts(reason, revision)
        target = job.get("httpTarget")
        if (
            not isinstance(target, dict)
            or target.get("uri") != uri + "/run"
            or target.get("httpMethod") != "POST"
        ):
            return SourceFacts("scheduler_target_mismatch", revision)
        try:
            cron = job["schedule"]
            zone = job["timeZone"]
            if (
                not isinstance(cron, str)
                or len(cron.split()) != 5
                or not isinstance(zone, str)
            ):
                raise ValueError
            ZoneInfo(zone)
            cron_matches(cron, dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc))
        except Exception:
            return SourceFacts("scheduler_unevaluable", revision)
        return SourceFacts("verified", revision, " ".join(cron.split()), zone)
    except Exception:
        return SourceFacts("service_unavailable")
    finally:
        if owned and session is not None:
            try:
                session.close()
            except Exception:
                pass


def effective_schedule(
    policy: Mapping[str, Any],
    *,
    facts: SourceFacts,
    **options: Any,
) -> dict[str, Any] | None:
    """Never repair missing effective facts with a declared schedule or TTL."""
    if (
        facts.reason != "verified"
        or not facts.scheduler_cron
        or not facts.scheduler_timezone
    ):
        return None
    effective = dict(policy)
    effective["scheduler"] = {
        "main_time": facts.scheduler_cron,
        "timezone": facts.scheduler_timezone,
    }
    return matured_schedule(effective, **options)
