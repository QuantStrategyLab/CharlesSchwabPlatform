from __future__ import annotations

import json
import stat
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from application.execution_claim import claim_execution_marker
from quant_platform_kit.common.execution_state import ExecutionMarkerStore


def test_local_execution_claim_has_exactly_one_concurrent_winner() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        store = ExecutionMarkerStore(local_dir=tmpdir, cloud_prefix_uri=None)
        marker_key = "v1/schwab/live/strategy/live/2026-08-21/2026-08-24/t-plus-1"

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(
                executor.map(
                    lambda _: claim_execution_marker(
                        store,
                        marker_key,
                        metadata={"account_scope": "LIVE"},
                    ),
                    range(24),
                )
            )

        assert results.count(True) == 1
        assert results.count(False) == 23
        marker_path = store._local_path(marker_key)
        payload = json.loads(marker_path.read_text(encoding="utf-8"))
        assert payload["schema_version"] == "execution_claim.v1"
        assert payload["state"] == "claimed"
        assert stat.S_IMODE(Path(marker_path).stat().st_mode) == 0o600


def test_native_execution_claim_errors_fail_closed() -> None:
    class BrokenStore:
        def claim_marker(self, _marker_key, *, metadata):
            del metadata
            raise OSError("backend unavailable")

    try:
        claim_execution_marker(BrokenStore(), "marker")
    except RuntimeError as exc:
        assert "execution state claim failed" in str(exc)
    else:
        raise AssertionError("claim backend failure must abort before execution")


def test_gcs_execution_claim_uses_create_only_generation_precondition() -> None:
    observed = {}

    class FakeBlob:
        def upload_from_string(self, payload, *, content_type, if_generation_match):
            observed.update(
                payload=json.loads(payload),
                content_type=content_type,
                if_generation_match=if_generation_match,
            )

    class FakeBucket:
        def blob(self, object_name):
            observed["object_name"] = object_name
            return FakeBlob()

    class FakeClient:
        def bucket(self, bucket_name):
            observed["bucket_name"] = bucket_name
            return FakeBucket()

    store = ExecutionMarkerStore(
        local_dir=None,
        cloud_prefix_uri="gs://runtime-state/execution-reports",
        client_factory=FakeClient,
    )

    assert claim_execution_marker(store, "v1/schwab/live/strategy") is True
    assert observed["bucket_name"] == "runtime-state"
    assert observed["object_name"].endswith("v1/schwab/live/strategy.json")
    assert observed["content_type"] == "application/json"
    assert observed["if_generation_match"] == 0
    assert observed["payload"]["state"] == "claimed"
