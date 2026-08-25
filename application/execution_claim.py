"""Atomic execution claims for Schwab rebalance deduplication.

The claim is intentionally durable and has no automatic expiry.  If a worker
dies after claiming but before it can persist a terminal marker, later runs
must stop for reconciliation instead of risking a duplicate broker order.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class ExecutionClaimUnavailableError(RuntimeError):
    """Raised when an execution claim cannot be acquired safely."""


def _claim_payload(marker_key: str, metadata: Mapping[str, Any] | None) -> str:
    return json.dumps(
        {
            "schema_version": "execution_claim.v1",
            "marker_key": str(marker_key),
            "claimed_at": datetime.now(timezone.utc).isoformat(),
            "state": "claimed",
            "metadata": dict(metadata or {}),
        },
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )


def _build_gcs_client(store):
    client_factory = getattr(store, "client_factory", None)
    project_id = getattr(store, "project_id", None)
    if client_factory is not None:
        try:
            return client_factory(project=project_id) if project_id else client_factory()
        except TypeError:
            return client_factory()

    from google.cloud import storage

    return storage.Client(project=project_id) if project_id else storage.Client()


def _claim_gcs_marker(store, marker_key: str, payload: str) -> bool:
    cloud_uri_builder = getattr(store, "_cloud_uri", None)
    if not callable(cloud_uri_builder):
        raise ExecutionClaimUnavailableError(
            "execution state store cannot resolve its cloud marker URI"
        )
    uri = str(cloud_uri_builder(marker_key) or "").strip()
    if not uri.startswith("gs://"):
        raise ExecutionClaimUnavailableError(
            "Schwab atomic execution claims require a gs:// state URI"
        )
    bucket_name, separator, object_name = uri[5:].partition("/")
    if not separator or not bucket_name or not object_name:
        raise ExecutionClaimUnavailableError("invalid GCS execution claim URI")

    try:
        from google.api_core.exceptions import Conflict, PreconditionFailed

        blob = _build_gcs_client(store).bucket(bucket_name).blob(object_name)
        blob.upload_from_string(
            payload,
            content_type="application/json",
            if_generation_match=0,
        )
        return True
    except (Conflict, PreconditionFailed):
        return False
    except ExecutionClaimUnavailableError:
        raise
    except Exception as exc:
        raise ExecutionClaimUnavailableError(
            f"atomic GCS execution claim failed: {type(exc).__name__}"
        ) from exc


def _claim_local_marker(store, marker_key: str, payload: str) -> bool:
    path_builder = getattr(store, "_local_path", None)
    if not callable(path_builder):
        raise ExecutionClaimUnavailableError(
            "execution state store cannot resolve its local marker path"
        )
    path = Path(path_builder(marker_key))
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return False
    except OSError as exc:
        raise ExecutionClaimUnavailableError(
            f"atomic local execution claim failed: {type(exc).__name__}"
        ) from exc
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception as exc:
        raise ExecutionClaimUnavailableError(
            f"atomic local execution claim write failed: {type(exc).__name__}"
        ) from exc
    return True


def claim_execution_marker(
    store,
    marker_key: str,
    *,
    metadata: Mapping[str, Any] | None = None,
) -> bool:
    """Atomically claim one execution identity.

    Returns ``True`` only for the single caller that created the claim.  A
    pre-existing claim or terminal marker returns ``False``.  Storage errors
    raise so the caller fails closed before broker submission.
    """
    if not store or not str(marker_key or "").strip():
        raise ExecutionClaimUnavailableError("execution claim requires a store and marker key")

    native_claim = getattr(store, "claim_marker", None)
    # Test and adapter callers may provide a concrete GCS client through the
    # legacy store hook.  The shared object-store implementation intentionally
    # owns normal runtime credentials, so keep this narrow fallback only when
    # that explicit client hook is present.
    if callable(native_claim) and getattr(store, "client_factory", None) is None:
        try:
            return bool(native_claim(marker_key, metadata=dict(metadata or {})))
        except Exception as exc:
            raise ExecutionClaimUnavailableError(
                f"execution state claim failed: {type(exc).__name__}"
            ) from exc

    payload = _claim_payload(marker_key, metadata)
    if str(getattr(store, "cloud_prefix_uri", "") or "").strip():
        return _claim_gcs_marker(store, marker_key, payload)
    if getattr(store, "local_dir", None):
        return _claim_local_marker(store, marker_key, payload)
    raise ExecutionClaimUnavailableError("execution state store has no durable claim backend")
