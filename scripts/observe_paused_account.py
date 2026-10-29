"""Bounded read-only current Schwab observations; never grants recovery authority."""
from __future__ import annotations

import datetime as dt
import json
import os
from urllib.parse import quote

from scripts import diagnose_pilot_metadata as metadata
from scripts.capture_schwab_transactions_once import _private_call_output
from scripts.diagnose_pilot_native_identity import _account_hash
from scripts.schwab_native_identity import _access_token, _account_hash_matches, _load_existing_token, SECRET_ID


class ReadError(Exception):
    pass


class _Response:
    status_code = 200
    def __init__(self, value):
        self.value = value
    def json(self):
        return self.value


class ReadOnlyClient:
    """Only three fixed GETs, with response limits, no redirects or refresh."""
    def __init__(self, token, *, http=None):
        import httpx
        self.http = http or httpx.Client(
            headers={"Authorization": "Bearer " + token, "Accept-Encoding": "identity"},
            follow_redirects=False, trust_env=False,
            transport=httpx.HTTPTransport(retries=0, trust_env=False), timeout=20,
        )
        self.cache = {}
        self.account = None

    def _get(self, key, path, *, params=None):
        if key in self.cache:
            return _Response(self.cache[key])
        # Cache failure too: a caller must never automatically retry a provider request.
        self.cache[key] = None
        with self.http.stream("GET", "https://api.schwabapi.com/trader/v1/" + path, params=params) as response:
            if response.status_code != 200 or response.headers.get("content-encoding", "identity") not in ("", "identity"):
                raise ReadError("broker_read_failed")
            body = bytearray()
            for part in response.iter_raw():
                if len(body) + len(part) > 16 * 1024 * 1024:
                    raise ReadError("broker_response_limit")
                body.extend(part)
        try:
            value = metadata._json(bytes(body).decode("utf-8"))
        except Exception:
            raise ReadError("broker_response_invalid") from None
        self.cache[key] = value
        return _Response(value)

    def get_account_numbers(self):
        return self._get("identities", "accounts/accountNumbers")

    def get_account(self, account_hash, *, fields=None):
        if account_hash != self.account:
            raise ReadError("account_binding_mismatch")
        return self._get("account", "accounts/" + quote(account_hash, safe=""), params={"fields": "positions"})

    def get_orders_for_account(self, account_hash, *, from_entered_datetime, to_entered_datetime, max_results):
        if account_hash != self.account or max_results != 3000:
            raise ReadError("account_binding_mismatch")
        return self._get("orders", "accounts/" + quote(account_hash, safe="") + "/orders", params={
            "fromEnteredTime": from_entered_datetime.isoformat(timespec="seconds").replace("+00:00", "Z"),
            "toEnteredTime": to_entered_datetime.isoformat(timespec="seconds").replace("+00:00", "Z"),
            "maxResults": max_results,
        })

    def close(self):
        self.http.close()


def observe(environ, *, describe=None, load_token=None, client_factory=None, now=None):
    """Read current facts only, keeping private values in memory and PAUSED unchanged."""
    from application.broker_reconciliation import (
        _normalize_order, _TERMINAL_ORDER_STATUSES, collect_read_only_reconciliation_observations,
    )
    from quant_platform_kit.schwab.portfolio import fetch_account_snapshot
    now = now or dt.datetime.now(dt.timezone.utc)
    result = {"schema_version": "schwab.paused-current-observation.v1", "status": "unavailable",
              "reason": "read_unavailable", "positions": "unknown", "cash": "unknown",
              "open_orders": "unknown", "order_coverage": "unknown", "recent_executions": "unknown",
              "order_window_days": 365, "observed_at": None, "history_link": "unknown", "recovery_qualified": False, "no_order": True}
    client = None
    read = describe or metadata._describe
    try:
        with _private_call_output():
            service, project, region, expected, _ = metadata._expected(environ)
            revision = metadata._serving(read("services", service, project, region), service)
            target = metadata._runtime_target(metadata._revision_env(read("revisions", revision, project, region), revision, service), service)
            if target.get("live_continuity", {}).get("state") != "PAUSED":
                raise ReadError("target_not_paused")
            if any(target[field] != expected[field] for field in metadata._IDENTITY_FIELDS):
                raise ReadError("target_binding_mismatch")
            expected_hash = _account_hash(target)
            if expected_hash != _account_hash(expected):
                raise ReadError("target_binding_mismatch")
            if metadata._serving(read("services", service, project, region), service) != revision:
                raise ReadError("serving_changed")
            raw = (load_token or _load_existing_token)(project, SECRET_ID)
            token, token_status = _access_token(raw, now=now.timestamp())
            if token is None:
                raise ReadError(token_status)
            client = (client_factory or ReadOnlyClient)(token)
            identities = client.get_account_numbers().json()
            if not isinstance(identities, list) or len(identities) != 1 or not isinstance(identities[0], dict):
                raise ReadError("native_identity_unverified")
            native = identities[0].get("hashValue")
            if not _account_hash_matches(expected_hash, native):
                raise ReadError("native_identity_unverified")
            client.account = native
            try:
                account = client.get_account(native).json()
            except Exception:
                account = None
            account = account.get("securitiesAccount") if isinstance(account, dict) else None
            if not isinstance(account, dict):
                account = {}
            try:
                orders = client.get_orders_for_account(native, from_entered_datetime=now - dt.timedelta(days=365),
                                                        to_entered_datetime=now, max_results=3000).json()
                if not isinstance(orders, list) or any(not isinstance(row, dict) for row in orders):
                    raise ReadError("orders_unavailable")
                normalized = [_normalize_order(row) for row in orders]
            except Exception:
                orders = None
            else:
                if any(row.get("childOrderStrategies") for row in orders):
                    result["order_coverage"] = "nested_orders_unverified"
                elif len(orders) < 3000:
                    result["order_coverage"] = "bounded_not_at_limit"
                    result["open_orders"] = "present_in_window" if any(row["status"] not in _TERMINAL_ORDER_STATUSES for row in normalized) else "none_observed_in_window"
                else:
                    result["order_coverage"] = "truncated_or_at_limit"
            positions = account.get("positions")
            if isinstance(positions, list):
                try:
                    observations = (collect_read_only_reconciliation_observations(client, fetch_account_snapshot=fetch_account_snapshot, now=now)
                                    if orders is not None else fetch_account_snapshot(client, strategy_symbols=()))
                except Exception:
                    pass  # Keep the independent orders result; account facts remain unknown.
                else:
                    result["positions"] = "present" if observations.positions else "none_observed"
                    result["cash"] = "observed_unqualified"
            if metadata._serving(read("services", service, project, region), service) != revision:
                raise ReadError("serving_changed")
            result.update(status="observed", reason="partial_current_observation",
                          observed_at=dt.datetime.now(dt.timezone.utc).isoformat())
    except Exception as exc:
        allowed = {"target_not_paused", "target_binding_mismatch", "serving_changed", "native_identity_unverified", "token_expired", "token_invalid", "broker_read_failed", "broker_response_limit", "broker_response_invalid", "account_unavailable", "orders_unavailable"}
        result["reason"] = str(exc) if isinstance(exc, ReadError) and str(exc) in allowed else "read_unavailable"
        result.update(positions="unknown", cash="unknown", open_orders="unknown", order_coverage="unknown", observed_at=None)
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                result.update(status="unavailable", reason="client_close_failed", positions="unknown", cash="unknown", open_orders="unknown", order_coverage="unknown", observed_at=None)
    return result


def main():
    result = observe(os.environ)
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "observed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
