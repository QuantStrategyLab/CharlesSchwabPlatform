from datetime import datetime, timezone

from scripts import observe_paused_account as mod


class FakeClient:
    def __init__(self, *, paused_positions=None, orders=None):
        self.account = None
        self.positions = paused_positions
        self.orders = orders or []
        self.calls = []
        self.closed = False
    def get_account_numbers(self):
        self.calls.append("identities")
        return mod._Response([{"hashValue": "a" * 64}])
    def get_account(self, account_hash, **_kwargs):
        self.calls.append("account")
        account = {"currentBalances": {"cashAvailableForTrading": 10}}
        if self.positions is not None:
            account["positions"] = self.positions
        return mod._Response({"securitiesAccount": account})
    def get_orders_for_account(self, account_hash, **_kwargs):
        self.calls.append("orders")
        return mod._Response(self.orders)
    def close(self):
        self.closed = True


def run(monkeypatch, client, *, state="PAUSED", changed=False):
    target = {name: "x" for name in mod.metadata._IDENTITY_FIELDS}
    target.update(live_continuity={"state": state}, runtime_risk_limits={"binding": {"account_hash": "a" * 64}})
    monkeypatch.setattr(mod.metadata, "_expected", lambda _env: ("service", "project", "region", target, {}))
    monkeypatch.setattr(mod.metadata, "_revision_env", lambda *_args: {})
    monkeypatch.setattr(mod.metadata, "_runtime_target", lambda *_args: target)
    calls = []
    def describe(kind, *_args):
        if kind == "services":
            calls.append(kind)
            return {"revision": "different" if changed and len(calls) == 3 else "initial"}
        return {}
    monkeypatch.setattr(mod.metadata, "_serving", lambda value, _service: value["revision"])
    monkeypatch.setattr(mod, "_access_token", lambda *_args, **_kwargs: ("synthetic-token", "verified"))
    return mod.observe({}, describe=describe, load_token=lambda *_: "synthetic", client_factory=lambda _: client,
                       now=datetime(2026, 10, 29, tzinfo=timezone.utc))


def test_missing_positions_remains_unknown_while_orders_are_checked(monkeypatch):
    client = FakeClient(orders=[{"orderId": "private-order-canary", "status": "WORKING", "orderLegCollection": [{"instrument": {"symbol": "SYN"}, "quantity": 1}]}])
    result = run(monkeypatch, client)
    assert result["status"] == "observed"
    assert result["positions"] == "unknown"
    assert result["open_orders"] == "present_in_window"
    assert result["recent_executions"] == "unknown"
    assert result["history_link"] == "unknown"
    assert result["recovery_qualified"] is False and result["no_order"] is True
    assert client.closed and "private-order-canary" not in str(result) and "SYN" not in str(result)


def test_paused_only_prevents_any_broker_request(monkeypatch):
    client = FakeClient()
    result = run(monkeypatch, client, state="ACTIVE_LKG")
    assert result["reason"] == "target_not_paused" and client.calls == []


def test_serving_change_discards_observation(monkeypatch):
    result = run(monkeypatch, FakeClient(), changed=True)
    assert result["status"] == "unavailable"
    assert result["reason"] == "serving_changed"
    assert result["open_orders"] == "unknown"


def test_order_limit_cannot_be_reported_as_empty(monkeypatch):
    row = {"orderId": 42, "status": "FILLED", "orderLegCollection": [{"instrument": {"symbol": "SYN"}, "quantity": 1}]}
    result = run(monkeypatch, FakeClient(orders=[row] * 3000))
    assert result["status"] == "observed"
    assert result["order_coverage"] == "truncated_or_at_limit"
    assert result["open_orders"] == "unknown"


def test_present_positions_use_existing_collector(monkeypatch):
    result = run(monkeypatch, FakeClient(paused_positions=[{"instrument": {"symbol": "SYN"}, "longQuantity": 1, "marketValue": 10}]))
    assert result["positions"] == "present" and result["cash"] == "observed_unqualified"
    assert result["open_orders"] == "none_observed_in_window"
    assert result["order_coverage"] == "bounded_not_at_limit" and result["order_window_days"] == 365


def test_collector_reuses_each_network_read_without_refresh_or_write(monkeypatch):
    import httpx
    seen = []
    def handler(request):
        seen.append((request.method, request.url.path))
        if request.url.path.endswith("/accountNumbers"):
            payload = [{"hashValue": "a" * 64}]
        elif request.url.path.endswith("/orders"):
            payload = []
        else:
            payload = {"securitiesAccount": {"positions": [], "currentBalances": {"cashAvailableForTrading": 10}}}
        return httpx.Response(200, stream=httpx.ByteStream(__import__("json").dumps(payload).encode()))
    http = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
    result = run(monkeypatch, mod.ReadOnlyClient("synthetic", http=http))
    assert result["status"] == "observed" and result["positions"] == "none_observed"
    assert [method for method, _ in seen] == ["GET", "GET", "GET"]
    assert sum(path.endswith("/orders") for _, path in seen) == 1


def test_nested_order_remains_unknown(monkeypatch):
    row = {"orderId": 42, "status": "FILLED", "orderLegCollection": [{"instrument": {"symbol": "SYN"}, "quantity": 1}],
           "childOrderStrategies": [{"status": "WORKING"}]}
    result = run(monkeypatch, FakeClient(orders=[row]))
    assert result["order_coverage"] == "nested_orders_unverified"
    assert result["open_orders"] == "unknown"


def test_account_read_failure_does_not_block_order_observation(monkeypatch):
    class AccountUnavailable(FakeClient):
        def get_account(self, *_args, **_kwargs):
            raise RuntimeError("synthetic-private-detail")
    result = run(monkeypatch, AccountUnavailable())
    assert result["status"] == "observed" and result["positions"] == "unknown"
    assert result["open_orders"] == "none_observed_in_window"
    assert "synthetic-private-detail" not in str(result)


def test_order_failure_keeps_independent_account_observation(monkeypatch):
    class OrdersUnavailable(FakeClient):
        def get_orders_for_account(self, *_args, **_kwargs):
            raise RuntimeError("synthetic-private-detail")
    result = run(monkeypatch, OrdersUnavailable(paused_positions=[]))
    assert result["status"] == "observed" and result["positions"] == "none_observed"
    assert result["open_orders"] == "unknown" and result["order_coverage"] == "unknown"
    assert "synthetic-private-detail" not in str(result)
