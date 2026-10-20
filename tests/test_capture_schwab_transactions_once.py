"""Actual SDK/HTTPX/collector, synthetic transport/auth only; no live capture."""
import json
import logging
import os
import signal
import socket
import time
import threading
from pathlib import Path

import httpx
import pytest
from schwab.client import Client

from scripts import capture_schwab_transactions_once as capture
from scripts.publish_account_facts_from_reports import _binding_id
from scripts.publish_runtime_daily_from_reports import TARGET
from application.external_flow_snapshot import collect_external_flow_snapshot

HASH = "synthetic-native-account"
NOW = 1792179900


@pytest.fixture(autouse=True)
def no_external_io(monkeypatch):
    def deny(*_args, **_kwargs):
        raise AssertionError("external I/O forbidden")
    monkeypatch.setattr(socket, "socket", deny)
    monkeypatch.setattr("subprocess.Popen", deny)
    monkeypatch.setattr("scripts.schwab_native_identity._load_existing_token", deny)


@pytest.fixture
def inputs(tmp_path):
    os.chmod(tmp_path, 0o700)
    env = {"GCP_PROJECT_ID": "charlesschwabquant", "SCHWAB_API_KEY": "synthetic-api-key",
           "SCHWAB_ACCOUNT_FACTS_TARGET_ID": "synthetic-target",
           "RUNTIME_TARGET_JSON": json.dumps({**TARGET, "account_selector": ["live"],
               "runtime_risk_limits": {"binding": {"account_hash": HASH}},
               "market_calendar": "NYSE", "market_timezone": "America/New_York"})}
    binding = _binding_id(HASH, TARGET["service"])
    document = {"ok": True, "projection": "identity", "target_id": "synthetic-target",
                "source_binding_id": binding, "records": [
                    {"observation_date": date, "observed_finished_at": stamp,
                     "target_id_matches": True, "source_binding_id_matches": True,
                     "account_scope_matches": True, "record_revision": None}
                    for date, stamp in zip(["2026-10-15", "2026-10-16"],
                                          [capture.FINANCIAL_START, capture.FINANCIAL_END])]}
    metadata = tmp_path / "identity.json"
    metadata.write_text(json.dumps(document))
    metadata.chmod(0o600)
    return env, metadata, document, tmp_path / "capture"


class Stream(httpx.SyncByteStream):
    def __init__(self, chunks):
        self.chunks, self.closed, self.reads = chunks, False, 0

    def __iter__(self):
        for chunk in self.chunks:
            self.reads += 1
            if isinstance(chunk, BaseException):
                raise chunk
            yield chunk

    def close(self):
        self.closed = True


def execute(inputs, *, body=b"[]", status=200, encoding="identity", chunks=None, loader=None):
    env, metadata, _document, out = inputs
    requests, loaded = [], []
    stream = Stream([body] if chunks is None else chunks)

    def handler(request):
        requests.append(request)
        return httpx.Response(status, headers={"Content-Encoding": encoding}, stream=stream)

    def auth(project, secret):
        loaded.append((project, secret))
        return json.dumps({"creation_timestamp": NOW - 60,
                           "token": {"access_token": "PRIVATE_TOKEN_SENTINEL", "expires_at": NOW + 3600}})

    result = capture.capture(metadata_path=metadata, output_dir=out, environ=env,
                             token_loader=loader or auth, http_transport=httpx.MockTransport(handler), now=NOW)
    return result.receipt, requests, loaded, stream


def test_actual_sdk_stream_and_collector_window_privacy(inputs, capsys):
    rows = [{"type": "CASH_RECEIPT", "time": stamp, "amount": "PRIVATE_AMOUNT_SENTINEL"}
            for stamp in [capture.FINANCIAL_START, "2026-10-15T19:45:05.443767Z",
                          capture.FINANCIAL_END, "2026-10-16T19:45:06.304510Z",
                          "2026-10-16T19:45:06.3045091Z"]]
    raw = json.dumps(rows).encode()
    receipt, requests, loaded, stream = execute(inputs, body=raw)
    assert receipt["status"] == "captured_incomplete"
    assert receipt["source_complete"] is False and receipt["authority"] == "not_assessed"
    assert receipt["request_attempts"] == 1 and receipt["raw_count"] == 5
    assert receipt["precision_counts"]["beyond_microsecond"] == 1
    assert receipt["boundary_counts"]["inside_original_window"] == 2
    assert len(requests) == len(loaded) == 1
    assert requests[0].method == "GET"
    assert requests[0].url.params["startDate"] == capture.WIRE_START
    assert requests[0].url.params["endDate"] == capture.WIRE_END
    assert requests[0].url.params["types"] == capture.DEFAULT_TYPES
    assert stream.closed and stream.reads == 1
    out = inputs[3]
    assert (out / "response.body").read_bytes() == raw
    assert {p.name for p in out.iterdir()} == {"response.body", "receipt.json", "capture.started"}
    assert all(p.stat().st_mode & 0o777 == 0o600 for p in out.iterdir())
    assert out.stat().st_mode & 0o777 == 0o700
    public = json.dumps(receipt) + str(capsys.readouterr())
    assert all(value not in public for value in [HASH, "PRIVATE_AMOUNT_SENTINEL", "PRIVATE_TOKEN_SENTINEL"])


@pytest.mark.parametrize("body,status,encoding,reason", [
    (b'{"error":"PRIVATE_BODY"}', 403, "identity", "http_error"),
    (b"[]", 200, "gzip", "encoding_unsupported"),
    (b"not-json", 200, "identity", "schema_invalid"),
    (b'[{"time":"a","time":"b"}]', 200, "identity", "schema_invalid"),
    (b'[[[[[[[[[[[[[[[[[[[[[', 200, "identity", "schema_invalid"),
    (b"{}", 200, "identity", "schema_invalid"),
    (b"[NaN]", 200, "identity", "schema_invalid"),
])
def test_wire_fail_closed_once(inputs, body, status, encoding, reason, capsys):
    receipt, requests, _loaded, stream = execute(inputs, body=body, status=status, encoding=encoding)
    assert receipt["status"] == "blocked" and receipt["reason"] == reason
    assert receipt["request_attempts"] == len(requests) == 1
    assert stream.closed
    assert "PRIVATE_BODY" not in json.dumps(receipt) + str(capsys.readouterr())


def test_cap_is_raw_and_stream_is_not_prebuffered(inputs):
    receipt, requests, _, _ = execute(inputs, body=json.dumps([{}] * 3000).encode())
    assert receipt["reason"] == "row_limit" and receipt["cap_hit"] is True
    assert len(requests) == 1


def test_byte_cap_stops_stream_before_next_chunk(inputs, monkeypatch):
    monkeypatch.setattr(capture, "MAX_BYTES", 16)
    receipt, requests, _, stream = execute(inputs, chunks=[b" " * 17, b"PRIVATE_UNREAD"])
    assert receipt["reason"] == "body_limit" and len(requests) == 1
    assert stream.reads == 1 and stream.closed


def test_stream_exception_is_private_and_not_retried(inputs, capsys):
    receipt, requests, _, stream = execute(inputs, chunks=[b"[", OSError("PRIVATE_STREAM")])
    assert receipt["reason"] == "stream_error" and len(requests) == 1 and stream.closed
    assert "PRIVATE_STREAM" not in str(capsys.readouterr()) + json.dumps(receipt)


@pytest.mark.parametrize("mutation", ["binding", "missing", "time", "flag", "duplicate", "mode", "symlink", "output", "extra", "appended"])
def test_preflight_before_auth_or_request(inputs, mutation):
    env, metadata, document, out = inputs
    if mutation == "binding":
        env["SCHWAB_ACCOUNT_FACTS_TARGET_ID"] = "different"
    elif mutation == "missing":
        metadata.unlink()
    elif mutation == "time":
        document["records"][0]["observed_finished_at"] = "2026-10-15T19:45:05Z"
        metadata.write_text(json.dumps(document))
    elif mutation == "flag":
        document["records"][0]["target_id_matches"] = None
        metadata.write_text(json.dumps(document))
    elif mutation == "duplicate":
        metadata.write_text('{"ok":true,"ok":true}')
    elif mutation == "mode":
        metadata.chmod(0o644)
    elif mutation == "symlink":
        alternate = metadata.with_name("link.json")
        alternate.symlink_to(metadata)
        inputs = env, alternate, document, out
    elif mutation == "extra":
        document["payload_json"] = "PRIVATE_ASSET_SENTINEL"
        metadata.write_text(json.dumps(document))
    elif mutation == "appended":
        metadata.write_text(json.dumps(document) + ", {}")
    else:
        out.mkdir()
    receipt, requests, loaded, _ = execute(inputs)
    assert receipt["status"] == "blocked" and not requests and not loaded


@pytest.mark.parametrize("exception", [RuntimeError("PRIVATE_ERROR"), KeyboardInterrupt(), SystemExit(9)])
def test_timer_output_and_logging_restored_after_exception(inputs, exception, capsys):
    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_logging = logging.root.manager.disable
    def auth(*_args):
        print("PRIVATE_TOKEN_SENTINEL")
        logging.error("PRIVATE_TOKEN_SENTINEL")
        raise exception
    receipt, requests, _, _ = execute(inputs, loader=auth)
    assert receipt["status"] == "blocked" and not requests
    assert signal.getsignal(signal.SIGALRM) is previous_handler
    assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
    assert logging.root.manager.disable == previous_logging
    assert "PRIVATE_TOKEN_SENTINEL" not in str(capsys.readouterr())


def test_alarm_is_sticky_when_client_swallows_it(tmp_path):
    os.chmod(tmp_path, 0o700)
    binding = capture.BindingInputs(HASH, "target", "binding", "target", "binding")
    def builder(session):
        try:
            signal.getsignal(signal.SIGALRM)(signal.SIGALRM, None)
        except capture.CaptureError:
            pass
        return Client("synthetic-api", session)
    result = capture.CaptureOnce(client_builder=builder, transport=None,
        collector=lambda *_a, **_k: {"source_complete": False}).run(binding=binding, output_dir=tmp_path / "out")
    assert result.receipt["status"] == "blocked"
    assert result.receipt["reason"] == "deadline_exceeded"


def test_real_alarm_interrupts_blocking_transport_once(inputs, monkeypatch):
    original = signal.setitimer
    previous_handler = signal.getsignal(signal.SIGALRM)
    def short_timer(which, seconds, *args):
        return original(which, 0.02 if seconds == 30.0 else seconds, *args)
    monkeypatch.setattr(signal, "setitimer", short_timer)
    def chunks():
        time.sleep(0.5)  # Synthetic blocking stream, no socket or service.
        yield b"[]"
    receipt, requests, _, stream = execute(inputs, chunks=chunks())
    assert receipt["reason"] == "deadline_exceeded" and len(requests) == 1 and stream.closed
    assert signal.getsignal(signal.SIGALRM) is previous_handler
    assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)


def test_expired_auth_never_reaches_http(inputs):
    def expired(*_args):
        return json.dumps({"creation_timestamp": NOW - 1000,
                           "token": {"access_token": "synthetic", "expires_at": NOW - 1}})
    receipt, requests, _, _ = execute(inputs, loader=expired)
    assert receipt["reason"] == "auth_invalid" and not requests


def test_existing_timer_and_worker_thread_do_not_read_auth(inputs):
    signal.setitimer(signal.ITIMER_REAL, 20)
    try:
        receipt, requests, loaded, _ = execute(inputs)
        assert receipt["reason"] == "deadline_unavailable" and not requests and not loaded
        assert signal.getitimer(signal.ITIMER_REAL)[0] > 0
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
    inputs[3].rename(inputs[3].with_name("old"))
    result = []
    thread = threading.Thread(target=lambda: result.append(execute(inputs)))
    thread.start()
    thread.join()
    assert result[0][0]["reason"] == "deadline_unavailable"
    assert not result[0][1] and not result[0][2]


@pytest.mark.parametrize("attack", ["second", "enumerate", "post", "schema"])
def test_swallowed_client_errors_cannot_restore_success(tmp_path, attack):
    os.chmod(tmp_path, 0o700)
    binding = capture.BindingInputs(HASH, "target", "binding", "target", "binding")
    class Transport:
        def request(self, *_args, **_kwargs):
            return capture.WireReply(200, [b"bad" if attack == "schema" else b"[]"])
    def collector(client, **_kwargs):
        try:
            if attack == "enumerate":
                client.get_account_numbers()
            elif attack == "post":
                client.session.post("https://forbidden")
            else:
                client.get_transactions(HASH, start_date=__import__('datetime').datetime(2026, 10, 15, 19, 45, 5),
                                        end_date=__import__('datetime').datetime(2026, 10, 16, 19, 45, 7))
                if attack == "second":
                    client.get_transactions(HASH)
        except BaseException:
            pass
        return {"source_complete": False}
    operation = capture.CaptureOnce(client_builder=lambda session: Client("synthetic", session),
        transport=Transport(), collector=collector)
    result = operation.run(binding=binding, output_dir=tmp_path / "out")
    assert result.receipt["status"] == "blocked"
    assert result.receipt["request_attempts"] <= 1
    assert operation.run(binding=binding, output_dir=tmp_path / "another").receipt["reason"] == "request_budget_exhausted"


def test_real_collector_preserves_exact_endpoints_and_precision(tmp_path):
    os.chmod(tmp_path, 0o700)
    seen = []
    binding = capture.BindingInputs(HASH, "target", "binding", "target", "binding")
    rows = [{"type": "CASH_RECEIPT", "time": stamp} for stamp in [capture.FINANCIAL_START,
        capture.FINANCIAL_END, "2026-10-16T19:45:06.3045091Z"]]
    class Transport:
        def request(self, *_args, **_kwargs):
            return capture.WireReply(200, [json.dumps(rows).encode()])
    def collector(*args, **kwargs):
        snapshot = collect_external_flow_snapshot(*args, **kwargs)
        seen.append(snapshot)
        return snapshot
    result = capture.CaptureOnce(client_builder=lambda session: Client("synthetic", session),
        transport=Transport(), collector=collector).run(binding=binding, output_dir=tmp_path / "out")
    assert result.receipt["status"] == "captured_incomplete"
    assert seen[0]["transactions"] == rows[1:]
    assert seen[0]["source_complete"] is False
