"""Explicit private one-shot transactions capture; never an authority/completeness proof.

Uses the existing collector and normal SDK with a non-refreshing streaming session.
Matching a supplied private metadata file is not proof of its remote provenance.
Only an operator-approved original runtime may invoke the live CLI; tests inject
synthetic auth/HTTP ports. No account discovery, OAuth refresh, report publication
or automatic retry is implemented here.
"""
from dataclasses import dataclass, field
from contextlib import contextmanager, redirect_stdout, redirect_stderr
from datetime import datetime
import hashlib
import io
import json
import logging
import os
from pathlib import Path
import re
import time
import signal
import stat
import threading
import argparse
from urllib.parse import quote

FINANCIAL_START = "2026-10-15T19:45:05.443766Z"
FINANCIAL_END = "2026-10-16T19:45:06.304509Z"
WIRE_START = "2026-10-15T19:45:05Z"
WIRE_END = "2026-10-16T19:45:07Z"
DEFAULT_TYPES = "TRADE,RECEIVE_AND_DELIVER,DIVIDEND_OR_INTEREST,ACH_RECEIPT,ACH_DISBURSEMENT,CASH_RECEIPT,CASH_DISBURSEMENT,ELECTRONIC_FUND,WIRE_OUT,WIRE_IN,JOURNAL,MEMORANDUM,MARGIN_CALL,MONEY_MARKET,SMA_ADJUSTMENT"
MAX_BYTES = 16 * 1024 * 1024
_CODES = frozenset("binding_mismatch binding_invalid output_invalid private_save_failed request_forbidden request_budget_exhausted transport_error stream_error stream_invalid body_limit deadline_exceeded encoding_unsupported http_error schema_invalid row_limit client_error collector_error collector_contract auth_invalid metadata_invalid deadline_unavailable arguments_invalid".split())


class CaptureError(Exception):
    def __init__(self, code):
        self.code = code if type(code) is str and code in _CODES else "collector_error"
        super().__init__(self.code)


@dataclass(frozen=True, repr=False)
class BindingInputs:
    account_hash: str = field(repr=False)
    target_id: str = field(repr=False)
    source_binding_id: str = field(repr=False)
    historical_target_id: str = field(repr=False)
    historical_source_binding_id: str = field(repr=False)


@dataclass(repr=False)
class WireReply:
    status_code: int
    chunks: object = field(repr=False)
    encoding: str = "identity"


@dataclass(repr=False)
class CaptureResult:
    receipt: dict

    def __repr__(self):
        return "<CaptureResult incomplete; authority not assessed>"


class _Discard(io.TextIOBase):
    def write(self, text):
        return len(text)


@contextmanager
def _private_call_output():
    # Standalone one-shot process only; do not embed this in a concurrent server.
    previous = logging.root.manager.disable
    with redirect_stdout(_Discard()), redirect_stderr(_Discard()):
        logging.disable(max(previous, 2**31 - 1))
        try:
            yield
        finally:
            logging.disable(previous)


def _binding_matches(binding):
    if type(binding) is not BindingInputs:
        raise CaptureError("binding_invalid")
    values = (binding.account_hash, binding.target_id, binding.source_binding_id,
              binding.historical_target_id, binding.historical_source_binding_id)
    if any(type(v) is not str or not v or v != v.strip() or len(v) > 512 for v in values):
        raise CaptureError("binding_invalid")
    if (binding.target_id != binding.historical_target_id
            or binding.source_binding_id != binding.historical_source_binding_id):
        raise CaptureError("binding_mismatch")


def _json(body):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError
            result[key] = value
        return result
    def bad_constant(_value):
        raise ValueError
    try:
        data = json.loads(body, object_pairs_hook=pairs, parse_constant=bad_constant)
    except (ValueError, UnicodeError, RecursionError):
        raise CaptureError("schema_invalid") from None
    if type(data) is not list:
        raise CaptureError("schema_invalid")
    return data


class _Response:
    def __init__(self, session):
        self._session, self.status_code = session, session.status

    @property
    def text(self):
        # SDK debug logging evaluates .text even when logging is disabled.
        return "[private response suppressed]"

    def json(self):
        return _json(self._session.body)


class _OneShotSession:
    def __init__(self, transport, account_hash, clock):
        self.transport, self.clock = transport, clock
        self.timeout = 30.0
        self.url = "https://api.schwabapi.com/trader/v1/accounts/" + quote(account_hash, safe="") + "/transactions"
        self.params = {"types": DEFAULT_TYPES, "startDate": WIRE_START, "endDate": WIRE_END}
        self.deadline = clock() + 30
        self.calls, self.status, self.body, self.rows, self.fatal = 0, None, None, None, None

    def fail(self, reason):
        self.fatal = self.fatal or reason
        raise CaptureError(self.fatal) from None

    def check_deadline(self):
        remaining = self.deadline - self.clock()
        if remaining <= 0:
            self.fail("deadline_exceeded")
        return remaining

    def request(self, method, url, *, params=None, **options):
        if self.fatal:
            self.fail(self.fatal)
        if self.calls:
            self.fail("request_budget_exhausted")
        if (type(method) is not str or method != "GET" or type(url) is not str
                or url != self.url or type(params) is not dict or params != self.params or options):
            self.fail("request_forbidden")
        remaining = self.check_deadline()
        self.calls += 1
        try:
            reply = self.transport.request("GET", self.url, params=dict(self.params),
                timeout_seconds=remaining, follow_redirects=False, retries=0, trust_env=False)
        except BaseException:
            self.fail("transport_error")
        self.check_deadline()
        if type(reply) is not WireReply or type(reply.status_code) is not int or not 100 <= reply.status_code <= 599:
            self.fail("stream_invalid")
        self.status = reply.status_code
        if type(reply.encoding) is not str or reply.encoding not in ("", "identity"):
            close = getattr(reply.chunks, "close", None)
            if close is not None:
                try:
                    close()
                except BaseException:
                    pass
            self.fail("encoding_unsupported")
        body = bytearray()
        try:
            for chunk in reply.chunks:
                self.check_deadline()
                if type(chunk) is not bytes:
                    self.fail("stream_invalid")
                if len(body) + len(chunk) > MAX_BYTES:
                    self.fail("body_limit")
                body.extend(chunk)
        except CaptureError:
            raise
        except BaseException:
            self.fail("stream_error")
        finally:
            close = getattr(reply.chunks, "close", None)
            if close is not None:
                try:
                    close()
                except BaseException:
                    self.fatal = self.fatal or "stream_error"
        if self.fatal:
            self.fail(self.fatal)
        self.check_deadline()
        self.body = bytes(body)
        if self.status != 200:
            self.fail("http_error")
        try:
            self.rows = _json(self.body)
        except CaptureError as error:
            self.fail(error.code)
        if len(self.rows) >= 3000:
            self.fail("row_limit")
        return _Response(self)

    def get(self, url, *, params=None, **options):
        return self.request("GET", url, params=params, **options)

    def post(self, *_args, **_kwargs):
        self.fail("request_forbidden")

    put = post
    delete = post
    patch = post


def _profiles(rows):
    precision = dict(whole_second=0, millisecond_or_less=0, microsecond=0,
                     beyond_microsecond=0, unparseable=0)
    boundary = dict(exact_start=0, exact_end=0, before_or_at_start=0,
                    inside_original_window=0, after_end=0, unresolved=0)
    start, end = (datetime.fromisoformat(s.replace("Z", "+00:00"))
                  for s in (FINANCIAL_START, FINANCIAL_END))
    for row in rows or []:
        value = row.get("time") if type(row) is dict else None
        match = re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.(\d+))?(?:Z|[+-]\d{2}:\d{2})", value) if type(value) is str else None
        try:
            if not match:
                raise ValueError
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            precision["unparseable"] += 1
            boundary["unresolved"] += 1
            continue
        digits = len(match[1] or "")
        key = "whole_second" if not digits else "millisecond_or_less" if digits <= 3 else "microsecond" if digits <= 6 else "beyond_microsecond"
        precision[key] += 1
        if digits > 6:
            boundary["unresolved"] += 1
            continue
        if parsed == start:
            boundary["exact_start"] += 1
        if parsed == end:
            boundary["exact_end"] += 1
        if parsed <= start:
            boundary["before_or_at_start"] += 1
        elif parsed <= end:
            boundary["inside_original_window"] += 1
        else:
            boundary["after_end"] += 1
    return precision, boundary


def _write_private(path, body):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())


def _private_path(path, *, file=False):
    """No symlink traversal; private owner-controlled input or output parent only."""
    path = Path(path)
    if not path.is_absolute() or path.resolve() != path:
        raise CaptureError("output_invalid" if not file else "metadata_invalid")
    for part in (path, *path.parents):
        if part.is_symlink():
            raise CaptureError("output_invalid" if not file else "metadata_invalid")
    info = path.stat()
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
        raise CaptureError("output_invalid" if not file else "metadata_invalid")
    if not (stat.S_ISREG(info.st_mode) if file else stat.S_ISDIR(info.st_mode)):
        raise CaptureError("output_invalid" if not file else "metadata_invalid")


def _prepare_output(out):
    if not out.is_absolute() or out.exists() or out.is_symlink():
        raise CaptureError("output_invalid")
    _private_path(out.parent)
    out.mkdir(mode=0o700)


@contextmanager
def _deadline_guard():
    # A timer owned by another caller is never replaced. Standalone/main-thread only.
    if (threading.current_thread() is not threading.main_thread()
            or not hasattr(signal, "setitimer")
            or signal.getitimer(signal.ITIMER_REAL) != (0.0, 0.0)):
        raise CaptureError("deadline_unavailable")
    previous = signal.getsignal(signal.SIGALRM)
    session = [None]

    def expired(_number, _frame):
        if session[0] is not None:
            session[0].fatal = "deadline_exceeded"
        raise CaptureError("deadline_exceeded")

    signal.signal(signal.SIGALRM, expired)
    try:
        signal.setitimer(signal.ITIMER_REAL, 30.0)
        yield session
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


class HttpxTransport:
    """Normal HTTPX, without OAuth callbacks, redirects, proxies or retry transport."""
    def __init__(self, access_token, *, transport=None):
        import httpx

        self.client = httpx.Client(
            headers={"Authorization": "Bearer " + access_token, "Accept-Encoding": "identity"},
            trust_env=False, verify=True, follow_redirects=False,
            transport=transport if transport is not None else httpx.HTTPTransport(retries=0),
        )

    def request(self, method, url, *, params, timeout_seconds, **policy):
        if policy != {"follow_redirects": False, "retries": 0, "trust_env": False}:
            raise CaptureError("request_forbidden")
        context = self.client.stream(method, url, params=params, timeout=timeout_seconds)
        response = context.__enter__()

        class Chunks:
            closed = False

            def __iter__(self):
                return response.iter_raw()

            def close(self):
                if not self.closed:
                    self.closed = True
                    context.__exit__(None, None, None)

        return WireReply(response.status_code, Chunks(), response.headers.get("Content-Encoding", "identity"))

    def close(self):
        self.client.close()


def _read_metadata(path):
    _private_path(Path(path), file=True)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as handle:
        raw = handle.read(65537)
    if len(raw) > 65536:
        raise CaptureError("metadata_invalid")
    # Reuse strict duplicate-key/NaN validation without accepting a list as input.
    try:
        documents = _json(b"[" + raw + b"]")
        if len(documents) != 1:
            raise ValueError
        document = documents[0]
        if (type(document) is not dict or document.get("ok") is not True
                or document.get("projection") != "identity"
                or set(document) != {"ok", "projection", "target_id", "source_binding_id", "records"}):
            raise ValueError
        target, binding = document["target_id"], document["source_binding_id"]
        if (type(target) is not str or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,256}", target)
                or type(binding) is not str or not re.fullmatch(r"[0-9a-f]{64}", binding)):
            raise ValueError
        records = document["records"]
        if type(records) is not list or len(records) != 2:
            raise ValueError
        for row, date, time_value in zip(records, ["2026-10-15", "2026-10-16"], [FINANCIAL_START, FINANCIAL_END]):
            if (type(row) is not dict or row.get("observation_date") != date
                    or row.get("observed_finished_at") != time_value
                    or set(row) != {"observation_date", "observed_finished_at", "target_id_matches",
                                    "source_binding_id_matches", "account_scope_matches", "record_revision"}
                    or row.get("record_revision") is not None
                    or any(row.get(key) is not True for key in (
                        "target_id_matches", "source_binding_id_matches", "account_scope_matches"))):
                raise ValueError
    except (CaptureError, KeyError, TypeError, ValueError):
        raise CaptureError("metadata_invalid") from None
    return target, binding


def capture(*, metadata_path, output_dir, environ, token_loader=None, http_transport=None, now=None):
    """Private file provenance is an operator precondition, not established by equal IDs."""
    from scripts.publish_runtime_daily_from_reports import PROJECT_ID, TARGET, _select_identity
    from scripts.publish_account_facts_from_reports import _binding_id
    from scripts.schwab_native_identity import _access_token, _load_existing_token, SECRET_ID
    from application.external_flow_snapshot import collect_external_flow_snapshot

    transport = None
    try:
        if environ.get("GCP_PROJECT_ID") != PROJECT_ID:
            raise CaptureError("binding_invalid")
        expected_target, expected_binding = _read_metadata(metadata_path)
        account_hash, _policy = _select_identity(environ)
        actual_target = environ.get("SCHWAB_ACCOUNT_FACTS_TARGET_ID")
        binding = BindingInputs(account_hash, actual_target, _binding_id(account_hash, TARGET["service"]),
                                expected_target, expected_binding)
        _binding_matches(binding)
        api_key = environ.get("SCHWAB_API_KEY")
        if type(api_key) is not str or not api_key:
            raise CaptureError("auth_invalid")

        def builder(session):
            nonlocal transport
            from schwab.client import Client

            payload = (token_loader or _load_existing_token)(PROJECT_ID, SECRET_ID)
            access_token, _status = _access_token(payload, now=time.time() if now is None else now)
            if access_token is None:
                raise CaptureError("auth_invalid")
            transport = HttpxTransport(access_token, transport=http_transport)
            session.transport = transport
            return Client(api_key, session)

        return CaptureOnce(client_builder=builder, transport=None, collector=collect_external_flow_snapshot).run(
            binding=binding, output_dir=output_dir)
    except BaseException as error:
        code = error.code if type(error) is CaptureError else "binding_invalid"
        return CaptureResult(dict(status="blocked", reason=code, request_attempts=0,
                                  source_complete=False, authority="not_assessed"))
    finally:
        if transport is not None:
            with _private_call_output():
                try:
                    transport.close()
                except BaseException:
                    pass


def main(argv=None, *, environ=None):
    parser = argparse.ArgumentParser(add_help=False, exit_on_error=False)
    parser.add_argument("--binding-metadata", required=True)
    parser.add_argument("--output-dir", required=True)
    try:
        with _private_call_output():
            args = parser.parse_args(argv)
        result = capture(metadata_path=args.binding_metadata, output_dir=args.output_dir,
                         environ=os.environ if environ is None else environ)
    except BaseException:
        result = CaptureResult(dict(status="blocked", reason="arguments_invalid", source_complete=False))
    print(json.dumps(result.receipt, sort_keys=True))
    return 0 if result.receipt.get("status") == "captured_incomplete" else 2


class CaptureOnce:
    def __init__(self, *, client_builder, transport, collector, clock=None):
        self.builder, self.transport, self.collector = client_builder, transport, collector
        self.clock, self.used = clock or time.monotonic, False

    def run(self, *, binding, output_dir):
        receipt = dict(status="blocked", reason=None, http_status=None, request_attempts=0,
            raw_count=None, cap_hit=False, source_complete=False, coverage="incomplete",
            authority="not_assessed", raw_saved=False, target_id_matches=None, source_binding_id_matches=None)
        if self.used:
            receipt["reason"] = "request_budget_exhausted"
            return CaptureResult(receipt)
        self.used = True
        session, directory_ready = None, False
        try:
            _binding_matches(binding)
            receipt.update(target_id_matches=True, source_binding_id_matches=True)
            out = Path(output_dir)
            _prepare_output(out)
            directory_ready = True
            _write_private(out / "capture.started", b"one-shot reserved; no automatic retry\n")
            with _private_call_output(), _deadline_guard() as deadline:
                session = _OneShotSession(self.transport, binding.account_hash, self.clock)
                deadline[0] = session
                try:
                    client = self.builder(session)
                except CaptureError:
                    raise
                except BaseException:
                    raise CaptureError("client_error") from None
                try:
                    snapshot = self.collector(client, account_hash=binding.account_hash,
                        period_start=FINANCIAL_START, period_end=FINANCIAL_END,
                        currency="USD", enabled=True, source_complete=False)
                except CaptureError:
                    raise
                except BaseException:
                    raise CaptureError("collector_error") from None
            if session.fatal:
                raise CaptureError(session.fatal)
            if session.calls != 1 or type(snapshot) is not dict or snapshot.get("source_complete") is not False:
                raise CaptureError("collector_contract")
            receipt["status"] = "captured_incomplete"
        except CaptureError as error:
            receipt["reason"] = error.code
        except BaseException:
            receipt["reason"] = "collector_error"
        if session is not None:
            receipt.update(http_status=session.status, request_attempts=session.calls,
                raw_count=len(session.rows) if session.rows is not None else None,
                cap_hit=session.rows is not None and len(session.rows) >= 3000)
            precision, boundary = (None, None) if receipt["cap_hit"] else _profiles(session.rows)
            receipt.update(precision_counts=precision, boundary_counts=boundary)
        if directory_ready:
            try:
                private = dict(receipt)
                if session is not None and session.body is not None:
                    _write_private(out / "response.body", session.body)
                    receipt["raw_saved"] = True
                    private.update(raw_saved=True, raw_bytes=len(session.body),
                                   raw_sha256=hashlib.sha256(session.body).hexdigest())
                _write_private(out / "receipt.json", (json.dumps(private, sort_keys=True) + "\n").encode())
            except BaseException:
                receipt.update(status="blocked", reason="private_save_failed")
        return CaptureResult(receipt)
