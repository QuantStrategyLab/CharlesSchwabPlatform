"""Synthetic checks for one-shot, no-refresh Schwab account identity reads."""

import datetime as dt
import json
from unittest.mock import Mock

import pytest
from google.api_core import exceptions as api_exceptions
from google.auth import exceptions as auth_exceptions

from scripts import schwab_native_identity as identity


NOW = dt.datetime(2026, 10, 8, 12, tzinfo=dt.timezone.utc)
EXPECTED_HASH = "synthetic-account-hash"
ENV = {"GCP_PROJECT_ID": identity.PROJECT_ID}


@pytest.fixture
def standard_refresh_ports(monkeypatch, capsys):
    """Real Authlib/Schwab metadata; synthetic HTTP and Secret Manager only."""
    import socket
    import subprocess
    from types import SimpleNamespace

    import google.auth
    import httpx
    from google.cloud import secretmanager_v1

    state = SimpleNamespace(events=[], writes=[], requests=[], fault=None, denials=[],
                            rotated=True, fresh=False, bad_input=False, now=NOW, http_status=200)
    parent = "projects/401309731911/secrets/"
    original = "SYNTHETIC_REFRESH_SECRET"

    def deny(*_args, **_kwargs):
        state.denials.append("forbidden_external_capability")
        raise AssertionError("forbidden_external_capability")

    monkeypatch.setattr(socket, "socket", deny)
    monkeypatch.setattr(subprocess, "Popen", deny)
    monkeypatch.setattr(google.auth, "default", deny)
    monkeypatch.setattr(secretmanager_v1, "SecretManagerServiceClient", deny)

    class Secrets:
        def access_secret_version(self, *, request, retry, timeout):
            assert retry is None and timeout == 15
            name = request["name"]
            state.events.append(("read", name))
            if state.fault == "read":
                raise PermissionError("SYNTHETIC_REFRESH_SECRET")
            if name == parent + "schwab_token/versions/latest":
                token = json.loads(token_payload(expires_at=NOW.timestamp() + (600 if state.fresh else -1),
                                                 token_fields={"refresh_token": original}))
                token["creation_timestamp"] = state.now.timestamp() - 120
                token["token"]["expires_at"] = state.now.timestamp() + (600 if state.fresh else -1)
                if state.bad_input:
                    token["token"].pop("refresh_token")
                data = json.dumps(token).encode()
                return SimpleNamespace(name=parent + "schwab_token/versions/77",
                                       payload=SimpleNamespace(data=data))
            assert name in (parent + "charles-schwab-api-key/versions/1",
                            parent + "charles-schwab-app-secret/versions/1")
            fault = getattr(state, "credential_faults", {}).get(name)
            if isinstance(fault, BaseException):
                raise fault
            data = fault if fault is not None else b"SYNTHETIC_APP_CREDENTIAL"
            actual = getattr(state, "credential_references", {}).get(name, name)
            return SimpleNamespace(name=actual, payload=SimpleNamespace(data=data))

        def test_iam_permissions(self, *, request, retry, timeout):
            assert request == {"resource": parent + "schwab_token",
                               "permissions": ["secretmanager.versions.add"]}
            assert retry is None and timeout == 15
            state.events.append(("write_permission", request["resource"]))
            return SimpleNamespace(permissions=[] if state.fault == "no_write_permission"
                                   else ["secretmanager.versions.add"])

        def add_secret_version(self, *, request, retry, timeout):
            assert retry is None and timeout == 15
            assert request["parent"] == parent + "schwab_token"
            state.events.append(("persist", request["parent"]))
            state.writes.append(json.loads(request["payload"]["data"]))
            if state.fault == "persist":
                raise TimeoutError("SYNTHETIC_REFRESH_SECRET uncertain write")
            if state.fault in ("persist_interrupt", "persist_exit"):
                raise KeyboardInterrupt() if state.fault == "persist_interrupt" else SystemExit()
            return SimpleNamespace(name=parent + "schwab_token/versions/" + ("77" if state.fault == "write_ack" else "78"))

    def handle(request):
        from urllib.parse import parse_qs

        state.events.append(("post", str(request.url)))
        state.requests.append(request)
        assert request.method == "POST"
        assert str(request.url) == "https://api.schwabapi.com/v1/oauth/token"
        assert parse_qs(request.content.decode()) == {
            "grant_type": ["refresh_token"], "refresh_token": [original],
        }
        assert request.headers["authorization"].startswith("Basic ")
        assert request.headers["accept-encoding"] == "identity"
        if state.fault == "network":
            raise httpx.ReadTimeout("SYNTHETIC_REFRESH_SECRET unknown result")
        if state.fault in ("interrupt", "exit"):
            raise KeyboardInterrupt() if state.fault == "interrupt" else SystemExit()
        if state.fault in ("invalid_grant", "invalid_client"):
            return httpx.Response(state.http_status if state.http_status != 200 else 400,
                                  json={"error": state.fault, "error_description": original})
        if state.fault == "redirect":
            return httpx.Response(302, headers={"location": "https://not-authorized.invalid/"})
        if state.fault == "encoding":
            return httpx.Response(200, headers={"content-encoding": "gzip"}, content=b"private")
        if state.fault == "oversize":
            return httpx.Response(200, content=b"X" * (64 * 1024 + 1))
        body = {"access_token": "SYNTHETIC_NEW_ACCESS", "token_type": "Bearer",
                "expires_at": state.now.timestamp() + 600}
        if state.rotated:
            body["refresh_token"] = "SYNTHETIC_NEW_REFRESH"
        if state.fault == "response":
            body.pop("access_token")
        if state.fault == "noisy":
            import logging
            import sys
            print(original)
            print(original, file=sys.stderr)
            logging.warning(original)
        return httpx.Response(state.http_status, json=body, headers={
            "location": "https://not-authorized.invalid/"} if state.http_status == 302 else {})

    def run(environ=None):
        result = identity.renew_access_token_once(
            environ={**ENV, "SCHWAB_RENEW_ACCESS_TOKEN_ONCE": "true"} if environ is None else environ,
            observed_at=state.now, secret_client=Secrets(), http_transport=httpx.MockTransport(handle),
        )
        captured = capsys.readouterr()
        assert captured.out == captured.err == ""
        assert state.denials == []
        assert "SYNTHETIC" not in json.dumps(result)
        return result

    state.run = run
    state.secret_factory = Secrets
    state.transport = httpx.MockTransport(handle)
    return state


def test_readonly_renewal_credentials_never_reads_token_or_consumes_grant(standard_refresh_ports):
    state = standard_refresh_ports
    result = identity.check_renewal_credentials(
        environ={**ENV, "SCHWAB_CHECK_RENEWAL_CREDENTIALS": "true"}, secret_client=state.secret_factory())
    assert result == {"status": "ready", "reason": "credentials_ready", "api_key_status": "ready",
                      "app_secret_status": "ready", "refresh_attempts": 0, "secret_write_attempts": 0}
    assert state.events == [("read", "projects/401309731911/secrets/" + name + "/versions/1")
                            for name in ("charles-schwab-api-key", "charles-schwab-app-secret")]
    assert state.writes == state.requests == state.denials == []


@pytest.mark.parametrize("environ", [ENV, {**ENV, "SCHWAB_CHECK_RENEWAL_CREDENTIALS": "false"},
                                      {"GCP_PROJECT_ID": "wrong", "SCHWAB_CHECK_RENEWAL_CREDENTIALS": "true"}])
def test_renewal_credential_check_is_default_off_and_configuration_bound(standard_refresh_ports, environ):
    state = standard_refresh_ports
    result = identity.check_renewal_credentials(environ=environ)
    assert result["status"] == "blocked" and result["refresh_attempts"] == result["secret_write_attempts"] == 0
    assert state.events == state.denials == []


@pytest.mark.parametrize("field,name", [("api_key", "charles-schwab-api-key"), ("app_secret", "charles-schwab-app-secret")])
@pytest.mark.parametrize("fault,reason", [("permission", "permission_denied"), ("missing", "not_found"),
    ("disabled", "version_unavailable"), ("newline", "payload_invalid"), ("unicode", "payload_invalid"),
    ("empty", "payload_invalid"), ("oversize", "payload_invalid"), ("reference", "reference_mismatch")])
def test_renewal_credential_failures_are_specific_and_readonly(standard_refresh_ports, field, name, fault, reason):
    from google.api_core.exceptions import FailedPrecondition, NotFound, PermissionDenied

    state = standard_refresh_ports
    ref = "projects/401309731911/secrets/" + name + "/versions/1"
    if fault == "reference":
        state.credential_references = {ref: ref[:-1] + "2"}
    else:
        state.credential_faults = {ref: {"permission": PermissionDenied("SYNTHETIC_PRIVATE_ERROR"),
            "missing": NotFound("SYNTHETIC_PRIVATE_ERROR"), "disabled": FailedPrecondition("SYNTHETIC_PRIVATE_ERROR"),
            "newline": b"SYNTHETIC_PRIVATE_VALUE\n", "unicode": b"\xff", "empty": b"", "oversize": b"x" * 8193}[fault]}
    result = identity.check_renewal_credentials(
        environ={**ENV, "SCHWAB_CHECK_RENEWAL_CREDENTIALS": "true"}, secret_client=state.secret_factory())
    assert result["status"] == "blocked" and result["reason"] == "credentials_unavailable"
    assert result[field + "_status"] == reason
    other = "app_secret" if field == "api_key" else "api_key"
    assert result[other + "_status"] == "ready"
    assert result["refresh_attempts"] == result["secret_write_attempts"] == 0
    assert state.writes == state.requests == state.denials == []
    assert len(state.events) == 2
    assert "SYNTHETIC" not in json.dumps(result)


def test_renewal_readiness_cli_does_not_call_renewal(standard_refresh_ports, monkeypatch, capsys):
    state = standard_refresh_ports
    from google.cloud import secretmanager_v1

    monkeypatch.setenv("GCP_PROJECT_ID", ENV["GCP_PROJECT_ID"])
    monkeypatch.setenv("SCHWAB_CHECK_RENEWAL_CREDENTIALS", "true")
    monkeypatch.setattr(secretmanager_v1, "SecretManagerServiceClient", state.secret_factory)
    monkeypatch.setattr(identity, "renew_access_token_once", lambda **_: pytest.fail("grant path forbidden"))
    assert identity.main(["--check-renewal-credentials"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "ready"
    assert result["refresh_attempts"] == result["secret_write_attempts"] == 0
    assert state.writes == state.requests == state.denials == []


def test_renewal_uses_same_credential_diagnosis_before_any_post(standard_refresh_ports):
    from google.api_core.exceptions import PermissionDenied

    state = standard_refresh_ports
    state.credential_faults = {"projects/401309731911/secrets/charles-schwab-app-secret/versions/1":
                              PermissionDenied("SYNTHETIC_PRIVATE_ERROR")}
    result = state.run()
    assert result["reason"] == "app_secret_permission_denied"
    assert result["refresh_attempts"] == result["secret_write_attempts"] == 0
    assert state.requests == state.writes == []


def test_readiness_suppresses_sensitive_dependency_output(standard_refresh_ports, capsys):
    import logging
    import sys

    state = standard_refresh_ports

    class Noisy(state.secret_factory):
        def access_secret_version(self, **kwargs):
            print("SYNTHETIC_PRIVATE_VALUE")
            print("SYNTHETIC_PRIVATE_VALUE", file=sys.stderr)
            logging.error("SYNTHETIC_PRIVATE_VALUE")
            return super().access_secret_version(**kwargs)

    result = identity.check_renewal_credentials(
        environ={**ENV, "SCHWAB_CHECK_RENEWAL_CREDENTIALS": "true"}, secret_client=Noisy())
    captured = capsys.readouterr()
    assert result["status"] == "ready" and captured.out == captured.err == ""
    assert state.requests == state.writes == []


@pytest.mark.parametrize("rotated", [False, True])
def test_standard_refresh_uses_actual_oauth_grant_and_appends_whole_metadata(standard_refresh_ports, rotated):
    state = standard_refresh_ports
    state.rotated = rotated
    result = state.run()
    assert result == {"status": "renewed", "reason": "token_persisted",
                      "refresh_attempts": 1, "secret_write_attempts": 1,
                      "source_version": 77, "new_version": 78}
    assert [x[0] for x in state.events] == ["read", "write_permission", "read", "read", "post", "persist"]
    assert len(state.requests) == len(state.writes) == 1
    assert state.writes[0]["creation_timestamp"] == NOW.timestamp() - 120
    assert state.writes[0]["token"]["access_token"] == "SYNTHETIC_NEW_ACCESS"
    assert state.writes[0]["token"]["refresh_token"] == (
        "SYNTHETIC_NEW_REFRESH" if rotated else "SYNTHETIC_REFRESH_SECRET")


@pytest.mark.parametrize("environ", [{}, ENV, {**ENV, "SCHWAB_RENEW_ACCESS_TOKEN_ONCE": "false"}])
def test_standard_refresh_is_default_off_before_credentials(standard_refresh_ports, environ):
    state = standard_refresh_ports
    result = state.run(environ)
    assert result["reason"] == "renewal_not_enabled"
    assert result["refresh_attempts"] == result["secret_write_attempts"] == 0
    assert state.events == []


def test_standard_refresh_valid_cache_needs_no_credentials_post_or_write(standard_refresh_ports):
    state = standard_refresh_ports
    state.fresh = True
    result = state.run()
    assert result["status"] == "not_needed"
    assert result["refresh_attempts"] == result["secret_write_attempts"] == 0
    assert len(state.events) == 1 and state.writes == []


def test_standard_refresh_missing_refresh_credential_stops_before_post(standard_refresh_ports):
    state = standard_refresh_ports
    state.bad_input = True
    result = state.run()
    assert result["reason"] == "refresh_credential_missing"
    assert result["refresh_attempts"] == result["secret_write_attempts"] == 0
    assert len(state.events) == 1


@pytest.mark.parametrize("fault,reason,writes", [
    ("network", "refresh_outcome_unknown", 0),
    ("interrupt", "refresh_outcome_unknown", 0),
    ("exit", "refresh_outcome_unknown", 0),
    ("invalid_grant", "refresh_rejected", 0),
    ("invalid_client", "client_rejected", 0),
    ("redirect", "refresh_outcome_unknown", 0),
    ("encoding", "refresh_outcome_unknown", 0),
    ("oversize", "refresh_outcome_unknown", 0),
    ("response", "refresh_response_invalid", 0),
    ("persist", "persistence_outcome_unknown", 1),
    ("persist_interrupt", "persistence_outcome_unknown", 1),
    ("persist_exit", "persistence_outcome_unknown", 1),
    ("write_ack", "persistence_outcome_unknown", 1),
])
def test_standard_refresh_uncertain_results_stop_without_retry_or_login(standard_refresh_ports, fault, reason, writes):
    state = standard_refresh_ports
    state.fault = fault
    result = state.run()
    assert result["status"] == "blocked" and result["reason"] == reason
    assert result["refresh_attempts"] == 1
    assert result["secret_write_attempts"] == writes
    assert result["new_version"] is None
    assert len(state.requests) == 1 and len(state.writes) == writes


@pytest.mark.parametrize("status", [201, 302, 401, 503])
def test_standard_refresh_non_200_token_body_never_reaches_persistence(standard_refresh_ports, status):
    state = standard_refresh_ports
    state.http_status = status
    result = state.run()
    assert result["status"] == "blocked"
    assert result["reason"] == "refresh_outcome_unknown"
    assert result["refresh_attempts"] == len(state.requests) == 1
    assert result["secret_write_attempts"] == 0 and state.writes == []
    assert result["new_version"] is None
    assert [event[0] for event in state.events] == ["read", "write_permission", "read", "read", "post"]


@pytest.mark.parametrize("error,reason", [("invalid_grant", "refresh_rejected"),
                                         ("invalid_client", "client_rejected")])
def test_standard_refresh_http_401_oauth_error_stays_rejected(standard_refresh_ports, error, reason):
    state = standard_refresh_ports
    state.http_status = 401
    state.fault = error
    result = state.run()
    assert result["status"] == "blocked" and result["reason"] == reason
    assert result["refresh_attempts"] == len(state.requests) == 1
    assert result["secret_write_attempts"] == 0 and state.writes == []


def test_standard_refresh_suppresses_sensitive_dependency_output(standard_refresh_ports):
    state = standard_refresh_ports
    state.fault = "noisy"
    assert state.run()["status"] == "renewed"


@pytest.mark.parametrize("fault,reason", [("read", "secret_read_failed"),
                                         ("no_write_permission", "persistence_not_permitted")])
def test_standard_refresh_credential_or_write_preflight_failure_is_truly_not_attempted(
    standard_refresh_ports, fault, reason,
):
    state = standard_refresh_ports
    state.fault = fault
    result = state.run()
    assert result["reason"] == reason
    assert result["refresh_attempts"] == result["secret_write_attempts"] == 0
    assert state.requests == state.writes == []


def test_standard_refresh_exact_cli_uses_real_core_with_only_external_ports_replaced(
    standard_refresh_ports, monkeypatch, capsys,
):
    import httpx
    from google.cloud import secretmanager_v1

    state = standard_refresh_ports
    state.now = dt.datetime.now(dt.timezone.utc)
    monkeypatch.setenv("GCP_PROJECT_ID", identity.PROJECT_ID)
    monkeypatch.setenv("SCHWAB_RENEW_ACCESS_TOKEN_ONCE", "true")
    monkeypatch.setattr(secretmanager_v1, "SecretManagerServiceClient", state.secret_factory)

    def fake_transport(**options):
        assert options == {"retries": 0, "trust_env": False}
        return state.transport

    monkeypatch.setattr(httpx, "HTTPTransport", fake_transport)
    assert identity.main(["--renew-access-token-once"]) == 0
    captured = capsys.readouterr()
    receipt = json.loads(captured.out)
    assert captured.err == "" and "SYNTHETIC" not in captured.out
    assert receipt["status"] == "renewed" and receipt["new_version"] == 78
    assert len(state.requests) == len(state.writes) == 1 and state.denials == []


@pytest.mark.parametrize("argv", [[], ["SYNTHETIC_PRIVATE_ARGUMENT"]])
def test_standard_refresh_cli_rejects_other_arguments_without_echoing_them(
    standard_refresh_ports, argv, capsys,
):
    assert identity.main(argv) == 2
    captured = capsys.readouterr()
    assert json.loads(captured.out)["reason"] == "arguments_invalid"
    assert captured.err == "" and "SYNTHETIC" not in captured.out
    assert standard_refresh_ports.events == standard_refresh_ports.denials == []


@pytest.mark.parametrize("observed", [None, NOW.replace(tzinfo=None)])
def test_standard_refresh_invalid_clock_never_initializes_credentials(standard_refresh_ports, observed):
    state = standard_refresh_ports
    result = identity.renew_access_token_once(
        environ={**ENV, "SCHWAB_RENEW_ACCESS_TOKEN_ONCE": "true"}, observed_at=observed,
        secret_client=state.secret_factory(), http_transport=state.transport,
    )
    assert result["reason"] == "configuration_invalid"
    assert result["refresh_attempts"] == result["secret_write_attempts"] == 0
    assert state.events == state.denials == []


class Response:
    def __init__(self, payload=None, *, status=200, raw=None):
        self.status_code = status
        self.raw_body = json.dumps(payload).encode() if raw is None else raw
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.closed = True
        return False

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.raw_body), chunk_size):
            yield self.raw_body[offset : offset + chunk_size]


def token_payload(*, expires_at=None, token_fields=None):
    token = {
        "access_token": "SYNTHETIC-ACCESS-TOKEN",
        "expires_at": NOW.timestamp() + 600 if expires_at is None else expires_at,
    }
    if token_fields:
        token.update(token_fields)
    return json.dumps({"creation_timestamp": NOW.timestamp() - 120, "token": token})


def session_for(payload, *, status=200):
    session = Mock()
    session.get.return_value = Response(payload, status=status)
    return session


def verify(session, *, payload=None, expected_hash=EXPECTED_HASH, environ=None):
    secret_loader = Mock(return_value=token_payload() if payload is None else payload)
    result = identity.verify_native_identity(
        environ=ENV if environ is None else environ,
        expected_account_hash=expected_hash,
        observed_at=NOW,
        secret_loader=secret_loader,
        session=session,
    )
    return result, secret_loader


def test_reads_fixed_native_endpoint_once_and_matches_exact_selected_hash():
    response = Response(
        [
            {"accountNumber": "SYNTHETIC-ACCOUNT-1", "hashValue": "other-hash"},
            {"accountNumber": "SYNTHETIC-ACCOUNT-2", "hashValue": EXPECTED_HASH},
        ]
    )
    session = Mock()
    session.get.return_value = response

    result, loader = verify(session)

    assert result == "verified"
    loader.assert_called_once_with(identity.PROJECT_ID, identity.SECRET_ID)
    session.get.assert_called_once()
    assert session.get.call_args.args == (identity.API_URL,)
    assert session.get.call_args.kwargs["headers"] == {
        "Authorization": "Bearer SYNTHETIC-ACCESS-TOKEN"
    }
    assert session.get.call_args.kwargs["timeout"] == identity.REQUEST_TIMEOUT
    assert session.get.call_args.kwargs["allow_redirects"] is False
    assert session.get.call_args.kwargs["stream"] is True
    assert response.closed is True
    assert EXPECTED_HASH not in result


def test_native_hex_digest_case_matches_serving_contract_without_refresh():
    expected = "a1b2c3d4" * 8
    session = session_for([{"hashValue": expected.upper()}])
    result, loader = verify(session, expected_hash=expected)
    assert result == "verified"
    session.get.assert_called_once()
    loader.assert_called_once()


@pytest.mark.parametrize("expected,observed", [
    ("a" * 64, "B" + "A" * 63),
    ("a" * 63, "A" * 63), ("a" * 65, "A" * 65),
    ("g" + "a" * 63, "G" + "A" * 63),
    ("k" + "a" * 63, "\N{KELVIN SIGN}" + "A" * 63),
    (EXPECTED_HASH, EXPECTED_HASH.upper()),
    ("synthetic--account", "SYNTHETIC-ACCOUNT"),
])
def test_native_digest_matching_rejects_other_identity_changes(expected, observed):
    session = session_for([{"hashValue": observed}])
    result, _ = verify(session, expected_hash=expected)
    assert result == "native_identity_mismatch"
    session.get.assert_called_once()


@pytest.mark.parametrize("second", ["a1b2c3d4" * 8, ("a1b2c3d4" * 8).upper()])
def test_native_multiple_same_digest_accounts_remain_ambiguous(second):
    expected = "a1b2c3d4" * 8
    session = session_for([{"hashValue": expected}, {"hashValue": second}])
    result, _ = verify(session, expected_hash=expected)
    assert result == "native_identity_ambiguous"
    session.get.assert_called_once()


def test_owned_session_ignores_proxy_environment_and_closes(monkeypatch):
    import requests

    response = Response([{"hashValue": EXPECTED_HASH}])
    session = Mock()
    session.trust_env = True
    session.get.return_value = response
    monkeypatch.setattr(requests, "Session", lambda: session)

    result = identity.verify_native_identity(
        environ=ENV,
        expected_account_hash=EXPECTED_HASH,
        observed_at=NOW,
        secret_loader=Mock(return_value=token_payload()),
    )

    assert result == "verified"
    assert session.trust_env is False
    session.close.assert_called_once()
    assert response.closed is True


@pytest.mark.parametrize(
    ("status", "expected"),
    [(401, "token_rejected"), (403, "native_account_access_denied"), (302, "native_identity_unavailable")],
)
def test_non_success_response_is_fixed_and_never_retried(status, expected):
    session = session_for({"private": "DO-NOT-RETURN"}, status=status)

    result, _loader = verify(session)

    assert result == expected
    session.get.assert_called_once()
    assert "DO-NOT-RETURN" not in result


def test_expired_token_stops_before_network_without_refresh():
    session = Mock()

    result, loader = verify(
        session,
        payload=token_payload(expires_at=NOW.timestamp() - 1),
    )

    assert result == "token_expired"
    loader.assert_called_once()
    session.get.assert_not_called()


@pytest.mark.parametrize(
    "payload",
    [
        "not-json",
        json.dumps({"access_token": "SYNTHETIC-ACCESS-TOKEN"}),
        json.dumps(
            {
                "creation_timestamp": NOW.timestamp(),
                "token": {"access_token": "SYNTHETIC-ACCESS-TOKEN"},
            }
        ),
        token_payload(token_fields={"expires_at": True}),
        token_payload(token_fields={"access_token": "bad\nheader"}),
    ],
)
def test_invalid_token_shape_stops_before_network(payload):
    session = Mock()

    result, _loader = verify(session, payload=payload)

    assert result == "token_invalid"
    session.get.assert_not_called()


def test_secret_read_failure_is_fixed_and_stops_before_network():
    session = Mock()
    loader = Mock(side_effect=RuntimeError("SECRET-AND-ERROR-BODY"))

    result = identity.verify_native_identity(
        environ=ENV,
        expected_account_hash=EXPECTED_HASH,
        observed_at=NOW,
        secret_loader=loader,
        session=session,
    )

    assert result == "token_load_failed"
    assert "SECRET-AND-ERROR-BODY" not in result
    session.get.assert_not_called()


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (ImportError("PRIVATE"), "token_dependency_unavailable"),
        (auth_exceptions.DefaultCredentialsError("PRIVATE"), "token_adc_unavailable"),
        (api_exceptions.PermissionDenied("PRIVATE"), "token_permission_denied"),
        (api_exceptions.NotFound("PRIVATE"), "token_not_found"),
        (api_exceptions.FailedPrecondition("PRIVATE"), "token_version_unavailable"),
        (auth_exceptions.TransportError("PRIVATE"), "token_network_unavailable"),
        (api_exceptions.ServiceUnavailable("PRIVATE"), "token_network_unavailable"),
        (RuntimeError("PRIVATE"), "token_load_failed"),
    ],
)
def test_secret_loader_failures_have_fixed_type_based_classification(error, expected):
    session = Mock()
    loader = Mock(side_effect=error)

    result = identity.verify_native_identity(
        environ=ENV,
        expected_account_hash=EXPECTED_HASH,
        observed_at=NOW,
        secret_loader=loader,
        session=session,
    )

    assert result == expected
    assert "PRIVATE" not in result
    loader.assert_called_once_with(identity.PROJECT_ID, identity.SECRET_ID)
    session.get.assert_not_called()


def test_real_loader_uses_fixed_latest_and_disables_sdk_retry(monkeypatch):
    from google.cloud import secretmanager_v1

    client = Mock()
    client.access_secret_version.return_value.payload.data = b"SYNTHETIC-TOKEN"
    monkeypatch.setattr(
        secretmanager_v1, "SecretManagerServiceClient", lambda: client
    )

    assert identity._load_existing_token(identity.PROJECT_ID, identity.SECRET_ID) == (
        "SYNTHETIC-TOKEN"
    )
    client.access_secret_version.assert_called_once_with(
        request={
            "name": f"projects/{identity.PROJECT_ID}/secrets/{identity.SECRET_ID}/versions/latest"
        },
        retry=None,
        timeout=identity.SECRET_READ_TIMEOUT,
    )


def test_real_loader_rejects_oversized_secret_payload(monkeypatch):
    from google.cloud import secretmanager_v1

    client = Mock()
    client.access_secret_version.return_value.payload.data = b"x" * (
        identity.MAX_TOKEN_BYTES + 1
    )
    monkeypatch.setattr(
        secretmanager_v1, "SecretManagerServiceClient", lambda: client
    )

    assert identity._load_existing_token(identity.PROJECT_ID, identity.SECRET_ID) == ""
    client.access_secret_version.assert_called_once()


def test_token_load_status_checks_shape_and_expiry_without_broker_io():
    assert identity.check_token_load_status(
        environ=ENV,
        observed_at=NOW,
        secret_loader=Mock(return_value=token_payload()),
    ) == "token_payload_valid"
    assert identity.check_token_load_status(
        environ=ENV,
        observed_at=NOW,
        secret_loader=Mock(
            return_value=token_payload(expires_at=NOW.timestamp() - 1)
        ),
    ) == "token_expired"


def test_wrong_project_or_missing_exact_selector_does_not_read_secret():
    session = Mock()
    loader = Mock(return_value=token_payload())

    assert identity.verify_native_identity(
        environ={"GCP_PROJECT_ID": "other-project"},
        expected_account_hash=EXPECTED_HASH,
        observed_at=NOW,
        secret_loader=loader,
        session=session,
    ) == "configuration_invalid"
    assert identity.verify_native_identity(
        environ=ENV,
        expected_account_hash=" ",
        observed_at=NOW,
        secret_loader=loader,
        session=session,
    ) == "configuration_invalid"
    loader.assert_not_called()
    session.get.assert_not_called()


def test_no_exact_account_match_is_not_inferred_from_single_or_masked_account():
    session = session_for([{"accountNumber": "SYNTHETIC-ONLY", "hashValue": "other-hash"}])

    result, _loader = verify(session)

    assert result == "native_identity_mismatch"
    session.get.assert_called_once()


def test_duplicate_exact_hash_is_ambiguous_even_when_other_accounts_are_present():
    session = session_for(
        [
            {"accountNumber": "SYNTHETIC-1", "hashValue": EXPECTED_HASH},
            {"accountNumber": "SYNTHETIC-2", "hashValue": EXPECTED_HASH},
        ]
    )

    result, _loader = verify(session)

    assert result == "native_identity_ambiguous"


@pytest.mark.parametrize(
    "payload",
    [
        {"not": "a-list"},
        [],
        [{"accountNumber": "SYNTHETIC", "hashValue": ""}],
        [{"accountNumber": "SYNTHETIC", "hashValue": EXPECTED_HASH}, None],
    ],
)
def test_malformed_identity_response_is_not_partially_accepted(payload):
    session = session_for(payload)

    result, _loader = verify(session)

    assert result == "native_identity_response_invalid"


def test_oversized_or_invalid_json_response_is_not_accepted():
    session = Mock()
    session.get.return_value = Response(raw=b"x" * (identity.MAX_RESPONSE_BYTES + 1))

    result, _loader = verify(session)

    assert result == "native_identity_response_invalid"
