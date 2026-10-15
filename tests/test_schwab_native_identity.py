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
