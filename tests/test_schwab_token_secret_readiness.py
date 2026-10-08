"""Synthetic tests for metadata-only Schwab Secret readiness diagnostics."""

import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from scripts import inspect_schwab_token_secret_readiness as readiness


ROOT = Path(__file__).resolve().parents[1]
ENV = {"GCP_PROJECT_ID": readiness.PROJECT_ID}


class Response:
    def __init__(self, payload=None, *, status=200, raw=None):
        self.status_code = status
        self.raw = json.dumps(payload or {}).encode() if raw is None else raw

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.raw), chunk_size):
            yield self.raw[offset : offset + chunk_size]


def checked_session(
    *, state="ENABLED", permissions=None, project_alias=None, version_project_alias=None
):
    project = project_alias or readiness.PROJECT_ID
    version_project = version_project_alias or project
    secret_name = f"projects/{project}/secrets/schwab_token"
    session = Mock()
    session.get.side_effect = [
        Response({"name": secret_name}),
        Response(
            {
                "name": f"projects/{version_project}/secrets/schwab_token/versions/7",
                "state": state,
            }
        ),
    ]
    session.post.return_value = Response({"permissions": permissions or []})
    return session


def test_checks_fixed_metadata_version_and_permission_only():
    session = checked_session(permissions=[readiness.PERMISSION])

    result = readiness.inspect_secret_readiness(ENV, session=session)

    assert result == {
        "status": "checked",
        "secret_exists": True,
        "latest_version_enabled": True,
        "permission_reported": True,
    }
    assert session.get.call_count == 2
    assert session.post.call_count == 1
    metadata_call, version_call = session.get.call_args_list
    assert metadata_call.args == (readiness.API_ROOT,)
    assert metadata_call.kwargs == {
        "json": None,
        "params": {"fields": "name"},
        "timeout": (5, 15),
        "stream": True,
        "allow_redirects": False,
    }
    assert version_call.args == (readiness.API_ROOT + "/versions/latest",)
    assert version_call.kwargs["params"] == {"fields": "name,state"}
    permission_call = session.post.call_args
    assert permission_call.args == (
        f"https://{readiness.API_HOST}/v1/projects/{readiness.PROJECT_ID}/secrets/schwab_token:testIamPermissions",
    )
    assert permission_call.kwargs["json"] == {
        "permissions": ["secretmanager.versions.access"]
    }
    assert permission_call.kwargs["allow_redirects"] is False
    assert "access" not in " ".join(call.args[0] for call in session.get.call_args_list)


def test_missing_secret_is_reported_without_followup_requests():
    session = Mock()
    session.get.return_value = Response(status=404)

    result = readiness.inspect_secret_readiness(ENV, session=session)

    assert result == {
        "status": "secret_not_found",
        "secret_exists": False,
        "latest_version_enabled": None,
        "permission_reported": None,
    }
    session.get.assert_called_once()
    session.post.assert_not_called()


@pytest.mark.parametrize(
    ("state", "permissions", "enabled", "allowed"),
    [("DISABLED", [readiness.PERMISSION], False, True), ("ENABLED", [], True, False)],
)
def test_version_state_and_access_permission_are_independent(
    state, permissions, enabled, allowed
):
    result = readiness.inspect_secret_readiness(
        ENV, session=checked_session(state=state, permissions=permissions)
    )

    assert result["status"] == "checked"
    assert result["latest_version_enabled"] is enabled
    assert result["permission_reported"] is allowed


def test_invalid_version_resource_stops_before_permission_test():
    session = Mock()
    session.get.side_effect = [
        Response({"name": f"projects/{readiness.PROJECT_ID}/secrets/schwab_token"}),
        Response(
            {
                "name": "projects/other/secrets/schwab_token/versions/7",
                "state": "ENABLED",
            }
        ),
    ]

    result = readiness.inspect_secret_readiness(ENV, session=session)

    assert result["status"] == "version_unavailable"
    assert result["secret_exists"] is True
    assert result["latest_version_enabled"] is None
    session.post.assert_not_called()


def test_official_secret_number_resource_shape_uses_secret_level_permission_endpoint():
    session = checked_session(
        permissions=[readiness.PERMISSION],
        project_alias=readiness.PROJECT_NUMBER,
    )

    result = readiness.inspect_secret_readiness(ENV, session=session)

    assert result == {
        "status": "checked",
        "secret_exists": True,
        "latest_version_enabled": True,
        "permission_reported": True,
    }
    assert session.get.call_args_list[1].args == (
        f"https://{readiness.API_HOST}/v1/projects/{readiness.PROJECT_NUMBER}/secrets/schwab_token/versions/latest",
    )
    assert session.post.call_args.args == (
        f"https://{readiness.API_HOST}/v1/projects/{readiness.PROJECT_NUMBER}/secrets/schwab_token:testIamPermissions",
    )


def test_canonical_project_number_version_is_accepted_after_project_id_metadata():
    session = checked_session(
        permissions=[readiness.PERMISSION],
        project_alias=readiness.PROJECT_ID,
        version_project_alias=readiness.PROJECT_NUMBER,
    )

    result = readiness.inspect_secret_readiness(ENV, session=session)

    assert result["latest_version_enabled"] is True
    assert result["permission_reported"] is True
    assert session.post.call_args.args == (
        f"https://{readiness.API_HOST}/v1/projects/{readiness.PROJECT_ID}/secrets/schwab_token:testIamPermissions",
    )


@pytest.mark.parametrize(
    "name",
    [
        "projects/other-project/secrets/schwab_token",
        f"projects/{readiness.PROJECT_ID}/secrets/other-secret",
    ],
)
def test_metadata_rejects_other_project_or_secret_name(name):
    session = Mock()
    session.get.return_value = Response({"name": name})

    result = readiness.inspect_secret_readiness(ENV, session=session)

    assert result["status"] == "metadata_unavailable"
    assert result["secret_exists"] is None
    session.get.assert_called_once()
    session.post.assert_not_called()


def test_unexpected_http_status_does_not_follow_redirect_or_expose_body():
    session = Mock()
    session.get.return_value = Response(
        {"private": "BODY-MUST-NOT-ESCAPE"}, status=302
    )

    result = readiness.inspect_secret_readiness(ENV, session=session)

    assert result["status"] == "metadata_unavailable"
    assert session.get.call_count == 1
    assert session.post.call_count == 0
    assert "BODY-MUST-NOT-ESCAPE" not in json.dumps(result)


def test_project_mismatch_fails_before_creating_adc_session(monkeypatch):
    monkeypatch.setattr(
        readiness,
        "_authorized_session",
        lambda: (_ for _ in ()).throw(AssertionError("ADC must not be created")),
    )
    result = readiness.inspect_secret_readiness({"GCP_PROJECT_ID": "other"})
    assert result["status"] == "configuration_invalid"


def test_transport_failure_is_fixed_and_does_not_expose_exception():
    session = Mock()
    session.get.side_effect = RuntimeError("PRIVATE-RESOURCE-AND-BODY")
    result = readiness.inspect_secret_readiness(ENV, session=session)
    assert result["status"] == "metadata_unavailable"
    assert "PRIVATE-RESOURCE-AND-BODY" not in json.dumps(result)
    session.post.assert_not_called()


def test_oversized_metadata_response_is_rejected_before_later_requests():
    session = Mock()
    session.get.return_value = Response(raw=b"x" * (readiness.MAX_RESPONSE_BYTES + 1))

    result = readiness.inspect_secret_readiness(ENV, session=session)

    assert result["status"] == "metadata_unavailable"
    assert session.get.call_count == 1
    session.post.assert_not_called()


def test_cli_prints_only_fixed_diagnostic_fields(monkeypatch, capsys):
    monkeypatch.setattr(
        readiness,
        "inspect_secret_readiness",
        lambda _environ: {
            "status": "checked",
            "secret_exists": True,
            "latest_version_enabled": True,
            "permission_reported": False,
        },
    )

    assert readiness.main(["--metadata-only"]) == 0
    output = capsys.readouterr().out
    assert output == (
        '{"latest_version_enabled": true, "permission_reported": false, '
        '"secret_exists": true, "status": "checked"}\n'
    )


def test_request_rejects_wrong_destination_and_method_before_network():
    session = Mock()
    with pytest.raises(ValueError):
        readiness._request_json(
            session,
            method="GET",
            url="https://example.invalid/secret",
        )
    with pytest.raises(ValueError):
        readiness._request_json(
            session,
            method="POST",
            url=readiness.API_ROOT,
            body={"payload": "not allowed"},
        )
    session.get.assert_not_called()
    session.post.assert_not_called()


def test_diagnostic_input_is_default_off_and_excludes_prepare_and_publish():
    workflow = (ROOT / ".github/workflows/runtime-daily-sync.yml").read_text()
    input_block = workflow.split("      diagnose_source_access:", 1)[1].split(
        "    permissions:", 1
    )[0]
    assert "type: boolean" in input_block
    assert "default: false" in input_block
    assert "github.event_name == 'workflow_dispatch' && github.ref == 'refs/heads/main'" in workflow
    assert "!inputs.publish && !inputs.diagnose_source_access" in workflow
    assert "inputs.diagnose_source_access && !inputs.publish" in workflow
    assert "inputs.publish && !inputs.diagnose_source_access" in workflow
    diagnostic = workflow.split(
        "- name: Check Schwab token Secret metadata and read permission", 1
    )[1].split("- name: Publish runtime daily", 1)[0]
    assert "inspect_schwab_token_secret_readiness.py --metadata-only" in diagnostic
    assert "EXECUTION_EVIDENCE_SYNC_TOKEN" not in diagnostic
    assert "SCHWAB_APP_SECRET" not in workflow
    assert "SCHWAB_API_KEY" not in workflow
