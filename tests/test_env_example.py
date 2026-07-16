from __future__ import annotations

from pathlib import Path
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[1]
ENV_EXAMPLE = ROOT / ".env.example"


def _env_value(name: str) -> str:
    prefix = f"{name}="
    matches = [
        line.removeprefix(prefix)
        for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines()
        if line.startswith(prefix)
    ]
    assert len(matches) == 1
    return matches[0]


def test_execution_report_gcs_uri_uses_public_placeholder() -> None:
    value = _env_value("EXECUTION_REPORT_GCS_URI")

    assert value == "gs://your-bucket/execution-reports"
    parsed = urlsplit(value)
    assert parsed.scheme == "gs"
    assert parsed.netloc == "your-bucket"
    assert parsed.path == "/execution-reports"
    assert not parsed.query
    assert not parsed.fragment
