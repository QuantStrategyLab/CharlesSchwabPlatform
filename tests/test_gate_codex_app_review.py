from __future__ import annotations

from scripts.gate_codex_app_review import check_metadata


def test_metadata_allows_removing_legacy_dependency_manifests() -> None:
    files = [
        {"filename": "requirements.txt", "status": "removed"},
        {"filename": "constraints.txt", "status": "removed"},
    ]

    assert check_metadata(files, {}) == []


def test_metadata_still_blocks_other_deleted_files() -> None:
    files = [{"filename": "main.py", "status": "removed"}]

    assert check_metadata(files, {}) == ["**File deleted**: `main.py` — verify intentional"]
