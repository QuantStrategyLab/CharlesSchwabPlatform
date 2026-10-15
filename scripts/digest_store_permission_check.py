#!/usr/bin/env python3
"""Read-only permission probe for the NOTIFY-04 digest-candidate store.

Uses the workflow's authenticated identity (gcloud access token) and only
calls read-only Cloud Storage APIs:

* ``testIamPermissions`` on the bucket (does not create/delete anything);
* ``buckets.get`` (public-access-prevention / uniform access), if allowed;
* ``buckets.getIamPolicy``, if allowed, to report whether ``allUsers`` /
  ``allAuthenticatedUsers`` appear (booleans only; no member list printed);
* ``objects.list`` under the prefix (object names / sizes / timestamps only)
  and a metadata-only ``objects.get`` on the first object (no content);
* an anonymous ``objects.list`` (expect 401/403 for a private bucket).

Prints a single JSON line of booleans / status codes. Never writes.
"""

from __future__ import annotations

import json
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request

BUCKET = os.environ.get("DIGEST_STORE_BUCKET", "qsl-runtime-logs-shared")
PREFIX = os.environ.get("DIGEST_STORE_PREFIX", "digest-candidates/")
PERMS = (
    "storage.objects.create",
    "storage.objects.delete",
    "storage.objects.get",
    "storage.objects.list",
    "storage.buckets.get",
    "storage.buckets.getIamPolicy",
)
API = "https://storage.googleapis.com/storage/v1/b/" + urllib.parse.quote(BUCKET, safe="")


def _get(url: str, token: str | None) -> tuple[int, dict]:
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return exc.code, {}
    except (urllib.error.URLError, TimeoutError, ValueError):
        return -1, {}


def main() -> int:
    token = subprocess.run(
        ["gcloud", "auth", "print-access-token"], check=True, capture_output=True, text=True
    ).stdout.strip()
    query = urllib.parse.urlencode([("permissions", p) for p in PERMS])
    code, body = _get(f"{API}/iam/testPermissions?{query}", token)
    granted = set(body.get("permissions") or [])
    result: dict[str, object] = {
        "bucket": BUCKET,
        "prefix": PREFIX,
        "test_permissions_http": code,
        **{p.replace("storage.", "").replace(".", "_"): (p in granted) for p in PERMS},
    }
    code, meta = _get(API, token)
    result["bucket_get_http"] = code
    if code == 200:
        iam = meta.get("iamConfiguration") or {}
        result["public_access_prevention"] = iam.get("publicAccessPrevention")
        result["uniform_bucket_level_access"] = bool((iam.get("uniformBucketLevelAccess") or {}).get("enabled"))
    code, policy = _get(f"{API}/iam?optionsRequestedPolicyVersion=3", token)
    result["get_iam_policy_http"] = code
    if code == 200:
        members = {m for b in policy.get("bindings") or [] for m in b.get("members") or []}
        result["policy_has_allUsers"] = "allUsers" in members
        result["policy_has_allAuthenticatedUsers"] = "allAuthenticatedUsers" in members
        result["policy_has_conditions"] = any(b.get("condition") for b in policy.get("bindings") or [])
    code, listing = _get(
        f"{API}/o?" + urllib.parse.urlencode({"prefix": PREFIX, "maxResults": 100, "fields": "items(name,size,updated)"}),
        token,
    )
    result["prefix_list_http"] = code
    items = listing.get("items") or [] if code == 200 else []
    result["prefix_has_objects"] = bool(items) if code == 200 else None
    # Object names/sizes/timestamps only (names carry platform/business_day/target, no values).
    result["prefix_objects"] = [
        {"name": item.get("name"), "size": int(item.get("size") or 0), "updated": item.get("updated")}
        for item in items
    ]
    if items:
        name = urllib.parse.quote(str(items[0].get("name")), safe="")
        # Metadata-only GET (alt=json default): proves objects.get without reading content.
        code, _meta = _get(f"{API}/o/{name}?fields=name,size", token)
        result["object_metadata_get_http"] = code
    anon_code, _ = _get(f"{API}/o?maxResults=1", None)
    result["anonymous_list_http"] = anon_code
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
