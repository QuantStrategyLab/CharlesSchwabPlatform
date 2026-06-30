#!/usr/bin/env python3
"""Check that all QPK git references match the canonical QPK_PIN.
Usage: python scripts/check_qpk_pin_consistency.py [--fix]
"""
import re, subprocess, sys
from pathlib import Path

QPK_PIN_URL = "https://raw.githubusercontent.com/QuantStrategyLab/QuantPlatformKit/main/QPK_PIN"
SHA_RE = re.compile(r"@([a-f0-9]{40})")

def fetch_pin() -> str:
    import urllib.request
    with urllib.request.urlopen(QPK_PIN_URL, timeout=10) as r:
        sha = r.read().decode().strip().split()[0]
    if len(sha) == 40: return sha
    raise RuntimeError(f"Invalid QPK_PIN: {sha}")

def main():
    fix = "--fix" in sys.argv
    target = fetch_pin()
    print(f"QPK_PIN: {target[:12]}...")
    errors = 0
    for path in sorted(Path.cwd().glob("**/requirements*.txt")) + sorted(Path.cwd().glob("**/pyproject.toml")):
        if "external" in str(path): continue
        content = path.read_text()
        for m in SHA_RE.finditer(content):
            sha = m.group(1)
            if "QuantPlatformKit" not in content[max(0,m.start()-200):m.end()]: continue
            if sha != target:
                errors += 1
                print(f"  ❌ {path}: QPK@{sha[:12]} (expected {target[:12]})")
                if fix:
                    path.write_text(content.replace(sha, target))
                    print("     → fixed")
    if errors:
        print(f"\n{errors} mismatch(es). Run with --fix to auto-fix.")
        return 1
    print("✅ All QPK pins match.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
