#!/usr/bin/env python3
"""Check that all QPK git references match the canonical QPK_PIN.
Usage: python scripts/check_qpk_pin_consistency.py [--fix]
"""
import re, sys
from pathlib import Path

QPK_PIN_URL = "https://raw.githubusercontent.com/QuantStrategyLab/QuantPlatformKit/main/QPK_PIN"
QPK_REF_RE = re.compile(r"QuantPlatformKit\.git@([a-f0-9]{40})")

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
    for path in sorted(Path.cwd().glob("**/requirements*.txt")) + sorted(Path.cwd().glob("**/constraints*.txt")) + sorted(Path.cwd().glob("**/pyproject.toml")):
        if "external" in str(path): continue
        content = path.read_text()
        updated = content
        for m in QPK_REF_RE.finditer(content):
            sha = m.group(1)
            if sha != target:
                errors += 1
                print(f"  ❌ {path}: QPK@{sha[:12]} (expected {target[:12]})")
                if fix:
                    updated = updated.replace(f"QuantPlatformKit.git@{sha}", f"QuantPlatformKit.git@{target}")
                    print("     → fixed")
        if fix and updated != content:
            path.write_text(updated)
    if errors:
        print(f"\n{errors} mismatch(es). Run with --fix to auto-fix.")
        return 1
    print("✅ All QPK pins match.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
