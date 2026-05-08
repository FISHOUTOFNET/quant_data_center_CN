#!/usr/bin/env python3
"""Codex PreToolUse hook that blocks dangerous git commands."""

import json
import re
import sys

DANGEROUS_PATTERNS = [
    r"\bgit\s+push\b",
    r"\bgit\s+reset\s+--hard\b",
    r"\bgit\s+clean\s+-f[d]?\b",
    r"\bgit\s+branch\s+-D\b",
    r"\bgit\s+checkout\s+\.\b",
    r"\bgit\s+restore\s+\.\b",
    r"\bpush\b.*\b--force\b",
    r"\breset\b.*\b--hard\b",
]


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        return 0

    command = str(payload.get("tool_input", {}).get("command", ""))
    for pattern in DANGEROUS_PATTERNS:
        if re.search(pattern, command):
            print(
                f"BLOCKED: {command!r} matches dangerous pattern {pattern!r}. "
                "The user has prevented Codex from doing this.",
                file=sys.stderr,
            )
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())