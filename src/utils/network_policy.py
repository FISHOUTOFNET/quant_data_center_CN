from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from threading import RLock

NETWORK_PROFILE_DIRECT = "direct"
NETWORK_PROFILE_INHERIT = "inherit"
NETWORK_PROFILES = {NETWORK_PROFILE_DIRECT, NETWORK_PROFILE_INHERIT}

PROXY_ENV_VARS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "FTP_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "ftp_proxy",
    "no_proxy",
)

_ENV_LOCK = RLock()


def build_network_env(
    base_env: Mapping[str, str] | None = None,
    *,
    profile: str = NETWORK_PROFILE_DIRECT,
) -> dict[str, str]:
    if profile not in NETWORK_PROFILES:
        allowed = ", ".join(sorted(NETWORK_PROFILES))
        raise ValueError(f"Invalid network profile {profile!r}; allowed values: {allowed}")

    env = dict(base_env or os.environ)
    if profile == NETWORK_PROFILE_INHERIT:
        env["QDC_NETWORK_PROFILE"] = NETWORK_PROFILE_INHERIT
        return env

    for key in PROXY_ENV_VARS:
        env.pop(key, None)
    env["NO_PROXY"] = "*"
    env["no_proxy"] = "*"
    env["QDC_NETWORK_PROFILE"] = NETWORK_PROFILE_DIRECT
    return env


@contextmanager
def network_env(profile: str = NETWORK_PROFILE_DIRECT) -> Iterator[None]:
    with _ENV_LOCK:
        original_env = dict(os.environ)
        try:
            target_env = build_network_env(os.environ, profile=profile)
            os.environ.clear()
            os.environ.update(target_env)
            yield
        finally:
            os.environ.clear()
            os.environ.update(original_env)
