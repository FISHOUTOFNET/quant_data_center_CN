from __future__ import annotations

import os

import pytest

from src.utils.network_policy import build_network_env, network_env


def test_build_network_env_direct_removes_proxy_vars_and_sets_profile() -> None:
    base_env = {
        "HTTP_PROXY": "http://proxy.example",
        "HTTPS_PROXY": "http://proxy.example",
        "ALL_PROXY": "socks://proxy.example",
        "FTP_PROXY": "http://proxy.example",
        "NO_PROXY": "localhost",
        "http_proxy": "http://proxy.example",
        "https_proxy": "http://proxy.example",
        "all_proxy": "socks://proxy.example",
        "ftp_proxy": "http://proxy.example",
        "no_proxy": "localhost",
        "KEEP": "1",
    }

    env = build_network_env(base_env, profile="direct")

    assert env["KEEP"] == "1"
    assert env["NO_PROXY"] == "*"
    assert env["no_proxy"] == "*"
    assert env["QDC_NETWORK_PROFILE"] == "direct"
    assert "HTTP_PROXY" not in env
    assert "HTTPS_PROXY" not in env
    assert "ALL_PROXY" not in env
    assert "http_proxy" not in env
    assert "https_proxy" not in env
    assert "all_proxy" not in env


def test_build_network_env_inherit_keeps_proxy_vars_and_overwrites_profile() -> None:
    base_env = {
        "HTTP_PROXY": "http://proxy.example",
        "HTTPS_PROXY": "http://proxy.example",
        "ALL_PROXY": "socks://proxy.example",
        "QDC_NETWORK_PROFILE": "direct",
    }

    env = build_network_env(base_env, profile="inherit")

    assert env["HTTP_PROXY"] == "http://proxy.example"
    assert env["HTTPS_PROXY"] == "http://proxy.example"
    assert env["ALL_PROXY"] == "socks://proxy.example"
    assert env["QDC_NETWORK_PROFILE"] == "inherit"


def test_build_network_env_rejects_invalid_profile() -> None:
    with pytest.raises(ValueError, match=r"invalid.*allowed values|Invalid.*allowed values"):
        build_network_env({}, profile="invalid")


def test_network_env_direct_restores_original_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.example")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example")
    monkeypatch.setenv("KEEP", "1")
    original = dict(os.environ)

    with network_env("direct"):
        assert "HTTP_PROXY" not in os.environ
        assert "HTTPS_PROXY" not in os.environ
        assert os.environ["NO_PROXY"] == "*"
        assert os.environ["no_proxy"] == "*"
        assert os.environ["QDC_NETWORK_PROFILE"] == "direct"
        assert os.environ["KEEP"] == "1"

    assert dict(os.environ) == original


def test_network_env_nested_contexts_restore_outer_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.example")
    monkeypatch.setenv("QDC_NETWORK_PROFILE", "inherit")
    original = dict(os.environ)

    with network_env("direct"):
        outer = dict(os.environ)
        assert "HTTP_PROXY" not in os.environ
        assert os.environ["QDC_NETWORK_PROFILE"] == "direct"
        with network_env("inherit"):
            assert "HTTP_PROXY" not in os.environ
            assert os.environ["QDC_NETWORK_PROFILE"] == "inherit"
        assert dict(os.environ) == outer

    assert dict(os.environ) == original
