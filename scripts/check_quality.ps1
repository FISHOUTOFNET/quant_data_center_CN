$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = (Resolve-Path "$PSScriptRoot/..").Path
Set-Location $repoRoot

# Fail fast: pyproject.toml is the SOLE Pyright configuration authority. A
# stray pyrightconfig.json at the repository root would override pyproject.toml
# (Pyright prefers pyrightconfig.json when both exist), re-creating local vs
# CI divergence. Local interpreter issues must be solved via .venv activation,
# VS Code interpreter selection, or IDE user/workspace settings — never via an
# untracked pyrightconfig.json. See section 8 of the PR-2 acceptance plan.
$unexpectedConfig = Join-Path $repoRoot "pyrightconfig.json"
if (Test-Path $unexpectedConfig) {
    Write-Error "Unexpected repository-root pyrightconfig.json found. Remove it; pyproject.toml is the authoritative Pyright configuration."
    exit 13
}

function Invoke-QualityStep {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock] $Command
    )

    & $Command
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

Invoke-QualityStep { python -m ruff format --check . }
Invoke-QualityStep { python -m ruff check . }
Invoke-QualityStep { python -m pyright }
Invoke-QualityStep { python -m pytest -m "not performance" --cov=src --cov-report=term-missing }
