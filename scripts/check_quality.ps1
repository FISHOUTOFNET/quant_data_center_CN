$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

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
Invoke-QualityStep { python -m pytest --cov=src --cov-report=term-missing }
