<#
.SYNOPSIS
    Reproducible Pyright baseline diagnostic and optional main-vs-feature compare.

.DESCRIPTION
    P1 (sections 7.3-7.4): make Pyright results reproducible and auditable.

    This script is NOT run on every CI invocation. ``scripts/check_quality.ps1``
    keeps the per-CI gate as a plain ``python -m pyright`` call. This script is
    for manual acceptance verification: it pins the environment, emits a stable
    JSON diagnostic, and (when ``-CompareTo`` is supplied) produces a
    main-vs-feature diff keyed on (severity, rule, normalized_relative_file,
    message) so that "all errors are pre-existing" can be backed by evidence
    rather than asserted.

    Usage (acceptance):

        # 1. Capture baseline on main
        git checkout main
        python -m pip install -e ".[dev]"
        pwsh scripts/check_pyright_baseline.ps1 -OutFile baseline-main.json

        # 2. Capture baseline on feature
        git checkout feature/derived-pipeline-refactor
        python -m pip install -e ".[dev]"
        pwsh scripts/check_pyright_baseline.ps1 -OutFile baseline-feature.json

        # 3. Compare
        pwsh scripts/check_pyright_baseline.ps1 -CompareTo baseline-main.json -OutFile baseline-feature.json

    The script does NOT mutate the working tree, install dependencies, or check
    out branches. It only runs ``python -m pyright --outputjson`` and writes a
    diagnostic envelope. The caller is responsible for ensuring the same venv,
    same pinned pyright (see pyproject.toml ``[project.optional-dependencies]
    dev``), and same Python interpreter are used for both captures.

.PARAMETER OutFile
    Path to write the diagnostic envelope (JSON). Defaults to
    ``.pyright_baseline.json`` in the repository root. When ``-CompareTo`` is
    also supplied, this file is the FEATURE capture.

.PARAMETER CompareTo
    Optional path to a previously captured baseline envelope (typically main).
    When supplied, the script runs pyright on the current tree, writes the
    current envelope to ``-OutFile``, and prints a main-vs-feature diff:
    new errors, resolved errors, unchanged diagnostics. Exit code is non-zero
    when the feature introduces ANY new error (warnings are reported but do
    not fail).

.PARAMETER PyrightArgs
    Extra arguments forwarded to ``python -m pyright``. Defaults to none.

.EXAMPLE
    pwsh scripts/check_pyright_baseline.ps1
    # Captures .pyright_baseline.json and prints a summary.

.EXAMPLE
    pwsh scripts/check_pyright_baseline.ps1 -CompareTo .\baseline-main.json -OutFile .\baseline-feature.json
    # Compares current tree against main baseline; exits non-zero on new errors.
#>

[CmdletBinding()]
param(
    [string] $OutFile = ".pyright_baseline.json",
    [string] $CompareTo = "",
    [string[]] $PyrightArgs = @()
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = (Resolve-Path "$PSScriptRoot/..").Path
Set-Location $repoRoot

# Fail fast: pyproject.toml is the SOLE Pyright configuration authority. A
# stray pyrightconfig.json at the repository root would override pyproject.toml
# (Pyright prefers pyrightconfig.json when both exist), re-creating local vs
# CI divergence and making baseline comparisons meaningless. Behaviour is
# identical to scripts/check_quality.ps1 so the baseline and the regular
# quality gate cannot disagree. See section 8.3 of the PR-2 acceptance plan.
$unexpectedConfig = Join-Path $repoRoot "pyrightconfig.json"
if (Test-Path $unexpectedConfig) {
    Write-Error "Unexpected repository-root pyrightconfig.json found. Remove it; pyproject.toml is the authoritative Pyright configuration."
    exit 13
}

function Test-PyrightAvailable {
    param([string] $PythonExe)
    try {
        & $PythonExe -m pyright --version 2>$null | Out-Null
        return $LASTEXITCODE -eq 0
    } catch {
        return $false
    }
}

# Capture the Python executable ONCE. All subsequent pyright/version calls use
# this exact path instead of re-resolving ``python`` from the PATH. This
# guarantees the script uses the same interpreter it identified at the start,
# even if the PATH changes in a subprocess or ``python`` would resolve to a
# different executable in a child shell.
$pythonExecutable = (Get-Command python).Source
if (-not $pythonExecutable) {
    Write-Error "python not found on PATH. Activate the project virtual environment first."
    exit 12
}

if (-not (Test-PyrightAvailable -PythonExe $pythonExecutable)) {
    Write-Error "pyright is not installed in the current interpreter. Run: python -m pip install -e `".[dev]`""
    exit 2
}

# Capture environment facts required by section 7.3.
$pythonVersion = (& $pythonExecutable -c "import sys; print(sys.version.split()[0])").Trim()
$pyrightVersion = (& $pythonExecutable -m pyright --version).Trim()
$repositorySha = (git rev-parse HEAD).Trim()
$configPath = (Resolve-Path "$repoRoot/pyproject.toml").Path

Write-Host "== Pyright baseline capture =="
Write-Host "python_executable : $pythonExecutable"
Write-Host "python_version    : $pythonVersion"
Write-Host "pyright_version   : $pyrightVersion"
Write-Host "repository_sha    : $repositorySha"
Write-Host "config_path       : $configPath"
Write-Host ""

# Run pyright --outputjson. We deliberately ignore its exit code (non-zero when
# any diagnostic is emitted) and read the diagnostic counts from the JSON.
# Use temp files for both stdout and stderr: PowerShell's `2>$null` can
# silently corrupt the stdout stream when the JSON is large, producing an empty
# generalDiagnostics array. Redirecting stderr to a temp file (and discarding
# it) avoids this while still suppressing pyright's human-readable progress
# output. Temp files are placed in the repo root (not [System.IO.Path]::GetTempFileName())
# to avoid non-ASCII characters in the user profile path breaking redirection.
$pyrightTempOut = Join-Path $repoRoot ".pyright-baseline-stdout.tmp"
$pyrightTempErr = Join-Path $repoRoot ".pyright-baseline-stderr.tmp"
try {
    & $pythonExecutable @("-m", "pyright", "--outputjson") $PyrightArgs > $pyrightTempOut 2> $pyrightTempErr
    $rawJson = Get-Content $pyrightTempOut -Raw -Encoding UTF8
} finally {
    if (Test-Path $pyrightTempOut) { Remove-Item $pyrightTempOut -Force -ErrorAction SilentlyContinue }
    if (Test-Path $pyrightTempErr) { Remove-Item $pyrightTempErr -Force -ErrorAction SilentlyContinue }
}
if ([string]::IsNullOrWhiteSpace($rawJson)) {
    Write-Error "pyright produced no JSON output. Re-run with: python -m pyright --outputjson"
    exit 3
}

$report = $rawJson | ConvertFrom-Json

# Normalize diagnostics into a stable key per section 7.4.
# Primary key (strict): severity, rule, normalized_relative_file, range.start.line, range.start.character, message
# Secondary key (loose): severity, rule, normalized_relative_file, message
# We emit both so line drift between branches does not erase evidence.
function ConvertTo-NormalizedRelative {
    param([string] $AbsPath)
    if ([string]::IsNullOrEmpty($AbsPath)) { return "" }
    try {
        $resolved = (Resolve-Path $AbsPath -ErrorAction SilentlyContinue).Path
        if (-not $resolved) { $resolved = $AbsPath }
        $rel = $resolved.Substring($repoRoot.Length).TrimStart('\', '/')
        return ($rel -replace '\\', '/')
    } catch {
        return ($AbsPath -replace '\\', '/')
    }
}

$diagnostics = @()
if ($report.generalDiagnostics) {
    foreach ($d in $report.generalDiagnostics) {
        $rel = ConvertTo-NormalizedRelative $d.file
        $startLine = if ($d.range.start.line) { [int]$d.range.start.line } else { -1 }
        $startChar = if ($d.range.start.character) { [int]$d.range.start.character } else { -1 }
        $rule = if ($d.rule) { $d.rule } else { "" }
        $message = if ($d.message) { $d.message } else { "" }
        $diagnostics += [pscustomobject]@{
            severity               = $d.severity
            rule                   = $rule
            normalized_relative_file = $rel
            start_line             = $startLine
            start_character        = $startChar
            message                = $message
            strict_key             = "$($d.severity)|$rule|$rel|$startLine|$startChar|$message"
            loose_key              = "$($d.severity)|$rule|$rel|$message"
        }
    }
}

# Wrap in @(...) so the result is always an array even when Where-Object
# returns $null (no matching diagnostics) or a single object. Without this,
# Set-StrictMode -Version Latest throws "The property 'Count' cannot be found
# on this object" when there are zero errors or zero warnings.
$errorCount = @($diagnostics | Where-Object { $_.severity -eq "error" }).Count
$warningCount = @($diagnostics | Where-Object { $_.severity -eq "warning" }).Count

# Resolve the output file to an ABSOLUTE path BEFORE writing. The previous
# implementation used ``Resolve-Path $OutFile -ErrorAction SilentlyContinue``
# which returns $null when the file does not yet exist (first run), leaving
# ``result_json_path`` empty in the JSON envelope. Use
# ``[System.IO.Path]::GetFullPath`` instead — it computes the absolute path
# purely from the string + CWD without requiring the file to exist. Handle
# relative paths, absolute paths, and the current directory uniformly. Create
# the parent directory if it does not exist so first-run captures do not fail
# on a missing output directory. See section 9 of the PR-2 acceptance plan.
if ([System.IO.Path]::IsPathRooted($OutFile)) {
    $resolvedOutFile = [System.IO.Path]::GetFullPath($OutFile)
} else {
    $resolvedOutFile = [System.IO.Path]::GetFullPath((Join-Path $repoRoot $OutFile))
}
$outDirectory = Split-Path $resolvedOutFile -Parent
if ($outDirectory -and -not (Test-Path $outDirectory)) {
    New-Item -ItemType Directory -Force -Path $outDirectory | Out-Null
}

$envelope = [pscustomobject]@{
    schema_version        = 1
    captured_at           = (Get-Date -Format "o")
    python_executable     = $pythonExecutable
    python_version        = $pythonVersion
    pyright_version       = $pyrightVersion
    repository_sha        = $repositorySha
    config_path           = $configPath
    errors                = $errorCount
    warnings              = $warningCount
    result_json_path      = $resolvedOutFile
    diagnostics           = $diagnostics
}

$envelope | ConvertTo-Json -Depth 10 | Set-Content -Path $resolvedOutFile -Encoding UTF8

Write-Host "errors            : $errorCount"
Write-Host "warnings          : $warningCount"
Write-Host "result_json_path  : $resolvedOutFile"
Write-Host ""

# Section 7.4: baseline comparison.
if (-not [string]::IsNullOrEmpty($CompareTo)) {
    if (-not (Test-Path $CompareTo)) {
        Write-Error "CompareTo baseline not found: $CompareTo"
        exit 4
    }
    $baseline = Get-Content $CompareTo -Raw | ConvertFrom-Json

    $baselineStrict = @{}
    foreach ($d in $baseline.diagnostics) { $baselineStrict[$d.strict_key] = $true }
    $baselineLoose = @{}
    foreach ($d in $baseline.diagnostics) { $baselineLoose[$d.loose_key] = $true }

    $newErrors = @()
    $resolvedErrors = @()
    $unchanged = @()
    $newWarnings = @()

    foreach ($d in $diagnostics) {
        if ($baselineStrict.ContainsKey($d.strict_key)) {
            $unchanged += $d
        } elseif ($baselineLoose.ContainsKey($d.loose_key)) {
            # Same severity+rule+file+message but different line: treat as
            # environment/line-drift, NOT a new error.
            $unchanged += $d
        } else {
            if ($d.severity -eq "error") { $newErrors += $d }
            else { $newWarnings += $d }
        }
    }

    $featureStrict = @{}
    foreach ($d in $diagnostics) { $featureStrict[$d.strict_key] = $true }
    $featureLoose = @{}
    foreach ($d in $diagnostics) { $featureLoose[$d.loose_key] = $true }

    foreach ($d in $baseline.diagnostics) {
        if ($d.severity -ne "error") { continue }
        if ($featureStrict.ContainsKey($d.strict_key)) { continue }
        if ($featureLoose.ContainsKey($d.loose_key)) { continue }
        $resolvedErrors += $d
    }

    Write-Host "== Baseline compare =="
    Write-Host "baseline_sha      : $($baseline.repository_sha)"
    Write-Host "baseline_errors   : $($baseline.errors)"
    Write-Host "baseline_warnings : $($baseline.warnings)"
    Write-Host "feature_errors    : $errorCount"
    Write-Host "feature_warnings  : $warningCount"
    Write-Host "new_errors        : $($newErrors.Count)"
    Write-Host "resolved_errors   : $($resolvedErrors.Count)"
    Write-Host "new_warnings      : $($newWarnings.Count)"
    Write-Host "unchanged         : $($unchanged.Count)"
    Write-Host ""

    if ($newErrors.Count -gt 0) {
        Write-Host "== NEW feature errors (must fix) =="
        foreach ($d in $newErrors) {
            Write-Host ("  [{0}] {1}:{2}  rule={3}  {4}" -f $d.severity, $d.normalized_relative_file, $d.start_line, $d.rule, $d.message)
        }
        Write-Host ""
        Write-Host "FAIL: feature introduces $($newErrors.Count) new pyright error(s)."
        exit 5
    }

    Write-Host "PASS: feature introduces 0 new pyright errors (warnings and unchanged diagnostics may still be present)."
    exit 0
}

Write-Host "Baseline captured. Re-run with -CompareTo <baseline.json> to diff against another capture."
exit 0
