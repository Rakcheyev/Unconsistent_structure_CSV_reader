Param(
    [switch] $Dev = $false
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvPath = Join-Path $repoRoot ".venv"

if (-not (Test-Path $venvPath)) {
    Write-Host "Creating virtual environment at $venvPath"
    python -m venv $venvPath
}

$pythonExe = Join-Path $venvPath "Scripts/python.exe"

& $pythonExe -m pip install --upgrade pip setuptools wheel

$installTarget = "."
if ($Dev) {
    $installTarget = ".[dev]"
}

Write-Host "Installing project ($installTarget)"
& $pythonExe -m pip install $installTarget

Write-Host "Environment ready. Activate with:`n`n    $venvPath\\Scripts\\Activate.ps1`n"
