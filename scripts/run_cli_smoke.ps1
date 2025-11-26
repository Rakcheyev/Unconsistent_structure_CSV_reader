param(
    [string]$Dataset = "tests/data/retail_small.csv",
    [string]$MappingOutput = "artifacts/cli_smoke_mapping.json",
    [string]$SqliteDb = "artifacts/cli_smoke.db"
)

$ErrorActionPreference = "Stop"
$pathsToCleanup = @()

$scriptDir = Split-Path -Parent $PSCommandPath
$repoRoot = Split-Path -Parent $scriptDir

function Resolve-RepoPath {
    param(
        [string]$PathValue,
        [switch]$RequireExists
    )

    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        $candidate = $PathValue
    }
    else {
        $candidate = Join-Path -Path $repoRoot -ChildPath $PathValue
    }

    if ($RequireExists -and -not (Test-Path $candidate)) {
        throw "Path '$PathValue' not found (resolved to '$candidate')."
    }
    return $candidate
}

$datasetPath = Resolve-RepoPath -PathValue $Dataset -RequireExists
$mappingPath = Resolve-RepoPath -PathValue $MappingOutput
$sqlitePath = Resolve-RepoPath -PathValue $SqliteDb
$pathsToCleanup = @($mappingPath, $sqlitePath)
$venvPython = Join-Path -Path $repoRoot -ChildPath ".venv\Scripts\python.exe"
if (Test-Path $venvPython) {
    $python = $venvPython
} else {
    $python = "python"
}

# Ensure `common` package resolves even when editable install is missing
$srcPath = Join-Path $repoRoot "src"
if ($env:PYTHONPATH) {
    $env:PYTHONPATH = "$srcPath;$env:PYTHONPATH"
} else {
    $env:PYTHONPATH = $srcPath
}

try {
    & $python -m src.ui.cli analyze $datasetPath --output $mappingPath --sqlite-db $sqlitePath
}
finally {
    foreach ($path in $pathsToCleanup) {
        if (Test-Path $path) {
            Remove-Item $path -Force
        }
    }
}
