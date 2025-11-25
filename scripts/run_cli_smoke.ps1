param(
    [string]$Dataset = "tests/data/retail_small.csv",
    [string]$MappingOutput = "artifacts/cli_smoke_mapping.json",
    [string]$SqliteDb = "artifacts/cli_smoke.db"
)

$ErrorActionPreference = "Stop"
$pathsToCleanup = @($MappingOutput, $SqliteDb)

try {
    python -m src.ui.cli analyze $Dataset --output $MappingOutput --sqlite-db $SqliteDb
}
finally {
    foreach ($path in $pathsToCleanup) {
        if (Test-Path $path) {
            Remove-Item $path -Force
        }
    }
}
