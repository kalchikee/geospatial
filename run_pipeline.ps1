# =============================================================================
# run_pipeline.ps1 — Run the Chicago NDVI Pipeline locally (no Docker)
# Usage:
#   .\run_pipeline.ps1                        # process current month
#   .\run_pipeline.ps1 -Month 2024-07         # process specific month
#   .\run_pipeline.ps1 -LoadParcels           # one-time parcel load
#   .\run_pipeline.ps1 -Month 2024-07 -DryRun # test ingest only
# =============================================================================

param(
    [string]$Month = "",
    [switch]$LoadParcels,
    [switch]$DryRun
)

$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$PIPELINE_DIR = "$SCRIPT_DIR\pipeline"
$ENV_FILE = "$SCRIPT_DIR\.env"
$PGBIN = "C:\Program Files\PostgreSQL\17\bin"

# --- Load .env file ---
if (Test-Path $ENV_FILE) {
    Get-Content $ENV_FILE | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
            $key = $matches[1].Trim()
            $val = $matches[2].Trim()
            if ($key -and $val) {
                [System.Environment]::SetEnvironmentVariable($key, $val, "Process")
            }
        }
    }
    Write-Host "[OK] Loaded .env" -ForegroundColor Green
} else {
    Write-Host "[ERROR] .env file not found at $ENV_FILE" -ForegroundColor Red
    exit 1
}

# --- Add PostgreSQL bin to PATH for raster2pgsql ---
$env:PATH = "$PGBIN;$env:PATH"

# --- Override SCRATCH_DIR to a guaranteed Windows path ---
if (-not $env:SCRATCH_DIR) {
    $env:SCRATCH_DIR = "C:\Users\$env:USERNAME\AppData\Local\Temp\chicago_ndvi_rasters"
}
New-Item -ItemType Directory -Force -Path $env:SCRATCH_DIR | Out-Null
Write-Host "[OK] Scratch dir: $env:SCRATCH_DIR" -ForegroundColor Green

# --- Build Python command ---
$pyArgs = @()

if ($LoadParcels) {
    $pyArgs += "--load-parcels"
    Write-Host "`n=== Loading Cook County Parcels (this takes ~5 min) ===" -ForegroundColor Cyan
}

if ($Month) {
    $pyArgs += "--month", $Month
    Write-Host "`n=== Processing NDVI for $Month ===" -ForegroundColor Cyan
} elseif (-not $LoadParcels) {
    # Default to last complete month
    $d = (Get-Date).AddMonths(-1)
    $Month = $d.ToString("yyyy-MM")
    $pyArgs += "--month", $Month
    Write-Host "`n=== Processing NDVI for $Month (last complete month) ===" -ForegroundColor Cyan
}

if ($DryRun) {
    $pyArgs += "--dry-run"
    Write-Host "[DRY RUN — will not process or load data]" -ForegroundColor Yellow
}

# --- Run pipeline ---
Write-Host ""
Push-Location $PIPELINE_DIR
python pipeline.py @pyArgs
$exitCode = $LASTEXITCODE
Pop-Location

if ($exitCode -eq 0) {
    Write-Host "`n=== Pipeline finished successfully! ===" -ForegroundColor Cyan
    if (-not $LoadParcels -and $Month) {
        Write-Host "View results: open docs\index.html with Live Server" -ForegroundColor Yellow
    }
} else {
    Write-Host "`n[ERROR] Pipeline exited with code $exitCode" -ForegroundColor Red
}
