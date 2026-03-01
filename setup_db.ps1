# =============================================================================
# setup_db.ps1 — One-time local database setup (no Docker needed)
# Run this once before using the pipeline.
# =============================================================================

$PGBIN = "C:\Program Files\PostgreSQL\17\bin"
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host ""
Write-Host "=== Chicago NDVI Pipeline — Local Database Setup ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "This will create:" -ForegroundColor Yellow
Write-Host "  - User:     ndvi_user"
Write-Host "  - Database: chicago_ndvi"
Write-Host "  - Extensions: postgis, postgis_raster, btree_gist"
Write-Host ""
Write-Host "Enter your PostgreSQL superuser (postgres) password:" -ForegroundColor Yellow
$pgPass = Read-Host -AsSecureString
$pgPassPlain = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($pgPass)
)

$env:PGPASSWORD = $pgPassPlain
$env:PGBIN = $PGBIN

function Run-SQL($db, $sql) {
    $result = & "$PGBIN\psql.exe" -U postgres -d $db -c $sql 2>&1
    Write-Host $result
}

function Run-SQL-File($db, $file) {
    $result = & "$PGBIN\psql.exe" -U postgres -d $db -f $file 2>&1
    Write-Host $result
}

# --- Create user (ignore error if already exists) ---
Write-Host "`n[1/5] Creating database user..." -ForegroundColor Green
Run-SQL "postgres" "CREATE USER ndvi_user WITH PASSWORD 'changeme_strong_password';"

# --- Create database ---
Write-Host "`n[2/5] Creating database..." -ForegroundColor Green
Run-SQL "postgres" "CREATE DATABASE chicago_ndvi OWNER ndvi_user;"

# --- Extensions ---
Write-Host "`n[3/5] Installing PostGIS extensions..." -ForegroundColor Green
Run-SQL "chicago_ndvi" "CREATE EXTENSION IF NOT EXISTS postgis;"
Run-SQL "chicago_ndvi" "CREATE EXTENSION IF NOT EXISTS postgis_raster;"
Run-SQL "chicago_ndvi" "CREATE EXTENSION IF NOT EXISTS btree_gist;"
Run-SQL "chicago_ndvi" "ALTER DATABASE chicago_ndvi SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';"

# --- Schema ---
Write-Host "`n[4/5] Creating tables and schema..." -ForegroundColor Green
Run-SQL-File "chicago_ndvi" "$SCRIPT_DIR\db\init\02_schema.sql"

# --- Indexes ---
Write-Host "`n[5/5] Creating indexes..." -ForegroundColor Green
Run-SQL-File "chicago_ndvi" "$SCRIPT_DIR\db\init\03_indexes.sql"

# --- Grant permissions ---
Run-SQL "chicago_ndvi" "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ndvi_user;"
Run-SQL "chicago_ndvi" "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ndvi_user;"
Run-SQL "chicago_ndvi" "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO ndvi_user;"

# Verify
Write-Host "`n[OK] Verifying PostGIS version..." -ForegroundColor Green
Run-SQL "chicago_ndvi" "SELECT PostGIS_Version();"

Write-Host "`n=== Database setup complete! ===" -ForegroundColor Cyan
Write-Host "Now run: .\run_pipeline.ps1" -ForegroundColor Yellow

$env:PGPASSWORD = ""
