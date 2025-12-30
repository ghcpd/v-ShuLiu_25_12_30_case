# Test runner for Windows PowerShell

# Run all tests
Write-Host "=======================================================================" -ForegroundColor Cyan
Write-Host "RUNNING ALL TESTS" -ForegroundColor Cyan
Write-Host "=======================================================================" -ForegroundColor Cyan

# Unit tests
Write-Host "`n[1/2] Running unit tests..." -ForegroundColor Yellow
python -m pytest tests/test_unit.py -v --tb=short
if ($LASTEXITCODE -ne 0) {
    Write-Host "Unit tests FAILED" -ForegroundColor Red
    exit 1
}

# Integration tests
Write-Host "`n[2/2] Running integration tests..." -ForegroundColor Yellow
python -m pytest tests/test_integration.py -v --tb=short
if ($LASTEXITCODE -ne 0) {
    Write-Host "Integration tests FAILED" -ForegroundColor Red
    exit 1
}

Write-Host "`n=======================================================================" -ForegroundColor Cyan
Write-Host "ALL TESTS PASSED ✓" -ForegroundColor Green
Write-Host "=======================================================================" -ForegroundColor Cyan
