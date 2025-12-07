# Test runner script for Windows PowerShell

Write-Host "Running E-commerce Data Processing Tests..." -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# Run tests with coverage
pytest tests/ -v --cov=src --cov-report=term-missing

# Check exit code
if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "✓ All tests passed!" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "✗ Some tests failed!" -ForegroundColor Red
    exit 1
}
