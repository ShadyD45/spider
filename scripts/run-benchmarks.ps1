Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "        SPIDER ARTIFACT MESH — BENCHMARK SUITE RUNNER           " -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

Write-Host "`n--- 1. Running Go Microbenchmarks ---" -ForegroundColor Yellow
go test -bench=. -benchmem ./pkg/...

Write-Host "`n--- 2. Running End-to-End Fleet Distribution Benchmark (50 MB x 4 Nodes) ---" -ForegroundColor Yellow
.\artifactctl.exe benchmark --size=50 --workers=4 --chunk-size=4

Write-Host "`n--- 3. Running Larger Scale Distribution Benchmark (100 MB x 6 Nodes) ---" -ForegroundColor Yellow
.\artifactctl.exe benchmark --size=100 --workers=6 --chunk-size=4

Write-Host "`n=================================================================" -ForegroundColor Green
Write-Host "                 BENCHMARK SUITE COMPLETED                      " -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Green
