# Preview diseminasi — server dari root repo (wajib agar diagram arsitektur bisa dibuka).
$Root = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
Set-Location $Root
Get-NetTCPConnection -LocalPort 8765 -ErrorAction SilentlyContinue |
  Select-Object -ExpandProperty OwningProcess -Unique |
  ForEach-Object { if ($_ -and $_ -ne 0) { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue } }
Start-Process python -ArgumentList "-m", "http.server", "8765", "--directory", $Root
Start-Sleep -Seconds 1
$url = "http://localhost:8765/docs/dissemination/preview.html#demo"
Write-Host "Preview: $url"
Start-Process $url
