$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path $PSScriptRoot -Parent
Set-Location $ProjectRoot

$vite = Start-Process `
  -FilePath "npm.cmd" `
  -ArgumentList "run", "dev", "--", "--host", "127.0.0.1", "--strictPort", "--port", "4173" `
  -WorkingDirectory $ProjectRoot `
  -PassThru `
  -WindowStyle Hidden

try {
  $ready = $false
  for ($attempt = 0; $attempt -lt 60; $attempt++) {
    try {
      $response = Invoke-WebRequest -Uri "http://127.0.0.1:4173" -UseBasicParsing
      if ($response.Content -match "SlateDB Workbench" -or $response.Content -match "src/main.tsx") {
        $ready = $true
        break
      }
    } catch {
      Start-Sleep -Seconds 1
    }
  }

  if (-not $ready) {
    throw "SlateDB Vite dev server did not become ready on http://127.0.0.1:4173"
  }

  $env:SLATEDB_DEV_SERVER_URL = "http://127.0.0.1:4173"
  & ".\node_modules\.bin\electron.cmd" "."
} finally {
  if ($null -ne $vite -and -not $vite.HasExited) {
    Stop-Process -Id $vite.Id -Force
  }
}
