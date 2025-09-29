# Flexurio Windows installer
# - Downloads latest release binary for Windows
# - Installs to %LOCALAPPDATA%\flexurio\bin
# - Creates wrapper scripts: flexurio.ps1 and flexurio.cmd
# - Adds install dir to user PATH (current session + persistent)

param(
  [switch]$Quiet
)

$ErrorActionPreference = 'Stop'

function Write-Log {
  param([string]$Message)
  if (-not $Quiet) { Write-Host "[flexurio-install] $Message" }
}

function Write-Err {
  param([string]$Message)
  Write-Error "[flexurio-install][ERROR] $Message"
}

function Get-ArchTag {
  switch ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture) {
    'X64'   { return 'x86_64' }
    'Arm64' { return 'aarch64' }
    default { throw "Unsupported architecture: $([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture)" }
  }
}

function Ensure-Dir {
  param([string]$Path)
  if (-not (Test-Path -LiteralPath $Path)) {
    New-Item -ItemType Directory -Path $Path | Out-Null
  }
}

function Add-ToUserPath {
  param([string]$Dir)
  # Update persistent user PATH
  $userPath = [System.Environment]::GetEnvironmentVariable('Path','User')
  if (-not $userPath) { $userPath = '' }
  $parts = $userPath -split ';' | Where-Object { $_ -ne '' }
  if ($parts -notcontains $Dir) {
    $newPath = ($userPath.TrimEnd(';') + ';' + $Dir).Trim(';')
    [System.Environment]::SetEnvironmentVariable('Path', $newPath, 'User')
    Write-Log "Added to user PATH: $Dir"
  } else {
    Write-Log "User PATH already includes: $Dir"
  }
  # Update current session PATH
  if (-not ($env:Path -split ';' | Where-Object { $_ -eq $Dir })) {
    $env:Path = ($env:Path.TrimEnd(';') + ';' + $Dir).Trim(';')
  }
}

function Get-LatestAssetUrl {
  param(
    [string]$RepoOwner,
    [string]$RepoName,
    [string]$ArchTag
  )
  $base = "https://github.com/$RepoOwner/$RepoName"
  $assetName = "flx-nocode-$ArchTag-pc-windows-gnu.exe"
  $direct = "$base/releases/latest/download/$assetName"

  # Try direct URL first
  try {
    $tmp = New-TemporaryFile
    Remove-Item $tmp -Force 2>$null
    $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("flexurio-probe-" + [System.Guid]::NewGuid().ToString() + ".tmp")
    Invoke-WebRequest -UseBasicParsing -Method Head -Uri $direct -OutFile $tmp | Out-Null
    Remove-Item $tmp -Force 2>$null
    return $direct
  } catch {
    Write-Log "Direct asset URL not accessible; querying GitHub API..."
  }

  $api = "https://api.github.com/repos/$RepoOwner/$RepoName/releases/latest"
  $json = Invoke-RestMethod -Headers @{ 'User-Agent' = 'flexurio-installer' } -Uri $api -Method Get
  foreach ($asset in $json.assets) {
    $url = $asset.browser_download_url
    if ($url -match [Regex]::Escape("$ArchTag") -and $url -match 'pc-windows-gnu' -and $url -like '*.exe') {
      return $url
    }
  }
  throw "No matching Windows asset found in latest release. Check $base/releases"
}

function Download-File {
  param([string]$Url, [string]$Destination)
  Write-Log "Downloading: $Url"
  Invoke-WebRequest -UseBasicParsing -Uri $Url -OutFile $Destination
}

function Install-Flexurio {
  $repoOwner = 'flexurio'
  $repoName  = 'flx-nocode-api'
  $archTag   = Get-ArchTag
  $installDir = if ($env:INSTALL_BIN_DIR -and $env:INSTALL_BIN_DIR.Trim() -ne '') { $env:INSTALL_BIN_DIR } else { Join-Path $env:LOCALAPPDATA 'flexurio\\bin' }
  Ensure-Dir $installDir

  $assetUrl = Get-LatestAssetUrl -RepoOwner $repoOwner -RepoName $repoName -ArchTag $archTag

  $workDir = Join-Path ([System.IO.Path]::GetTempPath()) ("flexurio-install-" + [System.Guid]::NewGuid().ToString())
  Ensure-Dir $workDir
  try {
    $downloadPath = Join-Path $workDir "flx-nocode.exe"
    Download-File -Url $assetUrl -Destination $downloadPath

    $targetExe = Join-Path $installDir 'flx-nocode.exe'
    if (Test-Path $targetExe) { Remove-Item $targetExe -Force }
    Move-Item -Path $downloadPath -Destination $targetExe
    Write-Log "Installed core binary to: $targetExe"

    # Create wrapper PowerShell script
    $wrapperPs1 = Join-Path $installDir 'flexurio.ps1'
    $wrapperCmd = Join-Path $installDir 'flexurio.cmd'

    @'
Param(
  [Parameter(ValueFromRemainingArguments = $true)]
  [string[]]$Args
)

$ErrorActionPreference = 'Stop'
function Write-Log { param([string]$m) Write-Host "[flexurio] $m" }
function Write-Err { param([string]$m) Write-Error "[flexurio][ERROR] $m" }

function Get-ArchTag {
  switch ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture) {
    'X64'   { return 'x86_64' }
    'Arm64' { return 'aarch64' }
    default { throw "Unsupported architecture: $([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture)" }
  }
}

function Load-DotEnv {
  $path = Join-Path (Get-Location) '.env'
  if (-not (Test-Path -LiteralPath $path)) { return }
  Get-Content -LiteralPath $path | ForEach-Object {
    $line = $_
    if (-not $line) { return }
    if ($line.Trim().StartsWith('#')) { return }
    if ($line -notmatch '^[A-Za-z_][A-Za-z0-9_]*\s*=') { return }
    $key, $val = $line -split '=', 2
    $key = $key.Trim()
    $val = $val.Trim()
    if ($val -match '^".*"$' -or $val -match "^'.*'$") {
      $val = $val.Substring(1, $val.Length - 2)
    }
    $env:$key = $val
  }
}

function Update-Binary {
  $repoOwner = 'flexurio'
  $repoName  = 'flx-nocode-api'
  $archTag   = Get-ArchTag
  $base = "https://github.com/$repoOwner/$repoName"
  $assetName = "flx-nocode-$archTag-pc-windows-gnu.exe"
  $direct = "$base/releases/latest/download/$assetName"
  $work = Join-Path ([System.IO.Path]::GetTempPath()) ("flexurio-update-" + [System.Guid]::NewGuid().ToString())
  New-Item -ItemType Directory -Path $work | Out-Null
  try {
    $tmp = Join-Path $work 'flx-nocode.exe'
    try {
      Invoke-WebRequest -UseBasicParsing -Uri $direct -OutFile $tmp
    } catch {
      $api = "https://api.github.com/repos/$repoOwner/$repoName/releases/latest"
      $json = Invoke-RestMethod -Headers @{ 'User-Agent' = 'flexurio-wrapper' } -Uri $api -Method Get
      $asset = $json.assets | Where-Object { $_.browser_download_url -match [Regex]::Escape($archTag) -and $_.browser_download_url -match 'pc-windows-gnu' -and $_.browser_download_url -like '*.exe' } | Select-Object -First 1
      if (-not $asset) { throw 'No matching asset found' }
      Invoke-WebRequest -UseBasicParsing -Uri $asset.browser_download_url -OutFile $tmp
    }
    $binDir = Split-Path -Parent $PSCommandPath
    $target = Join-Path $binDir 'flx-nocode.exe'
    if (Test-Path $target) { Remove-Item $target -Force }
    Move-Item $tmp $target
    Write-Log "Updated: $target"
  } finally {
    Remove-Item $work -Recurse -Force -ErrorAction SilentlyContinue
  }
}

$binDir = Split-Path -Parent $PSCommandPath
$exe = Join-Path $binDir 'flx-nocode.exe'
if (-not (Test-Path -LiteralPath $exe)) { Write-Err 'flx-nocode.exe not found next to wrapper'; exit 1 }

if ($Args.Count -gt 0 -and ($Args[0] -eq '--version' -or $Args[0] -eq 'version' -or $Args[0] -eq '-V')) {
  & $exe --version
  exit $LASTEXITCODE
}

if ($Args.Count -gt 0 -and ($Args[0] -eq '--update' -or $Args[0] -eq 'update' -or $Args[0] -eq '-U')) {
  Update-Binary
  exit 0
}

Load-DotEnv

& $exe @Args
exit $LASTEXITCODE
'@ | Set-Content -Encoding UTF8 -LiteralPath $wrapperPs1

    @'@echo off
setlocal
REM Flexurio CMD shim
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0flexurio.ps1" %*
'@ | Set-Content -Encoding ASCII -LiteralPath $wrapperCmd

    Write-Log "Installed wrapper to: $wrapperPs1 and $wrapperCmd"

    Add-ToUserPath -Dir $installDir
  }
  finally {
    Remove-Item $workDir -Recurse -Force -ErrorAction SilentlyContinue
  }

  Write-Log 'Installation complete!'
  Write-Host 'Next steps:'
  Write-Host "  1) Open a new terminal (PowerShell or CMD) to reload PATH"
  Write-Host "  2) Run: flexurio"
}

# Entry
Install-Flexurio
