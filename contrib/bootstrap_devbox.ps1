<#

.SYNOPSIS
  Bootstraps a Windows Cloud DevBox with WSL pre-reqs to develop in monitoring repo.

.NOTES

  - The script uninstalls Docker Desktop as it interferes with WSL2.
  - Must be run as Administrator in PowerShell 7+.

#>

#Requires -RunAsAdministrator

code --install-extension ms-vscode-remote.remote-wsl
code --install-extension ms-vscode-remote.remote-containers

if ($PSVersionTable.PSVersion.Major -lt 7) {
    Write-Error "This script requires PowerShell 7+. You are running PowerShell $($PSVersionTable.PSVersion).`nTo launch PowerShell 7 as Administrator:`n  Start Menu > search 'pwsh' > right-click 'PowerShell 7' > 'Run as administrator'"
    exit 1
}

$dockerProcesses = @("Docker Desktop")
foreach ($process in $dockerProcesses) {
    try {
        Get-Process -Name $process -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    } catch {

    }
}

winget uninstall "Docker Desktop" --silent --force --accept-source-agreements 2>$null
$pkg = Get-ChildItem 'HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall','HKLM:\Software\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall' | Get-ItemProperty | Where-Object { $_.DisplayName -like "Docker Desktop*" };
if ($pkg) {
    $cmd = $pkg.UninstallString
    Start-Process "cmd.exe" -ArgumentList "/c $cmd /VERYSILENT /SUPPRESSMSGBOXES /NORESTART /FORCECLOSEAPPLICATIONS" -Wait -ErrorAction SilentlyContinue
}
Remove-Item -Path "$env:PROGRAMFILES\Docker", "$env:PROGRAMDATA\Docker*", "$env:LOCALAPPDATA\Docker*", "$env:APPDATA\Docker*" -Recurse -Force -ErrorAction SilentlyContinue

$distros = (wsl -l -q) | Where-Object { $_ -and $_.Trim() } | ForEach-Object { $_.Trim().Replace("`0", "") } | Where-Object { $_ }
foreach ($distro in $distros) {
    Write-Host "Unregistering WSL distro: $distro"
    wsl --unregister $distro
}

$RECOMMENDED_CORES = 32
$RECOMMENDED_FREE_GB = 512

$memGB=[math]::Floor((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory/1GB)
$cpu=[Environment]::ProcessorCount
$swap=[math]::Floor($memGB/4)

if ($cpu -lt $RECOMMENDED_CORES) {
    Write-Host "WARNING: This machine has $cpu cores, which is below the recommended $RECOMMENDED_CORES cores." -ForegroundColor DarkYellow
    $requiredResponse = "I am OK with having a subpar development experience"
    do {
        $response = Read-Host "Please type '$requiredResponse' **EXACTLY** as is to continue"
    } while ($response -ne $requiredResponse)
} else {
    Write-Host "(detected $cpu cores, ${memGB}GB RAM)" -ForegroundColor Green
}

$driveOptions = @(Get-CimInstance Win32_LogicalDisk -Filter "DriveType=3" |
    Sort-Object -Property FreeSpace -Descending |
    ForEach-Object {
        [PSCustomObject]@{
            Path   = "$($_.DeviceID)\VHD"
            FreeGB = [math]::Floor($_.FreeSpace / 1GB)
        }
    })

if ($driveOptions.Count -eq 0) {
    Write-Error "No local fixed drives found to host the WSL VHD."
    exit 1
}

Write-Host ""
Write-Host "Choose a drive to store the WSL VHD (the repo recommends at least ${RECOMMENDED_FREE_GB} GB free):" -ForegroundColor Cyan
for ($i = 0; $i -lt $driveOptions.Count; $i++) {
    $opt = $driveOptions[$i]
    Write-Host ("  [{0}] {1}  ({2} GB free)" -f ($i + 1), $opt.Path, $opt.FreeGB)
}

do {
    $pick = Read-Host "Pick a drive number [1-$($driveOptions.Count)]"
} while (-not ($pick -as [int]) -or [int]$pick -lt 1 -or [int]$pick -gt $driveOptions.Count)

$wslLocation = $driveOptions[[int]$pick - 1]

if ($wslLocation.FreeGB -lt $RECOMMENDED_FREE_GB) {
    Write-Host "WARNING: $($wslLocation.Path) has only $($wslLocation.FreeGB) GB free, which is below the recommended ${RECOMMENDED_FREE_GB} GB." -ForegroundColor DarkYellow
    $requiredResponse = "I am OK with having my WSL crash when my hard disk fills up"
    do {
        $response = Read-Host "Please type '$requiredResponse' **EXACTLY** as is to continue"
    } while ($response -ne $requiredResponse)
} else {
    Write-Host "Using $($wslLocation.Path) for the WSL VHD." -ForegroundColor Green
}

New-Item -ItemType Directory -Path $wslLocation.Path -Force | Out-Null
@"
[wsl2]
memory=${memGB}GB
processors=$cpu
swap=${swap}GB
networkingMode=NAT
"@ | Set-Content -Path "$env:USERPROFILE\.wslconfig"

Write-Host "Restarting WSL to apply settings"
wsl --shutdown

winget install -e --id Microsoft.GitCredentialManagerCore

Write-Host "Updating WSL (the --location flag requires WSL 2.4.4+)"
wsl --update

$minWslVersion = [version]'2.4.4'
$prevEncoding = [Console]::OutputEncoding
try {
    [Console]::OutputEncoding = [System.Text.Encoding]::Unicode
    $wslVersionRaw = (wsl --version) 2>$null
} finally {
    [Console]::OutputEncoding = $prevEncoding
}
$versionMatch = [regex]::Match(($wslVersionRaw -join "`n"), '(\d+)\.(\d+)\.(\d+)')
if (-not $versionMatch.Success) {
    Write-Error "Could not determine WSL version from 'wsl --version'. Run 'wsl --update' manually and re-run this script."
    exit 1
}
$wslVersion = [version]("$($versionMatch.Groups[1].Value).$($versionMatch.Groups[2].Value).$($versionMatch.Groups[3].Value)")
if ($wslVersion -lt $minWslVersion) {
    Write-Error "Detected WSL $wslVersion, but $minWslVersion is required for 'wsl --install --location'. Run 'wsl --update' manually and re-run this script."
    exit 1
}
Write-Host "WSL version $wslVersion detected." -ForegroundColor Green

Write-Host "Installing Ubuntu to $($wslLocation.Path)"
wsl --install -d Ubuntu-24.04 --location $wslLocation.Path