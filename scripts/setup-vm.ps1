# setup-vm.ps1
# Installs VS Code, Git, Azure CLI, kubectl, Helm, and Python on a fresh Windows Server VM.
# Run as Administrator.

$ErrorActionPreference = "Stop"

$tempDir = "$env:TEMP\vm-setup"
if (-not (Test-Path $tempDir)) {
    New-Item -ItemType Directory -Path $tempDir | Out-Null
}

# --- Git ---
Write-Host "Installing Git..." -ForegroundColor Cyan
$gitInstaller = "$tempDir\git-installer.exe"
Invoke-WebRequest -Uri "https://github.com/git-for-windows/git/releases/download/v2.47.1.windows.2/Git-2.47.1.2-64-bit.exe" -OutFile $gitInstaller
Start-Process -FilePath $gitInstaller -ArgumentList "/VERYSILENT", "/NORESTART", "/NOCANCEL", "/SP-", "/CLOSEAPPLICATIONS", "/RESTARTAPPLICATIONS" -Wait
Write-Host "Git installed." -ForegroundColor Green

# --- Azure CLI ---
Write-Host "Installing Azure CLI..." -ForegroundColor Cyan
$azCliInstaller = "$tempDir\az-cli.msi"
Invoke-WebRequest -Uri "https://aka.ms/installazurecliwindowsx64" -OutFile $azCliInstaller
Start-Process msiexec.exe -ArgumentList "/I", $azCliInstaller, "/quiet", "/norestart" -Wait
Write-Host "Azure CLI installed." -ForegroundColor Green

# --- VS Code ---
Write-Host "Installing VS Code..." -ForegroundColor Cyan
$vscodeInstaller = "$tempDir\vscode-installer.exe"
Invoke-WebRequest -Uri "https://update.code.visualstudio.com/latest/win32-x64/stable" -OutFile $vscodeInstaller
Start-Process -FilePath $vscodeInstaller -ArgumentList "/verysilent", "/norestart", "/mergetasks=!runcode,addcontextmenufiles,addcontextmenufolders,addtopath" -Wait
Write-Host "VS Code installed." -ForegroundColor Green

# --- kubectl ---
Write-Host "Installing kubectl..." -ForegroundColor Cyan
$kubectlDir = "C:\kubectl"
if (-not (Test-Path $kubectlDir)) {
    New-Item -ItemType Directory -Path $kubectlDir | Out-Null
}
$kubectlVersion = (Invoke-RestMethod -Uri "https://dl.k8s.io/release/stable.txt").Trim()
Invoke-WebRequest -Uri "https://dl.k8s.io/release/$kubectlVersion/bin/windows/amd64/kubectl.exe" -OutFile "$kubectlDir\kubectl.exe"
$currentMachinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
if ($currentMachinePath -notlike "*$kubectlDir*") {
    [Environment]::SetEnvironmentVariable("Path", "$currentMachinePath;$kubectlDir", "Machine")
}
Write-Host "kubectl $kubectlVersion installed." -ForegroundColor Green

# --- Helm ---
Write-Host "Installing Helm..." -ForegroundColor Cyan
winget install --id Helm.Helm --exact --silent --accept-package-agreements --accept-source-agreements
Write-Host "Helm installed." -ForegroundColor Green

# --- Python ---
Write-Host "Installing Python (latest)..." -ForegroundColor Cyan
winget install --id Python.Python.3 --exact --silent --accept-package-agreements --accept-source-agreements --scope machine
Write-Host "Python installed." -ForegroundColor Green

# --- Refresh PATH ---
$machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
$userPath    = [Environment]::GetEnvironmentVariable("Path", "User")
$env:Path    = "$machinePath;$userPath"

# --- Verify installations ---
Write-Host "`nVerifying installations..." -ForegroundColor Cyan
try { git --version }   catch { Write-Warning "Git not found in PATH. You may need to restart the shell." }
try { az version }      catch { Write-Warning "Azure CLI not found in PATH. You may need to restart the shell." }
try { code --version }  catch { Write-Warning "VS Code not found in PATH. You may need to restart the shell." }
try { kubectl version --client } catch { Write-Warning "kubectl not found in PATH. You may need to restart the shell." }
try { helm version }    catch { Write-Warning "Helm not found in PATH. You may need to restart the shell." }
try { python --version } catch { Write-Warning "Python not found in PATH. You may need to restart the shell." }

# --- Cleanup ---
Remove-Item -Path $tempDir -Recurse -Force -ErrorAction SilentlyContinue
Write-Host "`nSetup complete. Restart the shell or log off/on for PATH changes to take full effect." -ForegroundColor Green
