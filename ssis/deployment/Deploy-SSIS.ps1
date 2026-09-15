param(
  [Parameter(Mandatory=$true)][string]$SqlServer,
  [string]$Catalog = "SSISDB",
  [string]$Folder = "GamingHospitality",
  [string]$Project = "EnterpriseDataPlatform",
  [Parameter(Mandatory=$true)][string]$IspacPath
)

$ErrorActionPreference = "Stop"
if (-not (Test-Path $IspacPath)) { throw "ISPAC not found: $IspacPath" }

$ispac = [System.IO.File]::ReadAllBytes((Resolve-Path $IspacPath))
$hex = "0x" + [System.BitConverter]::ToString($ispac).Replace("-", "")

$sql = @"
IF NOT EXISTS (SELECT 1 FROM [$Catalog].catalog.folders WHERE name = N'$Folder')
    EXEC [$Catalog].catalog.create_folder @folder_name = N'$Folder';
DECLARE @project varbinary(max) = $hex;
EXEC [$Catalog].catalog.deploy_project
    @folder_name = N'$Folder',
    @project_name = N'$Project',
    @project_stream = @project;
"@

Invoke-Sqlcmd -ServerInstance $SqlServer -Database master -Query $sql -QueryTimeout 600
Write-Host "Deployed $Project to $SqlServer/$Catalog/$Folder"
