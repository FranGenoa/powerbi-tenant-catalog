#requires -Version 7.0
<#
.SYNOPSIS
    Deploy the PBI Tenant Catalog solution (Lakehouse, Notebook, Pipeline, Semantic Model,
    Report) into a Fabric workspace in any tenant, from scratch.

.DESCRIPTION
    Recreates the exported Fabric artifacts in a target tenant and rewires every
    cross-artifact reference so nothing points back at the source workspace.

    Deployment order (dependencies flow left to right):

        Lakehouse -> Notebook -> DataPipeline -> [RUN PIPELINE] -> SemanticModel -> Report

    The pipeline is executed before the semantic model is created because all of the
    model's tables are Direct Lake 'entity' partitions bound to the pbi_* Delta tables
    that the notebook produces. Creating the model first yields a model that loads but
    fails on query.

.PARAMETER WorkspaceName
    Target workspace display name. Created if it does not exist (requires -CapacityName
    or -CapacityId).

.PARAMETER WorkspaceId
    Target workspace GUID. Use instead of -WorkspaceName to deploy into an existing
    workspace.

.PARAMETER CapacityName
    Fabric capacity display name used when creating the workspace.

.PARAMETER CapacityId
    Fabric capacity GUID used when creating the workspace.

.PARAMETER DefinitionsPath
    Root of the exported definitions. Defaults to ./definitions next to this script.

.PARAMETER ExpectedTenantId
    Optional guard. Aborts if the signed-in Azure CLI tenant does not match, which
    prevents deploying into the wrong tenant.

.PARAMETER SkipPipelineRun
    Create the artifacts but do not run the pipeline. The semantic model and report are
    still deployed; they will have no data until the pipeline is run manually.

.PARAMETER PipelineTimeoutMinutes
    How long to wait for the tenant scan pipeline. Default 120.

.PARAMETER PlanOnly
    Print the deployment plan and rebind map, then exit without changing anything.

.EXAMPLE
    az login --tenant contoso.onmicrosoft.com
    ./Deploy-PBICatalog.ps1 -WorkspaceName "PBI Catalog" -CapacityName "myfabriccap" -PlanOnly

.EXAMPLE
    ./Deploy-PBICatalog.ps1 -WorkspaceName "PBI Catalog" -CapacityName "myfabriccap"

.NOTES
    Prerequisites in the TARGET tenant:
      * az login completed; caller has permission to create workspaces.
      * An active Fabric/Premium capacity (Direct Lake and notebooks both require it).
      * For a full tenant scan the caller must be a Fabric Administrator, and these
        tenant settings must be ON under Admin portal -> Tenant settings -> Admin API:
          - Enhanced admin APIs responses with detailed metadata
          - Enhanced admin APIs responses with DAX and mashup expressions
        Without admin rights the notebook falls back to user mode and only scans
        workspaces the caller can access.
#>
[CmdletBinding(DefaultParameterSetName = 'ByName')]
param(
    [Parameter(Mandatory, ParameterSetName = 'ByName')][string]$WorkspaceName,
    [Parameter(Mandatory, ParameterSetName = 'ById')][string]$WorkspaceId,
    [Parameter(ParameterSetName = 'ByName')][string]$CapacityName,
    [Parameter(ParameterSetName = 'ByName')][string]$CapacityId,
    [string]$DefinitionsPath,
    [string]$ExpectedTenantId,
    [switch]$SkipPipelineRun,
    [int]$PipelineTimeoutMinutes = 120,
    [switch]$PlanOnly,
    [string]$Environment = "https://api.fabric.microsoft.com"
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$script:Base = "$($Environment.TrimEnd('/'))/v1"

# Parts with these extensions get token replacement; everything else is treated as
# binary and passed through untouched so images are never corrupted.
$script:TextExtensions = @(
    '.json', '.tmdl', '.tmsl', '.bim', '.pbir', '.pbism', '.py', '.kql',
    '.platform', '.sql', '.scala', '.r', '.pq', '.md', '.txt', '.ipynb'
)

# Items are created in this order so each item's new id exists before its dependents
# are uploaded. SQLEndpoint is auto-provisioned with the Lakehouse and is never created.
$script:DeployOrder = @('Lakehouse', 'Notebook', 'DataPipeline', 'SemanticModel', 'Report')
$script:SkipTypes = @('SQLEndpoint')

# ---------------------------------------------------------------------------
# Auth + REST helpers
# ---------------------------------------------------------------------------

function Get-FabToken {
    param([string]$Resource = $Environment)
    $token = az account get-access-token --resource $Resource --query accessToken -o tsv 2>$null
    if (-not $token) { throw "Could not acquire a token for $Resource. Run 'az login' first." }
    return $token
}

function Get-FabHeaders {
    return @{ Authorization = "Bearer $(Get-FabToken)"; "Content-Type" = "application/json" }
}

function Invoke-FabRequest {
    <#
        Wraps Invoke-WebRequest with throttling retries and returns the raw response so
        callers can inspect status codes and the Location header for long-running ops.
    #>
    param(
        [Parameter(Mandatory)][string]$Method,
        [Parameter(Mandatory)][string]$Uri,
        $Body,
        [int]$MaxRetries = 6
    )

    for ($attempt = 0; $attempt -le $MaxRetries; $attempt++) {
        $headers = Get-FabHeaders
        $params = @{
            Method             = $Method
            Uri                = $Uri
            Headers            = $headers
            SkipHttpErrorCheck = $true
        }
        if ($null -ne $Body) {
            $params.Body = if ($Body -is [string]) { $Body } else { $Body | ConvertTo-Json -Depth 40 -Compress }
        }

        $response = Invoke-WebRequest @params

        if ($response.StatusCode -in @(429, 503, 502, 500)) {
            if ($attempt -eq $MaxRetries) { break }
            $retryAfter = [string]$response.Headers['Retry-After']
            $wait = if ($retryAfter -match '^\d+$') { [int]$retryAfter } else { [Math]::Min(60, [Math]::Pow(2, $attempt + 1)) }
            Write-Host "    throttled ($($response.StatusCode)); retrying in ${wait}s" -ForegroundColor DarkYellow
            Start-Sleep -Seconds $wait
            continue
        }
        return $response
    }
    throw "Request to $Uri kept failing after $MaxRetries retries."
}

function Wait-FabLro {
    <#
        Polls the Location header of a 202 until it settles, then returns the operation
        result body (which carries the new item's id).
    #>
    param([Parameter(Mandatory)]$Response, [int]$TimeoutMinutes = 30)

    $operationUrl = [string]$Response.Headers['Location']
    if (-not $operationUrl) { throw "202 response did not include a Location header." }

    $deadline = (Get-Date).AddMinutes($TimeoutMinutes)
    $delay = 2
    do {
        Start-Sleep -Seconds $delay
        if ($delay -lt 10) { $delay++ }
        $operation = (Invoke-FabRequest -Method GET -Uri $operationUrl).Content | ConvertFrom-Json
        if ((Get-Date) -gt $deadline) { throw "Operation timed out after $TimeoutMinutes minutes." }
    } while ($operation.status -in @('NotStarted', 'Running', 'Undefined'))

    if ($operation.status -ne 'Succeeded') {
        $reason = if ($operation.error) { "$($operation.error.errorCode): $($operation.error.message)" } else { $operation.status }
        throw $reason
    }

    $result = Invoke-FabRequest -Method GET -Uri "$operationUrl/result"
    if ($result.StatusCode -ge 400) { throw "Failed to fetch operation result: HTTP $($result.StatusCode)" }
    return $result.Content | ConvertFrom-Json
}

# ---------------------------------------------------------------------------
# Definition loading + rebinding
# ---------------------------------------------------------------------------

function Get-ItemParts {
    <#
        Walks an exported item folder and returns each file as a part with its Fabric
        relative path. .platform is excluded because displayName and type are supplied
        in the create body, and re-uploading it can clash with the target's logicalId.
    #>
    param([Parameter(Mandatory)][string]$ItemFolder)

    $parts = @()
    foreach ($file in Get-ChildItem -Path $ItemFolder -Recurse -File) {
        $relative = [System.IO.Path]::GetRelativePath($ItemFolder, $file.FullName).Replace('\', '/')
        if ($relative -eq '.platform') { continue }
        $parts += [pscustomobject]@{
            Path      = $relative
            Bytes     = [System.IO.File]::ReadAllBytes($file.FullName)
            Extension = $file.Extension.ToLowerInvariant()
        }
    }
    return $parts
}

function ConvertTo-FabDefinitionParts {
    <#
        Applies the rebind map to text parts, then base64-encodes every part.

        $GlobalMap  - GUID replacements, safe to apply to any text part.
        $ScopedMap  - replacements restricted to specific part paths. The workspace
                      display name lives here: a blind global replace would also rewrite
                      visual titles and text boxes inside report JSON.
    #>
    param(
        [Parameter(Mandatory)]$Parts,
        [Parameter(Mandatory)][hashtable]$GlobalMap,
        [hashtable]$ScopedMap = @{}
    )

    $utf8 = [System.Text.UTF8Encoding]::new($false)
    $encoded = @()

    foreach ($part in $Parts) {
        $bytes = $part.Bytes

        if ($script:TextExtensions -contains $part.Extension) {
            $text = $utf8.GetString($bytes)

            foreach ($key in $GlobalMap.Keys) {
                $text = $text.Replace($key, $GlobalMap[$key])
            }
            foreach ($key in $ScopedMap.Keys) {
                $rule = $ScopedMap[$key]
                if ($rule.Paths -contains $part.Path) {
                    $text = $text.Replace($key, $rule.Value)
                }
            }
            $bytes = $utf8.GetBytes($text)
        }

        $encoded += @{
            path        = $part.Path
            payload     = [Convert]::ToBase64String($bytes)
            payloadType = 'InlineBase64'
        }
    }
    return $encoded
}

function Get-ReportFormat {
    # PBIR keeps report.json under definition/; PBIR-Legacy keeps it at the item root.
    param([Parameter(Mandatory)]$Parts)
    if ($Parts.Path -contains 'report.json') { return 'PBIR-Legacy' }
    return 'PBIR'
}

# ---------------------------------------------------------------------------
# Workspace + item operations
# ---------------------------------------------------------------------------

function Resolve-TargetWorkspace {
    param([Parameter(Mandatory)][string]$ParameterSet)

    if ($ParameterSet -eq 'ById') {
        $response = Invoke-FabRequest -Method GET -Uri "$script:Base/workspaces/$WorkspaceId"
        if ($response.StatusCode -ge 400) { throw "Workspace $WorkspaceId not found or not accessible." }
        $workspace = $response.Content | ConvertFrom-Json
        return [pscustomobject]@{ Id = $workspace.id; Name = $workspace.displayName; Created = $false }
    }

    $all = ((Invoke-FabRequest -Method GET -Uri "$script:Base/workspaces").Content | ConvertFrom-Json).value
    $match = @($all | Where-Object { $_.displayName -eq $WorkspaceName })

    if ($match.Count -gt 1) { throw "More than one workspace is named '$WorkspaceName'. Re-run with -WorkspaceId." }
    if ($match.Count -eq 1) {
        Write-Host "Using existing workspace '$WorkspaceName'." -ForegroundColor Yellow
        return [pscustomobject]@{ Id = $match[0].id; Name = $match[0].displayName; Created = $false }
    }

    # Not found -> create it, which requires a capacity.
    $resolvedCapacityId = $CapacityId
    if (-not $resolvedCapacityId) {
        if (-not $CapacityName) {
            throw "Workspace '$WorkspaceName' does not exist. Supply -CapacityName or -CapacityId so it can be created."
        }
        $capacities = ((Invoke-FabRequest -Method GET -Uri "$script:Base/capacities").Content | ConvertFrom-Json).value
        $capacity = @($capacities | Where-Object { $_.displayName -eq $CapacityName })
        if ($capacity.Count -ne 1) {
            $available = ($capacities | ForEach-Object { $_.displayName }) -join ', '
            throw "Capacity '$CapacityName' not found. Available: $available"
        }
        $resolvedCapacityId = $capacity[0].id
    }

    if ($PlanOnly) {
        return [pscustomobject]@{ Id = '<new-workspace-id>'; Name = $WorkspaceName; Created = $true }
    }

    $response = Invoke-FabRequest -Method POST -Uri "$script:Base/workspaces" -Body @{
        displayName = $WorkspaceName
        capacityId  = $resolvedCapacityId
        description = 'Power BI tenant catalog and data dictionary.'
    }
    if ($response.StatusCode -ge 400) { throw "Could not create workspace: HTTP $($response.StatusCode) $($response.Content)" }

    $workspace = $response.Content | ConvertFrom-Json
    Write-Host "Created workspace '$WorkspaceName' on capacity $resolvedCapacityId." -ForegroundColor Green
    return [pscustomobject]@{ Id = $workspace.id; Name = $workspace.displayName; Created = $true }
}

function New-FabItem {
    param(
        [Parameter(Mandatory)][string]$TargetWorkspaceId,
        [Parameter(Mandatory)][string]$DisplayName,
        [Parameter(Mandatory)][string]$Type,
        [string]$Description,
        $DefinitionParts,
        [string]$Format
    )

    $body = @{ displayName = $DisplayName; type = $Type }
    if ($Description) { $body.description = $Description }

    if ($DefinitionParts -and $DefinitionParts.Count -gt 0) {
        $definition = @{ parts = $DefinitionParts }
        if ($Format) { $definition.format = $Format }
        $body.definition = $definition
    }

    $response = Invoke-FabRequest -Method POST -Uri "$script:Base/workspaces/$TargetWorkspaceId/items" -Body $body

    switch ($response.StatusCode) {
        201 { return ($response.Content | ConvertFrom-Json).id }
        200 { return ($response.Content | ConvertFrom-Json).id }
        202 { return (Wait-FabLro -Response $response).id }
        default { throw "HTTP $($response.StatusCode): $($response.Content)" }
    }
}

function Invoke-FabPipeline {
    <#
        Starts the pipeline on demand and blocks until the run finishes. This is the step
        that populates the pbi_* Delta tables the semantic model binds to.
    #>
    param(
        [Parameter(Mandatory)][string]$TargetWorkspaceId,
        [Parameter(Mandatory)][string]$PipelineId,
        [int]$TimeoutMinutes = 120
    )

    $uri = "$script:Base/workspaces/$TargetWorkspaceId/items/$PipelineId/jobs/instances?jobType=Pipeline"
    $response = Invoke-FabRequest -Method POST -Uri $uri -Body @{}

    if ($response.StatusCode -notin @(200, 202)) {
        throw "Could not start pipeline: HTTP $($response.StatusCode) $($response.Content)"
    }

    $jobUrl = [string]$response.Headers['Location']
    if (-not $jobUrl) { throw "Pipeline start did not return a job instance Location header." }

    Write-Host "    pipeline started; polling for completion (timeout ${TimeoutMinutes}m)..." -ForegroundColor DarkGray
    $deadline = (Get-Date).AddMinutes($TimeoutMinutes)
    $lastStatus = ''

    do {
        Start-Sleep -Seconds 15
        $job = (Invoke-FabRequest -Method GET -Uri $jobUrl).Content | ConvertFrom-Json
        if ($job.status -ne $lastStatus) {
            Write-Host "    status: $($job.status)" -ForegroundColor DarkGray
            $lastStatus = $job.status
        }
        if ((Get-Date) -gt $deadline) { throw "Pipeline did not finish within $TimeoutMinutes minutes." }
    } while ($job.status -in @('NotStarted', 'InProgress'))

    if ($job.status -ne 'Completed') {
        $reason = if ($job.failureReason) { $job.failureReason.message } else { $job.status }
        throw "Pipeline run finished with status '$($job.status)'. $reason"
    }
    Write-Host "    pipeline completed." -ForegroundColor Green
}

function Test-LakehouseTables {
    # Confirms the scan actually produced tables before the Direct Lake model is created.
    param(
        [Parameter(Mandatory)][string]$TargetWorkspaceId,
        [Parameter(Mandatory)][string]$LakehouseId
    )

    $response = Invoke-FabRequest -Method GET -Uri "$script:Base/workspaces/$TargetWorkspaceId/lakehouses/$LakehouseId/tables"
    if ($response.StatusCode -ge 400) {
        Write-Warning "Could not list lakehouse tables (HTTP $($response.StatusCode)); continuing."
        return @()
    }
    $tables = ((($response.Content | ConvertFrom-Json).data) | ForEach-Object { $_.name })
    return @($tables | Where-Object { $_ -like 'pbi_*' })
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if (-not $DefinitionsPath) {
    $DefinitionsPath = Join-Path $PSScriptRoot 'definitions'
}
if (-not (Test-Path $DefinitionsPath)) {
    throw "Definitions folder not found at '$DefinitionsPath'. Pass -DefinitionsPath."
}

# --- Preflight -------------------------------------------------------------
$account = az account show --query "{user:user.name,tenantId:tenantId,name:name}" -o json 2>$null | ConvertFrom-Json
if (-not $account) { throw "Azure CLI is not logged in. Run 'az login' first." }

Write-Host ""
Write-Host "Signed in as : $($account.user)"
Write-Host "Tenant       : $($account.tenantId)"

if ($ExpectedTenantId -and $account.tenantId -ne $ExpectedTenantId) {
    throw "Signed-in tenant $($account.tenantId) does not match -ExpectedTenantId $ExpectedTenantId. Run 'az login --tenant $ExpectedTenantId'."
}

# --- Discover source artifacts --------------------------------------------
$inventoryPath = Join-Path (Split-Path $DefinitionsPath -Parent) '_items_inventory.json'
if (-not (Test-Path $inventoryPath)) { throw "Inventory not found at '$inventoryPath'." }
$inventory = Get-Content $inventoryPath -Raw | ConvertFrom-Json

# The source workspace id is not stored in the inventory, so recover it from the
# notebook's lakehouse binding, which always carries it.
$notebookFile = Get-ChildItem -Path (Join-Path $DefinitionsPath 'Notebook') -Recurse -Filter 'notebook-content.py' -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $notebookFile) { throw "Could not locate notebook-content.py under '$DefinitionsPath'." }
$notebookText = Get-Content $notebookFile.FullName -Raw
if ($notebookText -notmatch '"default_lakehouse_workspace_id":\s*"([0-9a-fA-F-]{36})"') {
    throw "Could not determine the source workspace id from the notebook metadata."
}
$sourceWorkspaceId = $Matches[1]

# The source workspace name only appears in the report connection string.
$sourceWorkspaceName = $null
$pbirFile = Get-ChildItem -Path (Join-Path $DefinitionsPath 'Report') -Recurse -Filter 'definition.pbir' -ErrorAction SilentlyContinue | Select-Object -First 1
if ($pbirFile) {
    $pbirText = Get-Content $pbirFile.FullName -Raw
    if ($pbirText -match 'myorg/([^"\\]+)\\?"') { $sourceWorkspaceName = $Matches[1] }
}

# Build the ordered work list from the inventory rather than hard-coding it.
$plan = @()
foreach ($type in $script:DeployOrder) {
    foreach ($item in @($inventory | Where-Object { $_.type -eq $type })) {
        $folder = Join-Path (Join-Path $DefinitionsPath $type) $item.displayName
        if (-not (Test-Path $folder)) {
            Write-Warning "No exported definition for $type '$($item.displayName)'; skipping."
            continue
        }
        $plan += [pscustomobject]@{
            Type        = $type
            DisplayName = $item.displayName
            SourceId    = $item.id
            Folder      = $folder
        }
    }
}
$skipped = @($inventory | Where-Object { $script:SkipTypes -contains $_.type })

# --- Resolve target --------------------------------------------------------
$target = Resolve-TargetWorkspace -ParameterSet $PSCmdlet.ParameterSetName

Write-Host "Target       : $($target.Name)  ($($target.Id))"
Write-Host ""
Write-Host "Deployment plan" -ForegroundColor Cyan
$plan | Select-Object @{n = 'Order'; e = { $plan.IndexOf($_) + 1 } }, Type, DisplayName | Format-Table -AutoSize
if ($skipped) {
    Write-Host "Skipped (auto-provisioned): $(($skipped | ForEach-Object { "$($_.type) '$($_.displayName)'" }) -join ', ')" -ForegroundColor DarkGray
}

Write-Host "Rebind map" -ForegroundColor Cyan
Write-Host "  workspace id   : $sourceWorkspaceId -> $($target.Id)"
if ($sourceWorkspaceName) {
    Write-Host "  workspace name : '$sourceWorkspaceName' -> '$($target.Name)'   (definition.pbir only)"
}
Write-Host "  item ids       : resolved during deployment (old -> new)"
Write-Host ""

if ($PlanOnly) {
    Write-Host "-PlanOnly specified; nothing was changed." -ForegroundColor Yellow
    return
}

# --- Deploy ----------------------------------------------------------------
# Seeded with the workspace id; each created item adds its own old->new mapping.
$globalMap = @{ $sourceWorkspaceId = $target.Id }
$scopedMap = @{}
if ($sourceWorkspaceName -and $sourceWorkspaceName -ne $target.Name) {
    $scopedMap[$sourceWorkspaceName] = @{ Value = $target.Name; Paths = @('definition.pbir') }
}

$results = [System.Collections.Generic.List[object]]::new()
$lakehouseId = $null
$pipelineRan = $false

foreach ($entry in $plan) {

    # The pipeline must finish before the Direct Lake model is created, otherwise the
    # pbi_* tables it binds to do not exist yet.
    if ($entry.Type -eq 'SemanticModel' -and -not $pipelineRan -and -not $SkipPipelineRun) {
        $pipelineResult = $results | Where-Object { $_.Type -eq 'DataPipeline' -and $_.Status -eq 'Deployed' } | Select-Object -First 1
        if ($pipelineResult) {
            Write-Host "[run] Pipeline '$($pipelineResult.Name)'" -ForegroundColor Magenta
            try {
                Invoke-FabPipeline -TargetWorkspaceId $target.Id -PipelineId $pipelineResult.NewId -TimeoutMinutes $PipelineTimeoutMinutes
                $pipelineRan = $true

                if ($lakehouseId) {
                    $tables = Test-LakehouseTables -TargetWorkspaceId $target.Id -LakehouseId $lakehouseId
                    if ($tables.Count -eq 0) {
                        Write-Warning "Pipeline completed but no pbi_* tables were found. The semantic model will deploy but return no data."
                    }
                    else {
                        Write-Host "    $($tables.Count) pbi_* tables present." -ForegroundColor Green
                    }
                }
            }
            catch {
                Write-Warning "Pipeline run failed: $($_.Exception.Message)"
                Write-Warning "Continuing with deployment; re-run the pipeline manually, then refresh the semantic model."
            }
        }
    }

    Write-Host "[$($entry.Type)] $($entry.DisplayName)" -ForegroundColor Cyan
    $status = 'Deployed'
    $newId = $null

    try {
        # Description travels in .platform, which is not uploaded as a part.
        $description = $null
        $platformPath = Join-Path $entry.Folder '.platform'
        if (Test-Path $platformPath) {
            $description = ((Get-Content $platformPath -Raw | ConvertFrom-Json).metadata).description
        }

        if ($entry.Type -eq 'Lakehouse') {
            # A Lakehouse takes no definition; its SQL endpoint is provisioned for us.
            $newId = New-FabItem -TargetWorkspaceId $target.Id -DisplayName $entry.DisplayName `
                -Type $entry.Type -Description $description
            $lakehouseId = $newId
        }
        else {
            $parts = Get-ItemParts -ItemFolder $entry.Folder
            $format = if ($entry.Type -eq 'Report') { Get-ReportFormat -Parts $parts } else { $null }
            $payload = ConvertTo-FabDefinitionParts -Parts $parts -GlobalMap $globalMap -ScopedMap $scopedMap

            $newId = New-FabItem -TargetWorkspaceId $target.Id -DisplayName $entry.DisplayName `
                -Type $entry.Type -Description $description -DefinitionParts $payload -Format $format
        }

        # Register the mapping so dependents resolve to the new id.
        $globalMap[$entry.SourceId] = $newId
        Write-Host "    created: $newId" -ForegroundColor Green
    }
    catch {
        $status = "Failed: $($_.Exception.Message)"
        Write-Host "    $status" -ForegroundColor Red
    }

    $results.Add([pscustomobject]@{
            Type   = $entry.Type
            Name   = $entry.DisplayName
            NewId  = $newId
            Status = $status
        })
}

# --- Summary ---------------------------------------------------------------
Write-Host ""
Write-Host "===== DEPLOYMENT SUMMARY =====" -ForegroundColor Cyan
$results | Format-Table -AutoSize

$deployed = @($results | Where-Object { $_.Status -eq 'Deployed' }).Count
$failed = @($results | Where-Object { $_.Status -like 'Failed*' }).Count

Write-Host "Deployed: $deployed   Failed: $failed   Skipped: $($skipped.Count)"
Write-Host "Workspace: https://app.powerbi.com/groups/$($target.Id)/list"

if ($SkipPipelineRun) {
    Write-Host ""
    Write-Host "-SkipPipelineRun was set. Run the pipeline, then refresh the semantic model to populate the report." -ForegroundColor Yellow
}

if ($failed -gt 0) { exit 1 }
