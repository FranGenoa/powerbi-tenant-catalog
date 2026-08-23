---
name: deploy-pbi-catalog
description: "Deploy the PBI Tenant Catalog solution (Lakehouse, Notebook, Data Pipeline, Direct Lake Semantic Model, Report) into a Microsoft Fabric workspace in any tenant, from scratch, using Azure CLI and the Fabric REST API. Runs the tenant scan pipeline before publishing the semantic model so Direct Lake tables exist. USE WHEN the user wants to: deploy, install, publish, provision, set up, roll out, or clone the PBI Tenant Catalog / Power BI tenant documentation solution; deploy this solution to a new tenant, customer, demo, or workspace; rebind exported Fabric artifacts to a new workspace. Triggers: 'deploy the catalog', 'deploy this solution', 'install the PBI catalog', 'set up the tenant catalog', 'deploy to my tenant', 'roll this out to a customer'. DO NOT USE FOR: exporting or backing up an existing workspace (use fabric-export-definitions), authoring new Fabric items, or running data queries."
---

# Deploy the PBI Tenant Catalog

Publish the whole solution into a target Fabric workspace in any tenant and rewire every
cross-artifact reference so nothing points back at the source workspace.

## What gets deployed

Five items, created in dependency order. The pipeline is executed **mid-deployment**:

```
Lakehouse -> Notebook -> DataPipeline -> [RUN PIPELINE] -> SemanticModel -> Report
```

The run must happen before the semantic model is created. All 11 model tables are
Direct Lake `entity` partitions bound to the `pbi_*` Delta tables the notebook writes.
A model created before those tables exist deploys "successfully" but fails on query.

`SQLEndpoint` is auto-provisioned with the Lakehouse and is never created directly.

## Before running — confirm with the user

1. **Which tenant.** `az login --tenant <tenant>` must already point at the target.
   Verify with `az account show`. Pass `-ExpectedTenantId` to hard-fail on a mismatch.
2. **Which capacity.** Required for both Direct Lake and notebook execution. If the
   workspace does not exist yet, `-CapacityName` or `-CapacityId` is mandatory.
3. **Scan scope.** A tenant-wide scan needs the caller to be a **Fabric Administrator**
   plus these tenant settings ON (Admin portal -> Tenant settings -> Admin API settings):
   - *Enhanced admin APIs responses with detailed metadata*
   - *Enhanced admin APIs responses with DAX and mashup expressions*

   Without them the notebook silently falls back to user mode and only scans workspaces
   the caller can access. Deployment still succeeds — coverage is just narrower. Say so
   rather than letting the user discover it in the report.

## How to run

Always run the plan first and show it to the user before deploying:

```pwsh
pwsh -File ./Deploy-PBICatalog.ps1 -WorkspaceName "PBI Catalog" -CapacityName "<capacity>" -PlanOnly
```

Then deploy:

```pwsh
pwsh -File ./Deploy-PBICatalog.ps1 -WorkspaceName "PBI Catalog" -CapacityName "<capacity>"
```

Key parameters:

- `-WorkspaceName` — created if missing (needs a capacity). Or `-WorkspaceId` for an existing one.
- `-CapacityName` / `-CapacityId` — capacity to bind a newly created workspace to.
- `-ExpectedTenantId` — abort if the signed-in tenant differs. Use this for customer work.
- `-SkipPipelineRun` — deploy artifacts only; model and report will have no data.
- `-PipelineTimeoutMinutes` — default 120. A large tenant scan can take a while.
- `-DefinitionsPath` — defaults to `./definitions`.

The scan pipeline is long-running. Run it in a terminal that can stay open, and warn the
user it may take tens of minutes on a large tenant.

## Rebinding

Nothing is hard-coded. The script recovers the source workspace id from the notebook's
lakehouse binding, the source workspace name from the report connection string, and the
item list from `_items_inventory.json`.

| File | Rebound |
|---|---|
| `notebook-content.py` | `default_lakehouse`, `default_lakehouse_workspace_id` |
| `pipeline-content.json` | `notebookId`, `workspaceId` |
| `expressions.tmdl` | OneLake path `.../{workspaceId}/{lakehouseId}` |
| `definition.pbir` | `semanticmodelid`, workspace **name** in the connection string |

**The workspace name is replaced only inside `definition.pbir`.** A global replace would
also rewrite matching text in visual titles and text boxes across the report's ~110
parts. GUIDs are safe to replace globally; the display name is not. Do not "simplify"
this into a single global map.

Binary parts are matched by extension and passed through untouched so images in
`StaticResources` are never corrupted.

## Interpreting the outcome

- Per-item failures are collected, not fatal; the script exits 1 if any item failed.
- A failed pipeline run warns and continues, so the artifacts still land. Re-run the
  pipeline from the portal, then refresh the semantic model.
- After a successful run the script prints the workspace URL and the count of `pbi_*`
  tables found. **Zero tables means the report will be empty** — check the notebook run
  and the admin tenant settings above.

## Pitfalls

- Deploying into an existing workspace that already has these items creates duplicates;
  the script does not update in place. Confirm the workspace is empty or intended.
- Direct Lake and notebook execution both require an active capacity. If the capacity is
  paused, item creation fails with misleading 400/404 errors.
- Re-running is not idempotent. To redeploy cleanly, delete the items (or the workspace)
  first.
