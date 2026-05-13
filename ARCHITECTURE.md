MarTech Fabric Pipeline - Infrastructure Overview

Purpose
-------
This document describes the Infrastructure-as-Code (IaC) artifacts included in this repository for the MarTech Retail Data Pipeline (data product). The implementation uses Azure ARM templates to provision resources aligned to a Medallion data architecture (raw / silver / gold) and follows Azure Well-Architected principles.

High-level architecture
-----------------------
- ADLS Gen2 Storage Account (hierarchical namespace): stores data using medallion containers/paths (raw, silver, gold).
- Azure Data Factory: orchestration and connectors for ingestion and ETL/ELT between medallion layers.
- Azure Key Vault: secure storage of secrets used by pipelines and services.
- Log Analytics / Monitoring: central workspace to collect pipeline and platform telemetry and diagnostics.

Repository IaC layout
---------------------
infra/arm-templates/
- main.json                  : Master ARM template that composes modules and accepts environment/parameters.
- modules/storage.json       : Storage account + container/folder structure for medallion layers; network ACLs and TLS settings.
- modules/data-factory.json  : Data Factory resource with parameterized linked services (DB types, connectors).
- modules/keyvault.json      : Key Vault resource enabling RBAC and secure secret usage.
- modules/monitoring.json    : Log Analytics workspace and diagnostic settings.
- parameters/*.parameters.json : Environment-specific parameter files (dev, prod) referencing Key Vault for secrets.

Scripts and CI/CD
-----------------
infra/scripts/deploy.sh       : Convenience script to validate and deploy ARM templates using Azure CLI.
cicd/azure-devops/azure-pipelines.yml : Example Azure DevOps pipeline that runs tests then deploys IaC (TDD-first).

Deployment (example)
--------------------
Prerequisites:
- Azure CLI logged in with sufficient permissions to create resource groups and resources.
- An existing resource group or create one (example RG name is used by the script): rg-martech-<env>

Quick deploy (dev):
1. Ensure parameter file infra/arm-templates/parameters/dev.parameters.json is configured to point to your Key Vault when using Key Vault references.
2. Run the deploy script:
   ./infra/scripts/deploy.sh dev postgres

Under the hood the script runs:
- az deployment group validate --resource-group <rg> --template-file infra/arm-templates/main.json --parameters @infra/arm-templates/parameters/dev.parameters.json --parameters dbConnectorType=postgres
- az deployment group create (when validation passes)

Parameters and environments
---------------------------
- environment: dev | staging | prod
- projectName: resource name prefix
- location: Azure region (defaults to resource group location)
- dbConnectorType: postgres | sqlserver | mysql — used to parameterize Data Factory linked service type and secrets
- enableAnalyticsConnectors / enableEcommerceConnectors: arrays to enable optional connector templates

Security and Ops notes
----------------------
- Key Vault is configured with RBAC and soft delete enabled. Secrets should be stored in Key Vault and referenced in parameter files to avoid plaintext credentials.
- Storage account is created with hierarchical namespace (HNS) enabled for ADLS Gen2, HTTPS-only, and minimum TLS 1.2.
- Network ACLs default to Deny in templates; adjust allowed IP ranges or private endpoints as required for your network topology.
- Log Analytics workspace is provisioned and diagnostic settings configured for pipeline telemetry. Retention and SKU can be tuned in the module.

Aligning to Azure Well-Architected Framework
-------------------------------------------
This IaC follows core Azure WA recommendations:
- Reliability: parameterized environments, templates validate before deployment, diagnostic settings for observability.
- Security: Key Vault for secrets, RBAC, network ACLs on storage, TLS enforcement.
- Cost optimization: SKU selection mapped by environment (dev -> lower-cost SKU) in storage module variables.
- Operational excellence: CI/CD pipeline example and deployment scripts encourage repeatable, test-first deployments.

Extending or migrating
----------------------
- To add resources, prefer creating a new ARM module under infra/arm-templates/modules and include a parameter/output contract so main.json can compose it.
- If you prefer Terraform or Bicep, you can translate these ARM modules; keep parameter and output contracts similar to preserve deployment automation.

Files changed
-------------
- Added: ARCHITECTURE.md (this document)

Limitations and next steps
--------------------------
- This repository currently provides ARM templates as the primary IaC. If the team standardizes on Terraform/Bicep, a migration should be planned and tested.
- Networking examples (private endpoints, VNET integration) are intentionally minimal; production deployments should integrate platform network controls and service endpoints.

Contact
-------
For questions about architecture and IaC patterns in this repo, contact the MarTech Platform Team.
