# **DB2 → Azure SQL Migration Automation Tool**

# Business Use Case Playbook

| Audience | Migration Engineers, Cloud DBAs, Solution Architects, Delivery Leads |
| :---- | :---- |
| Version | 1.5.0 |
| Last Updated | 2026-08-11 |

---

DB2 → Azure SQL Migration Automation Tool	1

Business Use Case Playbook	1

[Executive Summary	3](#executive-summary)

[1. Generic Solution vs. Particular problem	4](#1-generic-solution-vs-particular-problem)

[1.1 The Problem we solve	4](#11-the-problem-we-solve)

[1.2 Why a Generic Tool Does Not Fit	4](#12-why-a-generic-tool-does-not-fit)

[1.3 Generic Solution Characteristics	5](#13-generic-solution-characteristics)

[2. Architecture	6](#2-architecture)

[2.1 High Level Architecture	6](#21-high-level-architecture)

[2.2 Component Mapping	6](#22-component-mapping)

[2.3 Integration Points	6](#23-integration-points)

[2.4 Security	6](#24-security)

[2.5 Deployment Process	7](#25-deployment-process)

[3. Approach To The Solution	7](#3-approach-to-the-solution)

[3.1 Design Principles	7](#31-design-principles)

[3.2 Solution Approach (Phased Delivery)	9](#32-solution-approach-phased-delivery)

[3.3 The  Migration Workflow	9](#33-the-migration-workflow)

[4. Quantitative Benefits	9](#4-quantitative-benefits)

[4.1 Effort & Time Savings (Per Database)	9](#41-effort--time-savings-per-database)

[4.2 Conservative Rollup	9](#42-conservative-rollup)

[4.3 Project level Impact	9](#43-project-level-impact)

[4.4 Throughput & Revenue / Cost Avoidance	10](#44-throughput--revenue--cost-avoidance)

[4.5 Quality & Risk Reduction	10](#45-quality--risk-reduction)

[4.6 Resource Optimization	10](#46-resource-optimization)

[5. Reusability & Scale Model	10](#5-reusability--scale-model)

[5.1 What Gets Reused Every Migration	10](#51-what-gets-reused-every-migration)

[5.2 Scaling Dimensions	10](#52-scaling-dimensions)

[1. Horizontal Scaling (Scale-Out)	10](#1-horizontal-scaling-scale-out)

[2. Vertical Scaling (Scale-Up)	10](#2-vertical-scaling-scale-up)

[3. Network & Bandwidth Scaling	11](#3-network--bandwidth-scaling)

[6. Industry Alignment & Additional Playbook Factors	12](#6-industry-alignment--additional-playbook-factors)

[7. Risks, Assumptions & Mitigations	13](#7-risks-assumptions--mitigations)

[Technical Risks & Automated Mitigations	13](#technical-risks--automated-mitigations)

[8. Success Metrics & Governance	14](#8-success-metrics--governance)

[8.1 KPIs to Track	14](#81-kpis-to-track)

[8.2 Governance Cadence	16](#82-governance-cadence)

[9. Next Steps	16](#9-next-steps)

[10. Appendices	16](#10-appendices)

[10.1 Related Artifacts	16](#101-related-artifacts)

[10.2 Glossary	18](#102-glossary)

---

# Executive Summary

The DB2 → Azure SQL Migration Automation Tool unifies existing SQL Server, DB2, and Azure capabilities into a single Windows desktop workflow. It simplifies and automates end-to-end cloud migration through an intuitive, interactive user interface.

The tool optimizes the migration lifecycle across four critical phases:

* **High-Speed Backup:** Dynamically calculates and generates split backup files based on database size, reducing on-premises SQL Server backup windows before securely transferring assets to Azure Blob Storage.
* **Automated Staging:** Streams and restores backup files directly into a secure Azure SQL Managed Instance staging environment (or restores schema/data into Azure SQL targets via Full Migration / BCP / ADF paths).
* **Rigorous Validation:** Executes automated schema verification and precise record-count checks to guarantee data integrity between source and staging/destination databases.
* **Seamless Cutover:** Facilitates the final transition of validated databases to the production Azure SQL Managed Instance target for application consumption.

By consolidating these steps into a guided workflow, the tool eliminates manual friction, minimizes risk, and accelerates cloud adoption.

| Dimension | Without tool | With Tool | Improvements observed |
| :---- | :---- | :---- | :---- |
| Avg. effort per database | 32–40 hrs | 12–16 hrs | ~60% reduction |
| Calendar time per database | 4–5 business days | 1–2 business days | ~60–70% faster |
| Rework Cycles (Schema/Validation) | 2–3 per wave | 0–1 per wave | ~60–70% fewer |
| Parallel migration streams | 1 (manual) | 2–3 (tool-enforced) | 2–3 x throughput |
| Audit trail completeness | Partial/Scattered | Packaged logs + GitHub Actions artifacts + registry | Full Traceability |

> Figures above are target/illustrative estimates aligned to the original playbook ranges. Replace with measured wave data once available.

# 1. Generic Solution vs. Particular problem

## 1.1 The Problem we solve

Cloud database migrations are plagued by operational friction, security risks, and prolonged downtime. This tool directly addresses these critical pain points:

* **Eliminates Tool Fragmentation:** Migrations typically require switching between multiple disjointed utilities for backup, transfer, validation, and restore. This tool consolidates these features under a single, interactive umbrella to eliminate workflow context-switching.
* **Overcomes Backup Bottlenecks:** Large database backups take too long and frequently fail. The tool solves this by dynamically calculating database size and generating parallel, multi-file backups to drastically reduce backup windows.
* **Removes Manual Data Validation Risks:** Manual schema verification and row-count comparisons are slow and prone to human error. The tool automates these checks between source and staging environments to guarantee data integrity before final cutover.
* **Minimizes Production Downtime:** Migrating directly to production risks unforeseen compatibility issues and extended application blackouts. Using an isolated Azure SQL Managed Instance staging area allows validation to happen offline, ensuring the final production push is fast, safe, and predictable.

## 1.2 Why a Generic Tool Does Not Fit

Generic migration tools are built for broad compatibility, making them fundamentally inadequate for this highly optimized, ecosystem-specific workflow. A generic tool fails for several distinct reasons:

* **Lacks Native Ecosystem Integration:** Generic tools treat Azure and SQL Server as generic endpoints. They cannot natively tap into proprietary SQL Server backup mechanisms or seamlessly orchestrate Azure Blob Storage to Azure SQL Managed Instance (MI) staging environments.
* **Incapable of Smart Parallel Backup Allocation:** Standard tools use uniform data-transfer protocols. They lack the built-in database intelligence to dynamically analyze SQL DB sizes and automatically calculate the optimal number of split-backup files required to maximize throughput.
* **Fails on Automated Two-Tier Database Validation:** Off-the-shelf software does not feature automated schema and exact record-count cross-checking purpose-built for SQL/DB2-to-Azure-MI parity. Users would still have to script validations manually.
* **Missing the Isolated Staging-to-Target Pipeline:** Generic solutions generally push data straight from source to destination. They do not support a structured, isolated intermediate staging environment that allows validation to happen safely before the final application cutover.

## 1.3 Generic Solution Characteristics

A generic (off-the-shelf) migration solution typically has these characteristics — and those are exactly why it underfits this use case:

| Characteristic | Generic tool behavior | Why it falls short here |
| :---- | :---- | :---- |
| Broad connector coverage | Many source/target pairs with lowest-common-denominator features | Misses SQL Server `BACKUP TO URL` / striped `.bak`, MI-specific restore rules, and DB2 JDBC paths |
| Agent / middleware heavy | Extra translation or agent layers | Adds operational overhead; this tool prefers native platform capabilities |
| One-shot cutover bias | Push source → target with limited intermediate control | No first-class staging sandbox + automated parity gates before production |
| Manual validation add-ons | Leaves schema/row checks to scripts or separate products | Validation must be embedded in the same interactive run |
| Coarse auth model | Shared secrets or long-lived keys | Needs Entra MFA, Windows Auth, ephemeral SAS / Managed Identity patterns |
| Limited run packaging | Logs scattered across consoles | Needs per-run folders, redacted logs, and exportable validation artifacts |

**This playbook's particular solution** is therefore a purpose-built Windows orchestration client that:

1. Reuses native SQL Server / Azure primitives (no custom middleware).
2. Enforces backup → blob → staging/restore → validate → cutover sequencing.
3. Packages identity, logging, and validation into one operator-facing workflow.

# 2. Architecture

## 2.1 High Level Architecture

See the architecture diagram in the original playbook PDF / `Playbook Template.md`, and the detailed Mermaid diagrams in `docs/ARCHITECTURE.md`.

Logical flow:

`On-Premises SQL (or DB2) → Azure Blob Storage (for .bak path) → Azure SQL MI Staging / Target → Validation → Application Consumption`

Desktop app layers:

* **GUI** – Tkinter tabs (Projects, Backup & Restore, Full Migration, Schema, Validation, MI PITR)
* **Orchestration** – Full migration pipeline and backup/restore runners
* **Services** – Schema backup/restore, data migration (pyodbc / BCP / ADF), Azure Blob, MI PITR
* **Auth / Config** – Entra MFA (MSAL), Windows Auth, project folders, run metadata

## 2.2 Component Mapping

| Playbook component | Product module / artifact | Responsibility |
| :---- | :---- | :---- |
| Interactive control plane | `gui/main_window.py` + tabs | Operator UI, shared connections, menus, status |
| Project / run registry | Project folder + `project.json` + `migration_runs/<run_id>/` | Persist backups, logs, meta per run |
| Dynamic backup / striping | `src/backup/bak_to_blob.py`, local backup+upload | Size-aware multi-file `.bak` to Blob |
| Schema export / restore | `src/backup/schema_backup.py`, `src/restore/schema_restore.py` | DDL/metadata backup; skeleton then final schema apply |
| Data movement | `src/migration/data_migration.py`, BCP preflight, ADF client | Row/batch, BCP, or ADF pipeline loads |
| Staging / MI operations | Backup & Restore tab, MI PITR modules | Blob restore into MI; point-in-time restore between MIs |
| Schema validation | Schema Validation tab + compare/repair services | Structural compare, repair scripts, Excel export |
| Data validation | Data Validation + Legacy DB2 validation | Row counts / row-level checks (SQL or DB2) |
| Identity & secrets | `azure_token_cache.py`, Key Vault client, redact helpers | Entra MFA, SAS lifecycle, secret redaction |
| Packaging / deploy | `build_exe.py`, NSIS installer, GitHub Actions | Versioned exe + setup for Windows users |

## 2.3 Integration Points

| System | Integration method | Purpose |
| :---- | :---- | :---- |
| SQL Server (on-prem / Azure VM) | ODBC 17/18 (pyodbc), BCP, Windows Auth | Source backup, schema export, data copy |
| Azure SQL Database / Managed Instance | ODBC + Entra auth | Destination restore, full migration, validation |
| Azure Blob Storage | `BACKUP TO URL` / `RESTORE FROM URL`, azure-storage-blob, SAS or Managed Identity | Landing zone for striped backups |
| Microsoft Entra ID | MSAL / azure-identity (MFA, password, broker, CLI reuse) | Cloud identity for Blob, MI, ARM, ADF |
| Azure Data Factory | azure-mgmt-datafactory + `ADF_Lookup` control pattern | Optional bulk load orchestration |
| Azure Key Vault | Secret retrieval | ADF / control-DB credentials when configured |
| Azure ARM | REST with bearer token | MI PITR restore operations |
| IBM DB2 | jaydebeapi + JDBC (`db2jcc4.jar`), optional PySpark | Legacy schema/data validation and compare |
| CI / artifact store | GitHub Actions build workflow | Build, tag, and retain installer artifacts |

## 2.4 Security

Because this tool orchestrates the movement of critical enterprise data assets, security is deeply integrated into every phase of the pipeline. The tool enforces rigid identity verification and platform security across data isolation, identity management, and transit mechanics.

**1. Zero-Trust Identity & Authentication Controls**

* **MFA-Enforced Cloud Access:** Access to all target cloud infrastructure—including **Azure Blob Storage** and **Azure SQL Managed Instances**—is strictly governed by Microsoft Entra ID (Azure AD) requiring **Multi-Factor Authentication (MFA)**. The tool integrates with interactive authentication workflows to prompt administrators for secondary validation before initiating cloud-side tasks.
* **On-Premises Windows Authentication:** To ensure local alignment with enterprise domain security, the tool connects to the source database engine utilizing native **Active Directory / Windows Authentication**. This avoids the use of legacy SQL Server authentication logins, leveraging the existing kerberos/NTLM security context of the authorized execution account.

**2. Data-at-Rest Protection & Storage Security**

* **Encrypted Blob Storage Ingestion:** The temporary landing zone in **Azure Blob Storage** enforces mandatory 256-bit Advanced Encryption Standard (AES-256) Storage Service Encryption (SSE).
* **MFA-Gated Shared Access Signatures (SAS):** Time-bound User Delegation SAS tokens with minimum required permissions (**Write** for backup, **Read** for restore) are generated on the fly, but can only be issued after an administrative user has successfully cleared the **Entra ID MFA prompt**.
* **Staging Area Isolation:** The **Azure SQL Managed Instance Staging Area** resides in a dedicated, isolated network perimeter. Transparent Data Encryption (TDE) is enabled by default to secure the intermediate validation copies.
* **Operational hardening in product:** Encrypted ODBC connections, log secret redaction, SAS create/revoke lifecycle, and Azure compatibility filters for unsupported objects.

## 2.5 Deployment Process

**End-user deployment**

1. Obtain `AzureMigrationTool_Setup_<version>.exe` (NSIS) or the standalone exe from the build pipeline.
2. Run the installer (per-user or all-users). Optional bundled prerequisites: ODBC Driver 18, Java 17, SqlCmd/BCP utilities.
3. Launch the app; if ODBC is missing, use **Tools → Install database driver**.
4. Create or open a **Project** folder (stores `backups/`, `migrations/`, `restores/`, `validation/`, `logs/`, `project.json`).
5. Configure source/destination connections (Windows Auth and/or Entra MFA).
6. Execute the chosen workflow tab (Backup & Restore, Full Migration, Schema/Data Validation, MI PITR).

**Developer / build deployment**

```bash
pip install -r azure_migration_tool/requirements.txt
python -m azure_migration_tool.main
```

Build path: `python build_exe.py` → `installer/build_installer.ps1` → versioned setup exe. CI on `main` publishes build artifacts (retained per workflow policy).

**Runtime data location**

* Preferred: selected project path
* Fallback app data: `%LOCALAPPDATA%/AzureMigrationTool`

# 3. Approach To The Solution

## 3.1 Design Principles

To maintain a resilient, secure, and intuitive migration lifecycle, the tool is engineered against five core pillars aligned with the Microsoft Azure Well-Architected Framework:

**1. Simplicity & Interactive Governance (User-Centric Design)**

* **Single-Pane Orchestration:** Consolidates disjointed migration tasks—such as backup generation, storage movement, data loading, and integrity validation—into a unified interactive user dashboard.
* **Declarative Workflow Guidance:** Replaces script-heavy execution with a step-by-step interactive flow that abstracts complex cloud operations, minimizing human configuration error during execution.

**2. Native Feature Reutilisation (Efficiency Over Reinvention)**

* **Zero Custom Middleware:** Avoids proprietary, resource-heavy translation layers by directly invoking native SQL Server backup primitives and Azure platform capabilities.
* **Ecosystem Parity:** Maximizes compatibility and speed by preserving native engine behavior (e.g., striped `.bak` formats) to guarantee that backup and restore patterns remain fully supported by standard Microsoft SLA frameworks.

**3. High Performance through Intelligent I/O Scaling (Scalability)**

* **Dynamic File Optimization:** Analyzes source database sizing profiles before execution to dynamically compute the optimum number of parallel database files required to maximize local I/O performance.
* **Concurrent Multi-Threading:** Bypasses sequential bandwidth limits by splitting large monolithic storage operations into parallel streams, dramatically lowering the overall migration window.

**4. Strong Identity Gating & Explicit Trust (Security)**

* **MFA-Protected Control Plane:** Enforces modern identity guardrails by wrapping all Azure target interactions (Blob Storage and Managed Instances) behind mandatory Multi-Factor Authentication (MFA).
* **Domain-Aligned Local Boundaries:** Adheres to enterprise on-premises perimeter patterns by utilizing native Active Directory Windows Authentication, eliminating the storage or exposure of plaintext administrative passwords.

**5. Risk Isolation & Automated Verification (Data Quality & Integrity)**

* **Non-Disruptive Sandboxing:** Incorporates an isolated intermediate Azure SQL Managed Instance staging layer to host, extract, and test incoming assets out-of-band before any live production assets are touched.
* **Automated Data Parity Gates:** Implements an automated code-driven validation layer that checks structural schemas and absolute database record counts, turning final production cutovers into predictable, user-approved events.

## 3.2 Solution Approach (Phased Delivery)

Delivery is organized so each phase produces a usable operational capability without waiting for the full estate to finish:

| Phase | Capability delivered | Primary product surface | Exit criteria |
| :---- | :---- | :---- | :---- |
| **Phase 0 – Foundation** | Installer, ODBC/Java/BCP checks, project model, Entra/Windows auth | Setup + Projects tab | Operator can connect to source and Azure targets |
| **Phase 1 – Backup landing** | Striped `.bak` to Blob; restore from URL into MI staging | Backup & Restore | Backup lands in Blob; restore succeeds in staging |
| **Phase 2 – Schema + data path** | Schema backup → table skeletons → data load → final constraints | Full Migration (+ BCP/ADF options) | Destination objects + data loaded for pilot DBs |
| **Phase 3 – Validation gates** | Schema compare/repair; data/row validation (SQL & DB2 legacy) | Schema/Data Validation tabs | Schema delta = 0; row-count variance = 0 (or accepted exceptions) |
| **Phase 4 – Cutover & ops** | Promote validated DB to production MI; MI PITR fallback; run packaging | Cutover runbook + MI PITR | Apps consume target; rollback path documented |
| **Phase 5 – Scale-out** | Multi-DB waves, parallel streams, governance cadence | Wave planning + KPIs | Measured throughput and audit completeness for the program |

ADF Migration (Tools menu) is an optional advanced path for environments already standardized on Data Factory.

## 3.3 The  Migration Workflow

See workflow diagram in the original playbook / `Playbook Template.md`. End-to-end operator path:

1. **Prepare** – Open project; confirm disk, VPN/ExpressRoute, Entra MFA readiness, target capacity.
2. **Connect** – Authenticate to source (Windows Auth / SQL) and Azure (Entra MFA).
3. **Backup** – Run size-aware striped backup to Azure Blob (or schema backup for Full Migration path).
4. **Restore / Load** – Restore into Azure SQL MI staging **or** run Full Migration (skeletons → data → final schema).
5. **Validate** – Automated schema match and row-count (and optional deeper data validation).
6. **Approve cutover** – Only after parity gates pass; switch application connection strings to production target.
7. **Close out** – Retain run logs/meta; revoke ephemeral SAS; tear down unused staging to control cost.

Linear storage lifecycle (backup path):

`[On-Premises SQL] → [Azure Blob Storage] → [Azure SQL MI Staging Area] → [Final Target SQL MI] → [Application Consumption]`

# 4. Quantitative Benefits

> Replace illustrative hours with measured stopwatch data from pilot waves. Module hours below map to the product's major workstreams.

## 4.1 Effort & Time Savings (Per Database)

| Module / activity | Without tool (hrs) | With tool (hrs) | Delta saved (hrs) | Notes |
| :---- | ---: | ---: | ---: | :---- |
| Backup sizing, scripting & multi-file backup | 4–6 | 0.5–1 | ~3.5–5 | Dynamic striping + Backup & Restore tab |
| Transfer to Azure / Blob credentialing | 2–3 | 0.5–1 | ~1.5–2 | SAS/MI automation vs manual scripts |
| Restore into staging / target | 3–5 | 1–2 | ~2–3 | Guided restore / Full Migration orchestration |
| Schema verification & repair cycles | 6–8 | 1–2 | ~5–6 | Schema Validation + repair scripts |
| Row-count / data validation | 4–6 | 1–2 | ~3–4 | Data Validation / legacy DB2 checks |
| Cutover coordination & rework | 4–6 | 1–2 | ~3–4 | Staging gate reduces late surprises |
| Logging, handoff, audit packaging | 2–3 | 0.5–1 | ~1.5–2 | Per-run folders + exports |
| **Total (illustrative)** | **32–40** | **12–16** | **~20–24** | ~60% effort reduction |

Calendar time typically compresses from **4–5 business days** to **1–2 business days** when dependencies (network, MFA, target capacity) are pre-cleared.

## 4.2 Conservative Rollup

| Metric | Manual | Tool-assisted | Delta |
| :---- | ---: | ---: | ---: |
| Man-hours per database (midpoint) | 36 hrs | 14 hrs | **22 hrs saved** |
| Effort reduction | — | — | **~61%** |
| Calendar days per database (midpoint) | 4.5 days | 1.5 days | **~67% faster** |
| Rework cycles per wave | 2.5 | 0.5 | **~80% fewer** |

Conservative program view: use the **low end of savings** (e.g., 18 hrs/DB) until three measured waves confirm the midpoint.

## 4.3 Project level Impact

| Planning input | Illustrative program example | How to compute |
| :---- | :---- | :---- |
| Databases in migration scope | **N = 40** (replace with actual inventory) | Count in-scope DBs |
| Hours saved per DB (conservative) | 18 hrs | From §4.2 low-end |
| Total hours avoided | **720 hrs** | `N × hours_saved` |
| FTE equivalent (at 160 hrs/month) | **~4.5 person-months** | `total_hours / 160` |
| Waves (at 2–3 parallel streams) | Fewer calendar weeks than serial | Throughput × wave size |

Update **N** and measured hours after the pilot wave; keep this section as the program's living ROI table.

## 4.4 Throughput & Revenue / Cost Avoidance

* **Throughput:** Moving from 1 serial stream to 2–3 tool-assisted streams increases completed DBs per week by ~2–3× (network and MI capacity permitting).
* **Cost avoidance:** Fewer DBA overtime hours; shorter dual-run periods for on-prem + cloud; earlier decommission of legacy hosting.
* **Staging cost control:** Tear down unused staging MI databases promptly after validation to avoid idle compute charges.
* **Rework avoidance:** Catching schema/row mismatches before cutover avoids expensive production rollback windows.

## 4.5 Quality & Risk Reduction

* Schema match objective for cutover readiness: **0** unexplained structural deltas.
* Row-count variance objective: **0** (or documented approved exceptions).
* Staging isolation keeps failed loads off production.
* Secret redaction + ephemeral SAS reduce credential leakage risk in logs and scripts.
* Azure compatibility filters reduce mid-restore failures from unsupported objects (CLR, Windows principals, etc.).

## 4.6 Resource Optimization

* Reuse one Windows operator workstation pattern + one project template across waves.
* Scale MI vCores up only for heavy restore windows; scale down after cutover.
* Prefer striped parallel backup during off-peak; throttle during business hours to protect production I/O and VPN/ExpressRoute.
* Parallelize independent databases; serialize only when sharing a constrained host disk or network path.
* Optional ADF path for estates already invested in factory-based bulk load capacity.

# 5. Reusability & Scale Model

## 5.1 What Gets Reused Every Migration

Reusable assets (durable playbook assets):

* **Application binary / installer** – Same migration tool build across waves.
* **Project folder template** – Standard layout: `backups/`, `migrations/`, `restores/`, `validation/`, `logs/`, `project.json`.
* **Connection profiles & auth patterns** – Entra tenant/app settings, Windows Auth conventions, Blob account naming standards.
* **Workflow runbooks** – Backup→Restore, Full Migration, Validation, Cutover checklists.
* **Validation rule packs** – Schema compare settings, approved skip lists (Azure-incompatible objects), Excel export templates.
* **ADF artifacts (if used)** – Pipeline definitions and `ADF_Lookup` control patterns under `docs/adf/`.
* **Governance cadence** – Daily sync / weekly wave planning meeting structure (§8.2).
* **KPI definitions** – Throughput, MFA latency, schema delta, row-count variance, cutover window.
* **Security baselines** – MFA required for cloud actions, SAS policy lifetimes, log redaction expectations.
* **Glossary & architecture docs** – Shared language for engineers and auditors.

## 5.2 Scaling Dimensions

**Enterprise Scalability: Scaling Options for the Migration Tool**

To handle everything from small departmental databases to massive multi-terabyte enterprise data warehouses, the tool features both horizontal and vertical scaling levers across every tier of the migration pipeline.

### 1. Horizontal Scaling (Scale-Out)

* **Concurrent Multi-Database Migrations:** The tool's orchestration engine can execute multiple database migrations simultaneously by assigning distinct database pipelines to isolated execution worker threads.
* **Storage Ingestion Parallelism:** **Azure Blob Storage** natively handles massive concurrent ingestion. The tool leverages this by creating separate, isolated folder paths inside the reusable container, allowing multiple parallel backups to stream into Azure at the same time without I/O cross-talk.
* **Multi-Instance Target Routing:** For massive migration waves, the tool can scale horizontally by distributing the landing workloads across multiple distinct **Azure SQL Managed Instance Staging Areas** running in parallel across different subnets or regions.

### 2. Vertical Scaling (Scale-Up)

* **Dynamic Backup Striping Calibration:** For ultra-large databases, the tool vertically scales on-premises throughput by increasing the split-file allocation count. For example, a 100GB database might use 4 parallel files, while a 2TB database will automatically scale up to 32 striped `.bak` files to fully saturate local storage and network I/O capacity.
* **On-Demand Compute Scaling for Azure SQL MI:** Both the staging and target **Azure SQL Managed Instances** can be vertically scaled up on the fly. Administrators can boost the vCore count, memory-optimized hardware configurations, or storage IOPS right before a massive data load, and scale them back down once validation and final application cutover are complete.

### 3. Network & Bandwidth Scaling

* **Bandwidth Throttling & Allocation Controls:** The tool includes interactive throughput scaling, allowing administrators to dial up network utilization during off-peak hours (saturating the **Site-to-Site VPN** or **ExpressRoute** line for maximum speed) or throttle it down during peak business hours to protect standard office traffic.

**5.3 What is Not Reused**

Evaluated fresh on every run to prevent data contamination, security breaches, or collation mismatches:

**1. Runtime Database Sizing & File Allocations**

* **The Backup Split-Count Integer:** Because database sizes fluctuate constantly, the tool must calculate the file stripe count (`NUMBER_OF_FILES` in T-SQL) at the exact moment of execution. A previously calculated count cannot be reused.
* **Storage Allocation Footprints:** The target data file sizes (`.mdf` and `.ldf`) and disk space allocations must be provisioned fresh on the Azure SQL Managed Instance based on live source metrics.

**2. Unique Transient Identifiers & Paths**

* **Storage Folder Prefixes (Blobs):** To prevent overwriting active data, every migration execution requires a unique, timestamped, or GUID-based folder path inside the Azure Blob Storage container (e.g., `.../migration-container/db_hr_prod_20260810/`).
* **Session ID Logs & State Tracking:** The execution session ID, telemetry tracking variables, and individual transaction log markers must be freshly minted to ensure accurate auditing in Azure Monitor.

**3. Identity Verification & Fresh Security Tokens**

* **MFA Session Confirmations:** Multi-Factor Authentication (MFA) relies on live, time-based challenges. An administrator cannot reuse a previous Entra ID MFA approval token; a fresh interactive prompt must be cleared for every migration window.
* **Ephemeral SAS Tokens:** The specific Shared Access Signature (SAS) tokens embedded into the SQL Server backup command are cryptographically tied to a strict, immediate expiration window and cannot be reused for subsequent databases.

**4. Baseline Parity Records (Validation Metrics)**

* **Target Schema Snapshots:** The precise schema tree state changes between versions and environments. The validation engine must generate a fresh object manifest model for comparison every single time.
* **Dynamic Table Record Counts:** The row-count integers used during the validation phase are highly volatile real-time metrics. The tool must run active queries against `sys.partitions` at the moment of validation rather than relying on cached counts.

# 6. Industry Alignment & Additional Playbook Factors

**Strategic Standards: Compliance Frameworks and Operational Readiness**

To ensure this specialized database migration tool meets rigid enterprise governance mandates, its technical workflows are designed to align directly with major global industry standards and operational cloud readiness frameworks.

**1. Strategic Industry Framework Alignment**

* **Azure Cloud Adoption Framework (CAF):** The tool directly mirrors the "Adopt" and "Migrate" phases of the Azure CAF. It enforces structured landing zones, utilizes platform-native tooling over third-party agents, and enforces tagging taxonomies during the storage and staging phases.
* **ISO/IEC 27001 & SOC 2 Compliance:** The system satisfies strict data handling security controls. By implementing **MFA-gated** administrative actions, enforcing **TLS 1.2/1.3** in transit, using **AES-256** storage encryption at rest, and recording immutable audit logs, it ensures clear chain-of-custody tracking.
* **GDPR & HIPAA Data Governance:** The isolated **Azure SQL Managed Instance Staging Area** provides an essential compliance barrier. Data privacy teams can execute automated compliance scanning, masking, or data scrubbing rules in the staging sandbox before a database is ever introduced to production target environments.

**2. Environmental Pre-Requisites & Dependencies**

* **Local Disk & Network I/O Saturation:** While the tool's file sizer automatically stripes backups for speed, the on-premises host must have sufficient storage I/O bandwidth to write multiple split `.bak` files concurrently without stalling existing production transactional workloads.
* **Active Directory Domain Trust:** The system account running the tool requires local network permission to execute native Windows Authentication against the source database engine, necessitating established line-of-sight to the local Domain Controller.
* **ExpressRoute/VPN Bandwidth Windows:** Network paths must have explicit Quality of Service (QoS) routing rules configured to allow steady, multi-threaded streaming to Azure Blob Storage without causing latency injection into concurrent business web services.

**3. Operational Risk Management & Rollback Strategy**

* **Non-Disruptive Safe Testing:** Because validation happens strictly inside an isolated staging database replica, the on-premises source engine continues to function as the live system of record. If validation fails, the staging database is dropped with **zero** impact on production.
* **Clean Fallback Execution:** The tool maintains absolute rollback security. Until the final interactive "Application Consumption" cutover is approved and network connection strings are switched, the original on-premises source database remains active and entirely unaffected.

# 7. Risks, Assumptions & Mitigations

## Technical Risks & Automated Mitigations

| Risk Identified | Potential Impact | Tool / Process Mitigation Strategy |
| ----- | ----- | ----- |
| **Network Interruption During Upload** | Corrupted `.bak` files or broken upload streams to **Azure Blob Storage**. | The migration tool leverages block blob chunking and built-in **retry logic with exponential backoff** to resume interrupted file transfers without restarting the entire backup window. |
| **Local Disk I/O Saturation** | Severe latency injection or performance degradation on the active on-premises production application. | The tool's **dynamic file sizer** caps maximum parallelism based on host core counts. It includes an interactive I/O throttling dial to limit concurrent disk writes during business hours. |
| **Schema or Data Mismatch Post-Load** | Corrupted or missing tables, data truncation, or row-count drops in the cloud target database. | The tool enforces an isolated **Azure SQL MI Staging Area Gate**. It runs automated, code-driven structural schema checks and exact row-count validations before revealing the cutover switch. |
| **MFA / Session Timeout** | Administrative friction or failed automation tasks if the interactive login session expires during a long-running transfer. | The tool separates the **MFA-gated control plane authentication** from the background data-movement service principals, allowing long-running data transfers to continue securely in the background. |
| **Cutover Failure / Extended Downtime** | Application blackout or broken database dependencies during the final deployment push. | Absolute **non-disruptive fallback execution**. The source database remains the live system of record. If staging validation fails, the staging sandbox is safely torn down with zero production impact. |

# 8. Success Metrics & Governance

## 8.1 KPIs to Track

**1. Performance & Velocity KPIs (Speed Optimization)**

* **Backup Creation Throughput (MB/s):** Tracks the local disk I/O write speed during parallel backup generation. Low throughput indicates storage bottlenecks on the on-premises host.
* **Network Transfer Rate (Gbps):** Measures the data ingestion speed from local storage to **Azure Blob Storage** via the VPN tunnel, ensuring maximum bandwidth utilization without network saturation.
* **Dynamic File Sizing Efficiency:** Measures the reduction in backup windows achieved by utilizing striped multi-file sets compared to historical single-stream backup baselines.
* **Time-to-Restore in Staging:** Captures the duration from the start of blob streaming to a fully restored operational state within the **Azure SQL Managed Instance Staging Area**.

**2. Security & Compliance KPIs (Identity & Access Assurance)**

* **MFA Challenge Latency:** Tracks the time taken for an administrator to clear the **Entra ID MFA prompt** upon interactive initiation, ensuring no workflow timeouts occur during high-privilege operations.
* **Authentication Failure Rate:** Monitors failed local **Windows Authentication** attempts or cloud token rejections, serving as an early indicator of credential expiration or directory sync lag.
* **Ephemeral Token Expiration Parity:** Measures the percentage of migrations completed safely within the designated time-bound window of the generated storage SAS tokens.

**3. Data Quality & Parity KPIs (Validation Reliability)**

* **Schema Match Delta:** Calculates the number of structural schema discrepancies detected during the automated validation check. The target objective for cutover readiness is always exactly **0**.
* **Row-Count Variance:** Captures the variance between the live source row counts and the newly restored staging area records. A variance of **0** must be locked before the cutover option becomes interactive.
* **Validation Runtime Duration:** Measures how long the tool takes to execute automated verification algorithms, ensuring data integrity checks do not unnecessarily extend the overall migration window.

**4. Business Impact & Availability KPIs (Downtime Minimization)**

* **Final Cutover Execution Window:** Tracks the exact clock duration between the user approving the target push and the database becoming ready for consumption on the production **Azure SQL Managed Instance**.
* **Application Interruption Window (Downtime):** Measures the total application blackout period required to update connection strings and resume normal business consumption.
* **Migration Rollback Frequency:** Monitors how often a migration fails validation in staging and requires a safe teardown, helping engineering teams isolate problematic on-premises schemas.

## 8.2 Governance Cadence

To prevent configuration drift, ensure continuous alignment with shifting Azure security baselines, and guarantee a high success rate across migration waves, a structured governance cadence must be maintained. This framework defines the exact review touchpoints and accountability structures for stakeholders.

**1. Daily Operational Sync (Migration Execution Windows)**

* **Frequency:** Daily during active migration waves (15-minute time box).
* **Participants:** Migration Engineers, Cloud Database Administrators, Local Sysadmins.
* **Objective:** Review immediate telemetry from the previous 24 hours. Identify any failed **Windows Authentication** attempts, check network throughput over the **VPN/ExpressRoute** tunnel, and ensure that completed staging environments are being torn down promptly to manage costs.

**2. Weekly Technical Wave Planning**

* **Frequency:** Weekly.
* **Participants:** Lead Migration Architect, Database Owners, Application Stakeholders, Security Team.
* **Objective:** Audit upcoming target databases. Review sizing metrics to ensure the tool's **dynamic file sizer** will have accurate resource allocations, confirm that target users have cleared **Entra ID MFA** onboarding, and verify that target **Azure Blob Storage** accounts have appropriate capacity expansion limits pre-arranged.

# 9. Next Steps

| Initiative | Owner (suggested) | Target window | Outcome |
| :---- | :---- | :---- | :---- |
| Pilot wave on 2–3 representative databases | Migration Lead | Wave 0 (2 weeks) | Measured before/after hours to replace illustrative §4 figures |
| Lock staging + validation gate checklist | Cloud DBA + Security | Before Wave 1 | Written cutover criteria (schema delta 0, row variance 0) |
| Production wave playbook dry-run | Delivery Lead | End of Wave 0 | Confirmed rollback and app cutover steps |
| Enable optional ADF path where standardized | Data Platform | As needed | Documented when to choose BCP vs ADF |
| Complete Settings UX ("coming soon") & retire or promote POC experiments | Engineering | Next minor release | Lower operator friction |
| Publish measured KPI dashboard from run logs | Ops / Engineering | After Wave 1 | Living ROI for leadership |
| Estate-wide inventory (final **N** for §4.3) | Program PMO | Ongoing | Accurate program rollup |

# 10. Appendices

## 10.1 Related Artifacts

To ensure seamless handoffs between technical engineering, compliance auditing, and senior leadership, the following functional and visual artifacts have been compiled into the master playbook archive:

**1. Strategic and Executive Artifacts**

* **Executive Summary:** A high-level overview documenting the unified migration ecosystem, tool purposes, and four-stage optimization lifecycle for executive review and business case alignment.
* **The Problem Statement:** A detailed breakdown mapping the operational bottlenecks solved by the tool, including tool fragmentation, validation human errors, and production cutover risks.
* **Strategic Fit & Solution Comparison:** An analytical chapter establishing why standard, off-the-shelf "one-size-fits-all" utilities are inadequate for platform-native SQL Server / DB2 to Azure SQL MI parallel migrations.

**2. Visual Architecture & Design Artifacts**

* **Enterprise Topology Diagram:** A formalized network deployment map matching enterprise blueprint standards, complete with dashed network boundary lines splitting **On-Premises** infrastructure from protected **Azure VNet Subnets**.
* **Database Data Lifecycle Diagram:** A streamlined data-flow rendering tracking physical storage transformations linearly across all operational milestones: `[On-Premises SQL] → [Azure Blob Storage] → [Azure SQL MI Staging Area] → [Final Target SQL MI] → [Application Consumption]`.
* **Functional UML Sequence Diagram:** A code-driven `mermaid` sequence matrix explicitly tracing step-by-step conditional logic branches, dynamic sizing calls, parallel stream tasks, validation loop failure barriers, and user-approved cutovers.

**3. Security, Control, and Compliance Assets**

* **Security & Identity Blueprint:** A dedicated security framework outlining **MFA-enforced Entra ID cloud access**, **On-Premises Windows Authentication** logic, AES-256 Blob storage cryptography, and ephemeral User Delegation SAS token controls.
* **Core Design Principles Pillars:** An engineering document articulating the architecture's adherence to the *Azure Well-Architected Framework*, emphasizing feature reutilisation, dynamic I/O scaling, and non-disruptive sandboxing.
* **Risks, Assumptions & Mitigations Matrix:** A multi-dimensional risk registry establishing pre-requisite conditions and pairing real-time failure points with automated programmatic counters.

**4. Operational & Framework Governance Assets**

* **Telemetry KPI Monitoring Framework:** An operational metric map defining precise health telemetry indicators categorized across performance speed, security authentication, data validation variance, and total application downtime.
* **Structured Governance Cadence:** A programmatic lifecycle table defining meeting structures, intervals, target participant groups, and distinct milestone review objectives.
* **Centralized Architectural Glossary:** A master technical terms dictionary standardizing definition models for critical resources to align team language.

## 10.2 Glossary

* **AES-256 (Advanced Encryption Standard 256-bit):** A symmetric-key encryption standard used by Azure Storage to protect data-at-rest against unauthorized physical access.
* **Application Consumption:** The final phase of the migration lifecycle where downstream business applications, APIs, or reporting tools are actively reading and writing data from the production database.
* **Azure Blob Storage:** A scalable object storage service used by the migration tool as a secure, temporary landing zone for striped database backup files.
* **Azure Cloud Adoption Framework (CAF):** A set of documentation, guidance, and best practices provided by Microsoft to help organizations shape and execute cloud deployment strategies safely.
* **Azure ExpressRoute:** A private, high-speed physical network connection that bypasses the public internet to securely link an on-premises data center directly to the Microsoft Azure cloud.
* **Azure Log Analytics Workspace:** A centralized cloud logging repository where the migration tool pipes immutable audit trails, execution logs, and validation metrics for compliance monitoring.
* **Azure SQL Managed Instance (MI):** A fully managed, highly compatible cloud database platform service that combines the broad SQL Server database engine features with cloud operational benefits.
* **BCP (Bulk Copy Program):** A command-line utility path used for high-volume bulk data load into the destination database.
* **Conditional Access:** A set of granular identity policies in Microsoft Entra ID used to enforce specific security signals—such as checking device compliance or requiring MFA—before granting resource access.
* **Data Definition Language (DDL):** The subset of SQL statements used to define, alter, and manage database structures (e.g., tables, indexes, views, and schemas).
* **Dynamic File Sizer:** The proprietary algorithm inside the migration tool that evaluates the live size of an on-premises database and automatically calculates the optimum number of split-backup streams.
* **Ephemeral SAS Token (Shared Access Signature):** A temporary, cryptographically signed URI string that grants highly restricted, time-bound read or write access to an Azure Blob Storage container without exposing account master keys.
* **Full Migration:** The orchestrated four-step schema + data pipeline (schema backup → table skeletons → data load → final schema restore).
* **Microsoft Entra ID:** Azure's cloud-based identity and access management service (formerly known as Azure Active Directory) used to authenticate users via modern methods like MFA.
* **MFA (Multi-Factor Authentication):** A multi-step account verification process requiring users to supply a password as well as an independent verification token before entering the cloud environment.
* **MI PITR:** Point-in-time restore between Azure SQL Managed Instances performed via Azure ARM.
* **Parallel Multi-File Backup (Striping):** The native process of writing a single database backup across multiple individual `.bak` files simultaneously, fully saturating local hardware I/O to dramatically reduce the backup window.
* **Schema Validation:** The automated, code-driven quality check that structurally compares table structures, indexes, foreign keys, constraints, and data types between the source and staging environments.
* **Site-to-Site VPN Tunnel:** An encrypted internet-protocol security (IPSec) network connection that securely links an on-premises network gateway to an Azure Virtual Network gateway.
* **Staging Area:** An isolated sandbox database deployment tier inside Azure SQL MI where databases are temporarily restored, evaluated, and validated before being promoted to production.
* **TDE (Transparent Data Encryption):** A built-in database security mechanism that automatically encrypts SQL database data-at-rest, log files, and backups in real time.
* **TLS 1.2/1.3 (Transport Layer Security):** Cryptographic protocols designed to provide end-to-end communications security over a network, used by the migration tool to encrypt all data-in-transit.
* **Windows Authentication:** A secure authentication framework that uses centralized active directory domain credentials instead of discrete database user logins to authorize operations.
