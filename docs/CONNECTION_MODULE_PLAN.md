# Data Connector Module - Connection Layer Planning Document

**Version:** 1.0
**Date:** 2025-07-13
**Status:** Planning Phase
**Scope:** Connection Layer Only (Source & Destination connectivity)

---

## 1. Purpose

Define how source and destination connections are established, configured, authenticated, and managed. This layer is purely about connectivity — not about reading/writing data, format conversion, or checkpointing.

---

## 2. Source Type Taxonomy

### 2.1 Database Sources (9 Types)

| Category | Databases | Connection Method |
|---|---|---|
| **PostgreSQL Family** | PostgreSQL, Aurora PostgreSQL, Redshift | Native driver (pgx) |
| **MySQL Family** | MySQL, Aurora MySQL, MariaDB | Native driver (go-sql-driver/mysql) |
| **Oracle** | Oracle Database | godror |
| **SQL Server** | SQL Server, Azure SQL | go-mssqldb |
| **IBM DB2** | DB2 LUW, z/OS | ibm-db/go_ibm_db |
| **Generic JDBC** | Any JDBC-compliant | database/sql + driver |

### 2.2 Object Storage Sources (1 Type)

| Type | Connection |
|---|---|
| **AWS S3** | AWS SDK v2 |

### 2.3 API Sources (1 Type)

| Type | Connection |
|---|---|
| **External REST API** | HTTP/HTTPS |

### 2.4 Destination Targets

| Type | Connection Method |
|---|---|
| **S3-Compatible** | S3 SDK / MinIO / GCS |
| **Azure Blob** | azblob SDK |
| **Any Database (JDBC)** | database/sql + native driver |
| **Generic Cloud Object Store** | Pluggable upload driver |

---

## 3. Connection Architecture

### 3.1 Connection Manager Responsibilities

```
+-----------------------------------------------------------------+
|                    CONNECTION MANAGER                            |
+-----------------------------------------------------------------+
|  1. Connection Pool Management (per job, per partition)         |
|  2. Authentication & Credential Resolution (Vault/Env/Config)   |
|  3. Connection Validation (Test before job start)               |
|  4. Driver Registration & Version Management                    |
|  5. Connection Lifecycle (Open, Validate, Close, Retry)         |
|  6. SSL/TLS Configuration per Source Type                       |
|  7. Timeout Management (Connect, Query, Idle)                   |
+-----------------------------------------------------------------+
```

### 3.2 Connection Configuration (Bare Minimum)

```yaml
# === Database (Any — PostgreSQL, MySQL, Oracle, SQL Server, DB2, JDBC) ===
connection:
  url: string              # Connection string (e.g. postgres://host:5432/db)
  username: string
  password_ref: string

# === S3-Compatible ===
connection:
  bucket: string
  region: string
  access_key_ref: string
  secret_key_ref: string
  endpoint: string         # Custom endpoint for MinIO/GCS; omit for AWS

# === Azure Blob ===
connection:
  container: string
  storage_account: string
  access_key_ref: string

# === REST API ===
connection:
  base_url: string
  auth_type: api_key|bearer|basic
  auth_value_ref: string
```

---

## 4. Connection Interfaces

### 4.1 Source Connector Interface (Connection Only)

```go
type SourceConnector interface {
    // Lifecycle
    Connect(ctx context.Context, config ConnectionConfig) error
    Disconnect(ctx context.Context) error
    TestConnection(ctx context.Context) error

    // Capabilities
    GetCapabilities() ConnectorCapabilities
}
```

### 4.2 Destination Connector Interface (Connection Only)

```go
type DestinationConnector interface {
    Connect(ctx context.Context, config DestinationConfig) error
    Disconnect(ctx context.Context) error
    TestConnection(ctx context.Context) error
    GetCapabilities() ConnectorCapabilities
}
```

---

## 5. Authentication & Secrets Management

### 5.1 Credential Resolution Order

```
1. Explicit in config (dev only)
2. Environment variable (CONN_<TYPE>_PASSWORD)
3. Secret Store Reference (vault:path/to/secret#key)
4. Cloud Provider Default Chain (IAM Role, Workload Identity)
5. ~/.aws/credentials, ~/.azure/credentials
```

### 5.2 Secret Store Integration (Pluggable)

| Provider | Interface |
|---|---|
| **HashiCorp Vault** | KV v2, AppRole auth |
| **AWS Secrets Manager** | IAM-based access |
| **Azure Key Vault** | Managed Identity |
| **GCP Secret Manager** | Workload Identity |
| **Kubernetes Secrets** | Volume mount / CSI driver |
| **Local File** | JSON/YAML file (dev) |

### 5.3 Rotation Handling

- Connections validate on each job start
- Long-running jobs: re-validate every N hours (configurable)
- On auth failure: retry with fresh credentials (max 3x)

---

## 6. Connection-Level Error Handling

### 6.1 Error Categories

| Category | Examples | Retry? |
|---|---|---|
| **TRANSIENT** | Network timeout, connection reset | YES |
| **AUTH** | 401, 403, token expired | YES (refresh first) |
| **CONFIG** | Invalid host, bad credentials | NO |

---

## 7. Connection-Level Metrics

| Metric | Type | Labels |
|---|---|---|
| `connector_connection_acquire_duration_seconds` | Histogram | source_type, job_id |
| `connector_connection_active` | Gauge | source_type, job_id |
| `connector_errors_total` | Counter | source_type, error_category |

---

**End of Document**

*This document covers only the connection layer — establishing, configuring, authenticating, and managing connections to sources and destinations.*
