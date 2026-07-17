# S3 -> Athena end-to-end test

`test_s3_athena_e2e.py` hits real AWS: it writes a parquet file to S3 with
`S3Connector`, registers it as an Athena/Glue table, then reads it back with
`AthenaConnector` and asserts the round-tripped rows match. It's skipped
automatically unless `E2E_S3_BUCKET` is set.

## Env vars

| Var | Required | Notes |
|---|---|---|
| `E2E_S3_BUCKET` | yes | Bucket the test writes to and cleans up after itself (deletes only keys under `<table_name>/`) |
| `E2E_ATHENA_OUTPUT` | yes | S3 path for Athena query results, e.g. `s3://your-bucket/athena-results/` |
| `AWS_REGION` | no (default `us-east-1`) | Must match the bucket's region |
| `E2E_ATHENA_DATABASE` | no (default `udi_e2e`) | Created automatically if missing |
| `E2E_ATHENA_WORKGROUP` | no (default `primary`) | Must already exist |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` | no | Only needed if not using an instance role / SSO profile / default credential chain |

## Run it

```bash
export E2E_S3_BUCKET=your-bucket
export E2E_ATHENA_OUTPUT=s3://your-bucket/athena-results/
uv run pytest tests/e2e -v
```

## Minimal IAM policy

Replace `your-bucket` below (data + query-results bucket; split into two
statements if you use separate buckets).

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3Data",
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation", "s3:DeleteObject"],
      "Resource": ["arn:aws:s3:::your-bucket", "arn:aws:s3:::your-bucket/*"]
    },
    {
      "Sid": "Athena",
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:GetWorkGroup",
        "athena:ListTableMetadata",
        "athena:GetTableMetadata",
        "athena:ListDatabases"
      ],
      "Resource": "*"
    },
    {
      "Sid": "GlueCatalog",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase",
        "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:DeleteTable",
        "glue:GetPartitions"
      ],
      "Resource": "*"
    }
  ]
}
```

## What it does

1. `S3Connector.load` writes one parquet (snappy) file to
   `s3://$E2E_S3_BUCKET/<random_table_name>/<batch_id>.parquet`.
2. Raw `boto3` Athena DDL (`CREATE DATABASE IF NOT EXISTS`, then
   `CREATE EXTERNAL TABLE ... LOCATION ...`) registers that path in the Glue
   Data Catalog — dumping files to S3 alone doesn't make them queryable.
3. `AthenaConnector.connect` + `.extract` runs a real Athena query and streams
   the result back as Arrow batches.
4. Asserts the rows read back match what was written.
5. Teardown drops the table and deletes the uploaded objects. The database
   and workgroup are left in place since they may be shared.
