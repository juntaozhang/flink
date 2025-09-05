# Flink Paimon on Kubernetes

This guide demonstrates how to set up and run Apache Paimon with Apache Flink on Kubernetes, including integration with MinIO for S3-compatible storage.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Download Dependencies](#download-dependencies)
- [Environment Setup](#environment-setup)
- [Testing with SQL](#testing-with-sql)
- [Data Inspection](#data-inspection)

## Prerequisites

- Kubernetes cluster with sufficient resources
- kubectl configured to access the cluster
- Docker for building custom images
- MinIO deployed in the cluster for S3-compatible storage
- Flink service account configured in Kubernetes

## Download Dependencies

### Flink Distribution
```bash
# Download Flink 1.17.2
wget https://archive.apache.org/dist/flink/flink-1.17.2/flink-1.17.2-bin-scala_2.12.tgz
```

### Paimon and Hadoop Dependencies
```bash
# Download Paimon connector and Hadoop shaded dependencies
wget https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-1.17/
```

### Setup Flink Libraries
```bash
# Copy required JARs to Flink lib directory
cp paimon-flink-*.jar <FLINK_HOME>/lib/
cp flink-shaded-hadoop-2-uber-*.jar <FLINK_HOME>/lib/
```

### Reference Documentation
- [Paimon Flink Quick Start](https://paimon.apache.org/docs/master/flink/quick-start/)
- [Flink S3 FileSystem](https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/deployment/filesystems/s3/)

## Environment Setup

### Build Docker Image

Build a custom Flink image with Paimon dependencies:

```bash
docker build -t flink:1.17.2-scala_2.12-paimon -f /Users/juntzhang/src/github/juntaozhang/flink/flink-1.17/.local/paimon/Dockerfile ./
```

### Deploy Flink Session Cluster on Kubernetes

Deploy Flink session cluster with S3 configuration for MinIO:

```shell
../flink-1.17.2/bin/kubernetes-session.sh \
-Dkubernetes.cluster-id=flink1 \
-Dkubernetes.container.image=flink:1.17.2-scala_2.12-paimon \
-Dkubernetes.service-account=flink-service-account \
-Dkubernetes.rest-service.exposed.type=LoadBalancer \
-Dkubernetes.containerized.master.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.17.2.jar \
-Dkubernetes.containerized.taskmanager.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.17.2.jar \
-Dfs.s3a.endpoint=http://minio.default.svc.cluster.local:9000 \
-Dfs.s3a.path.style.access=true \
-Dfs.s3a.connection.ssl.enabled=false \
-Dfs.s3a.access.key=minio \
-Dfs.s3a.secret.key=minio12345 \
-Dstate.checkpoints.dir=s3a://flink-bucket/checkpoints \
-Dstate.savepoints.dir=s3a://flink-bucket/savepoints
```

### Setup SQL Client

#### Option 1: Using YAML Configuration
```bash
kubectl apply -f flink1-paimon-sql-client.yaml
```

#### Option 2: Using kubectl run
```bash
kubectl run flink-client \
  --image=flink:1.17.2-scala_2.12-paimon \
  --restart=Never \
  --command -- bash -lc 'sleep infinity'
```

#### Connect to SQL Client
```bash
# Login to the pod
kubectl exec -it deployment/flink-sql-client -- bash
# or
kubectl exec -it pod/flink-client -- bash

# Start SQL client
bin/sql-client.sh embedded \
  -Dexecution.target=remote \
  -Dkubernetes.cluster-id=flink1
```

## Testing with SQL

### Create Catalog and Tables

Setup Paimon catalog and create test tables:

```sql

CREATE CATALOG paimon_catalog WITH (
'type' = 'paimon',
'warehouse' = 's3a://warehouse/paimon'
);
USE CATALOG paimon_catalog;
CREATE DATABASE IF NOT EXISTS ods;
use ods;

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10 s';


CREATE TEMPORARY TABLE src_order (
order_number BIGINT,
price DECIMAL(32, 2),
ts TIMESTAMP(3), WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
'connector' = 'datagen',
'rows-per-second' = '1',
'fields.order_number.kind' = 'sequence',
'fields.order_number.start' = '1',
'fields.order_number.end' = '1000000',
'fields.price.min' = '1',
'fields.price.max' = '100'
);

CREATE TABLE my_order (
window_start STRING,
order_number BIGINT,
total_amount DECIMAL(32, 2)
) WITH (
'sink.rolling-policy.file-size' = '1MB',
'sink.rolling-policy.rollover-interval' = '1 min',
'sink.rolling-policy.check-interval' = '10 s'
);

INSERT INTO my_order
SELECT
CAST(TUMBLE_START(ts, INTERVAL '10' SECOND) AS STRING) window_start,
order_number,
SUM(price) total_amount
FROM src_order
GROUP BY order_number, TUMBLE(ts, INTERVAL '10' SECOND);
```

### Batch Query

Switch to batch mode and query the data:

```sql
SET 'execution.runtime-mode' = 'batch';
SELECT count(1), sum(total_amount) FROM my_order;
```

## Data Inspection

### MinIO Data Verification

Verify data stored in MinIO using mc (MinIO Client):

**Reference:** [Paimon Data File Specification](https://paimon.apache.org/docs/master/concepts/spec/datafile/)

```bash
# Setup MinIO alias
mc alias set myminio http://minio.default.svc.cluster.local:9000 minio minio12345

# List Paimon table files
mc ls localminio/warehouse/paimon/ods.db/my_order1/

# View snapshot metadata
mc cat localminio/warehouse/paimon/ods.db/my_order1/snapshot/snapshot-9
```

```json
{
"version" : 3,
"id" : 9,
"schemaId" : 0,
"baseManifestList" : "manifest-list-c1897398-a6fe-45db-9086-7eec0f04a816-14",
"baseManifestListSize" : 1047,
"deltaManifestList" : "manifest-list-c1897398-a6fe-45db-9086-7eec0f04a816-15",
"deltaManifestListSize" : 985,
"changelogManifestList" : null,
"commitUser" : "6135b1dc-53d4-4e2b-ab01-44d89a5301f2",
"commitIdentifier" : 8,
"commitKind" : "APPEND",
"timeMillis" : 1756515590731,
"logOffsets" : { },
"totalRecordCount" : 16701,
"deltaRecordCount" : 10,
"changelogRecordCount" : 0,
"watermark" : 1756515584705,
"nextRowId" : 0
}
```

### Paimon Metadata Analysis

Analyze Paimon manifest files using Avro tools:

```bash
# Convert manifest list to JSON format
java -jar ~/.m2/repository/org/apache/avro/avro-tools/1.11.3/avro-tools-1.11.3.jar \
  tojson <(mc cat localminio/warehouse/paimon/ods.db/my_order1/manifest/manifest-list-c1897398-a6fe-45db-9086-7eec0f04a816-14)
```

#### Manifest List Output:
```
{"_VERSION":2,"_FILE_NAME":"manifest-7acac4bc-df14-4537-b80a-0eaebe776fd0-0","_FILE_SIZE":2133,"_NUM_ADDED_FILES":1,"_NUM_DELETED_FILES":0,"_PARTITION_STATS":{"_MIN_VALUES":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","_MAX_VALUES":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","_NULL_COUNTS":{"array":[]}},"_SCHEMA_ID":0,"_MIN_BUCKET":{"int":0},"_MAX_BUCKET":{"int":0},"_MIN_LEVEL":{"int":0},"_MAX_LEVEL":{"int":0}}
{"_VERSION":2,"_FILE_NAME":"manifest-766f9495-6622-4a85-9225-278d321a238b-0","_FILE_SIZE":2125,"_NUM_ADDED_FILES":1,"_NUM_DELETED_FILES":0,"_PARTITION_STATS":{"_MIN_VALUES":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","_MAX_VALUES":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","_NULL_COUNTS":{"array":[]}},"_SCHEMA_ID":0,"_MIN_BUCKET":{"int":0},"_MAX_BUCKET":{"int":0},"_MIN_LEVEL":{"int":0},"_MAX_LEVEL":{"int":0}}
{"_VERSION":2,"_FILE_NAME":"manifest-766f9495-6622-4a85-9225-278d321a238b-1","_FILE_SIZE":2126,"_NUM_ADDED_FILES":1,"_NUM_DELETED_FILES":0,"_PARTITION_STATS":{"_MIN_VALUES":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","_MAX_VALUES":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","_NULL_COUNTS":{"array":[]}},"_SCHEMA_ID":0,"_MIN_BUCKET":{"int":0},"_MAX_BUCKET":{"int":0},"_MIN_LEVEL":{"int":0},"_MAX_LEVEL":{"int":0}}
{"_VERSION":2,"_FILE_NAME":"manifest-766f9495-6622-4a85-9225-278d321a238b-2","_FILE_SIZE":2124,"_NUM_ADDED_FILES":1,"_NUM_DELETED_FILES":0,"_PARTITION_STATS":{"_MIN_VALUES":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","_MAX_VALUES":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","_NULL_COUNTS":{"array":[]}},"_SCHEMA_ID":0,"_MIN_BUCKET":{"int":0},"_MAX_BUCKET":{"int":0},"_MIN_LEVEL":{"int":0},"_MAX_LEVEL":{"int":0}}
{"_VERSION":2,"_FILE_NAME":"manifest-766f9495-6622-4a85-9225-278d321a238b-3","_FILE_SIZE":2124,"_NUM_ADDED_FILES":1,"_NUM_DELETED_FILES":0,"_PARTITION_STATS":{"_MIN_VALUES":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","_MAX_VALUES":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","_NULL_COUNTS":{"array":[]}},"_SCHEMA_ID":0,"_MIN_BUCKET":{"int":0},"_MAX_BUCKET":{"int":0},"_MIN_LEVEL":{"int":0},"_MAX_LEVEL":{"int":0}}
{"_VERSION":2,"_FILE_NAME":"manifest-766f9495-6622-4a85-9225-278d321a238b-4","_FILE_SIZE":2123,"_NUM_ADDED_FILES":1,"_NUM_DELETED_FILES":0,"_PARTITION_STATS":{"_MIN_VALUES":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","_MAX_VALUES":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","_NULL_COUNTS":{"array":[]}},"_SCHEMA_ID":0,"_MIN_BUCKET":{"int":0},"_MAX_BUCKET":{"int":0},"_MIN_LEVEL":{"int":0},"_MAX_LEVEL":{"int":0}}
{"_VERSION":2,"_FILE_NAME":"manifest-766f9495-6622-4a85-9225-278d321a238b-5","_FILE_SIZE":2126,"_NUM_ADDED_FILES":1,"_NUM_DELETED_FILES":0,"_PARTITION_STATS":{"_MIN_VALUES":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","_MAX_VALUES":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","_NULL_COUNTS":{"array":[]}},"_SCHEMA_ID":0,"_MIN_BUCKET":{"int":0},"_MAX_BUCKET":{"int":0},"_MIN_LEVEL":{"int":0},"_MAX_LEVEL":{"int":0}}
{"_VERSION":2,"_FILE_NAME":"manifest-766f9495-6622-4a85-9225-278d321a238b-6","_FILE_SIZE":2365,"_NUM_ADDED_FILES":1,"_NUM_DELETED_FILES":5,"_PARTITION_STATS":{"_MIN_VALUES":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","_MAX_VALUES":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000","_NULL_COUNTS":{"array":[]}},"_SCHEMA_ID":0,"_MIN_BUCKET":{"int":0},"_MAX_BUCKET":{"int":0},"_MIN_LEVEL":{"int":0},"_MAX_LEVEL":{"int":0}}
```

```bash
# Convert delta manifest list to JSON
java -jar ~/.m2/repository/org/apache/avro/avro-tools/1.11.3/avro-tools-1.11.3.jar \
  tojson <(mc cat localminio/warehouse/paimon/ods.db/my_order1/manifest/manifest-list-c1897398-a6fe-45db-9086-7eec0f04a816-15)
```

#### Delta Manifest List Output:


```json
{
  "_VERSION": 2,
  "_FILE_NAME": "manifest-766f9495-6622-4a85-9225-278d321a238b-7",
  "_FILE_SIZE": 2125,
  "_NUM_ADDED_FILES": 1,
  "_NUM_DELETED_FILES": 0,
  "_PARTITION_STATS": {
    "_MIN_VALUES": "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000",
    "_MAX_VALUES": "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000",
    "_NULL_COUNTS": {
      "array": []
    }
  },
  "_SCHEMA_ID": 0,
  "_MIN_BUCKET": {
    "int": 0
  },
  "_MAX_BUCKET": {
    "int": 0
  },
  "_MIN_LEVEL": {
    "int": 0
  },
  "_MAX_LEVEL": {
    "int": 0
  }
}
```

```bash
# Convert specific manifest to JSON
java -jar ~/.m2/repository/org/apache/avro/avro-tools/1.11.3/avro-tools-1.11.3.jar \
  tojson <(mc cat localminio/warehouse/paimon/ods.db/my_order1/manifest/manifest-766f9495-6622-4a85-9225-278d321a238b-7)
```

#### Manifest File Details:

```json
{
  "_VERSION": 2,
  "_KIND": 0,
  "_PARTITION": "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000",
  "_BUCKET": 0,
  "_TOTAL_BUCKETS": -1,
  "_FILE": {
    "_FILE_NAME": "data-3d4cabfa-8c75-4cbd-a004-71247a318223-6.parquet",
    "_FILE_SIZE": 1090,
    "_ROW_COUNT": 10,
    "_MIN_KEY": "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000",
    "_MAX_KEY": "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000",
    "_KEY_STATS": {
      "_MIN_VALUES": "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000",
      "_MAX_VALUES": "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000",
      "_NULL_COUNTS": {
        "array": []
      }
    },
    "_VALUE_STATS": {
      "_MIN_VALUES": "\u0000\u0000\u0000\u0003\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0010\u0000\u0000\u0000 \u0000\u0000\u0000=\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0002\u0000\u0000\u00000\u0000\u0000\u00002025-08-30 00:59\u0000·\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000",
      "_MAX_VALUES": "\u0000\u0000\u0000\u0003\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0010\u0000\u0000\u0000 \u0000\u0000\u0000F\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0002\u0000\u0000\u00000\u0000\u0000\u00002025-08-30 00:5:$M\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000",
      "_NULL_COUNTS": {
        "array": [
          {
            "long": 0
          },
          {
            "long": 0
          },
          {
            "long": 0
          }
        ]
      }
    },
    "_MIN_SEQUENCE_NUMBER": 60,
    "_MAX_SEQUENCE_NUMBER": 69,
    "_SCHEMA_ID": 0,
    "_LEVEL": 0,
    "_EXTRA_FILES": [],
    "_CREATION_TIME": {
      "long": 1756515590384
    },
    "_DELETE_ROW_COUNT": {
      "long": 0
    },
    "_EMBEDDED_FILE_INDEX": null,
    "_FILE_SOURCE": {
      "int": 0
    },
    "_VALUE_STATS_COLS": null,
    "_EXTERNAL_PATH": null,
    "_FIRST_ROW_ID": null,
    "_WRITE_COLS": null
  }
}
```

### Parquet Data Files

Inspect Parquet data files using parquet-tools:

```bash
# Set AWS credentials for S3 access
export AWS_ACCESS_KEY_ID=minio
export AWS_SECRET_ACCESS_KEY=minio12345
export AWS_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:9000

# Show Parquet file content
parquet-tools show s3://warehouse/paimon/ods.db/my_order1/bucket-0/data-1315b321-1e18-465e-8d9b-cd9cb3f685d5-0.parquet
```
