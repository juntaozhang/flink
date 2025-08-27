# flink paimon on k8s
## get flink and paimon
- get flink
  https://archive.apache.org/dist/flink/flink-1.17.2/
- get paimon & flink-shaded-hadoop-2-uber
  https://repository.apache.org/snapshots/org/apache/paimon/paimon-flink-1.17/

https://paimon.apache.org/docs/master/flink/quick-start/
cp paimon-flink-*.jar <FLINK_HOME>/lib/
cp flink-shaded-hadoop-2-uber-*.jar <FLINK_HOME>/lib/

https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/deployment/filesystems/s3/

## build env
### build image
>docker build -t flink:1.17.2-scala_2.12-paimon -f /Users/juntzhang/src/github/juntaozhang/flink/flink-1.17/.local/paimon/Dockerfile ./

### build flink session cluster in k8s
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



### build sql client
```shell
k apply -f flink1-paimon-sql-client.yaml
``` 
or :
```shell
k run flink-client \
--image=flink:1.17.2-scala_2.12-paimon --restart=Never \
--command -- bash -lc 'sleep infinity'
```

login the pod
>k exec -it deployment/flink-sql-client -- bash

>k exec -it pod/flink-client -- bash
```shell
bin/sql-client.sh embedded \
-Dexecution.target=remote \
-Dkubernetes.cluster-id=flink1
```

## test sql
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

### batch query
```sql
SET 'execution.runtime-mode' = 'batch';
select count(1),sum(total_amount) from my_order;

```
