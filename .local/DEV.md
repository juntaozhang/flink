## Create Kubernetes Role
```bash
kubectl apply -f flink-rbac.yaml
```


## Run flink example
```bash
./bin/flink run-application \
    --target kubernetes-application \
    -Dkubernetes.cluster-id=wordcount \
    -Dkubernetes.container.image=flink:1.17.2-scala_2.12 \
    -Dkubernetes.service-account=flink-service-account \
    local:///opt/flink/examples/streaming/WordCount.jar
```


## Run custom socket example

### Build Flink Docker Image

build in the root of the project
```bash
docker build -t flink:1.17-SNAPSHOT-my -f .local/kubernetes-application/docker/Dockerfile ./
```

### Start Socket Server
```bash
kubectl apply -f socker-server.yaml
```

### Start Flink Job
```bash
cd ./flink/flink-dist/target/flink-1.17-SNAPSHOT-bin/flink-1.17-SNAPSHOT
./bin/flink run-application                                              \
    --target kubernetes-application                                      \
    -Dkubernetes.cluster-id=window-watermark-example                     \
    -Dkubernetes.container.image=flink:1.17-SNAPSHOT-my                  \
    -Dkubernetes.service-account=flink-service-account                   \
    -Dkubernetes.rest-service.exposed.type=LoadBalancer                  \
    -Dparallelism.default=4                                              \
    -Drestart-strategy=fixed-delay                                       \
    -Drestart-strategy.fixed-delay.attempts=10                           \
    -Drestart-strategy.fixed-delay.delay=10s                             \
    local:///opt/flink/examples/streaming/MyTest.jar socket-server.default.svc.cluster.local 19998
```

## Run Kafka example
### Install Kafka
```bash
cd .local/kubernetes-application/helm/kafka
helm install kafka bitnami/kafka -f values.yaml
```
### Change to kubernetes LoadBalance
```bash
kubectl patch svc kafka --type='json' -p '[{"op":"replace","path":"/spec/type","value":"LoadBalancer"}]'
```

### Or create by docker
https://hub.docker.com/r/apache/kafka

#### Single node
```bash
docker run -d -p 9092:9092 --name broker apache/kafka:3.9.0
docker exec -u root -it broker  /bin/bash

/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test-input
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test-output
/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-input
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-output --from-beginning
```

#### Multiple nodes
```bash
docker compose -p kafka -f docker-compose-kafka.yaml up -d
docker exec -u root -it controller-1  /bin/bash

/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --list
/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --topic test-input1 --describe
/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --create --topic test-input2 --partitions 3
/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --create --topic test-output2 --partitions 3
/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --topic test-input1
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 --topic test-output1 --from-beginning
```


