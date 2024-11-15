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
