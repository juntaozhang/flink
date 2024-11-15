#FROM flink:1.17.2-scala_2.12
FROM flink:1.17.0-scala_2.12-java8
RUN rm -rf /opt/flink
COPY ./flink-dist/target/flink-1.17-SNAPSHOT-bin/flink-1.17-SNAPSHOT /opt/flink
COPY ./flink-examples/flink-examples-streaming/target/flink-examples-streaming_2.12-1.17-SNAPSHOT-MyTest.jar /opt/flink/examples/streaming/MyTest.jar
COPY .local/kubernetes-application/docker/jars/commons-lang3-3.12.0.jar .local/kubernetes-application/docker/jars/guava-33.2.1-jre.jar .local/kubernetes-application/docker/jars/lombok-1.18.32.jar /opt/flink/lib/
