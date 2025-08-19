cd ../
export JAVA_HOME=/Library/Java/JavaVirtualMachines/openJDK-1.8.362
export PATH="/Users/juntzhang/Applications/apache-maven-3.8.6/bin:/opt/homebrew/opt/scala@2.12/bin:$PATH"
#mvn spotless:apply
#mvn clean package -Pscala-2.12 -Pfast -DskipTests -rf :flink-statebackend-forst  #-Puse-alibaba-mirror -pl '!flink-examples-table'
mvn clean install -Pscala-2.12 -Pfast -DskipTests -rf :flink-scala_2.12

