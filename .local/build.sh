cd ../
export JAVA_HOME=/Library/Java/JavaVirtualMachines/openJDK-17.0.6.2
export PATH="/Users/juntzhang/Applications/apache-maven-3.8.6/bin:/opt/homebrew/opt/scala@2.12/bin:$PATH"
#mvn spotless:apply
#mvn clean package -Djdk17 -Pjava17-target -Pscala-2.12 -Pfast -DskipTests -rf :flink-statebackend-forst  #-Puse-alibaba-mirror -pl '!flink-examples-table'
mvn clean package -Djdk17 -Pjava17-target -Pscala-2.12 -Pfast -DskipTests
