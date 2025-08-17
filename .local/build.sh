cd ../
export PATH="/Users/juntzhang/Applications/apache-maven-3.8.6/bin:/opt/homebrew/opt/scala@2.12/bin:$PATH"
mvn clean install -DskipTests -Djdk17 -Pjava17-target -Pscala-2.12
