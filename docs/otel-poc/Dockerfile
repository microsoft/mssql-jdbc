# syntax=docker/dockerfile:1
# Build context: repository root. No host Maven settings, caches or credentials.
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /work
COPY docs/otel-poc/settings.xml /root/.m2/settings.xml
COPY pom.xml maven-version-rules.xml mssql-jdbc-checkstyle.xml mssql-jdbc_formatter.xml LICENSE ./
COPY META-INF ./META-INF
COPY src/main/java ./src/main/java
# The root is a standalone jar, not a reactor containing the optional adapter.
RUN mvn -B -Pjre17 -DskipTests -Dmaven.test.skip=true -Dmaven.javadoc.skip=true install
COPY mssql-jdbc-otel/pom.xml ./mssql-jdbc-otel/pom.xml
COPY mssql-jdbc-otel/src ./mssql-jdbc-otel/src
RUN mvn -B -f mssql-jdbc-otel/pom.xml -DskipTests test-compile \
      org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
      -DincludeScope=test -DoutputDirectory=target/dependency \
 && test -f mssql-jdbc-otel/target/test-classes/com/microsoft/sqlserver/jdbc/otel/ConnectionErrorDemo.class

FROM eclipse-temurin:17-jre-jammy AS runtime
WORKDIR /app
COPY --from=build /work/mssql-jdbc-otel/target/classes ./target/classes
COPY --from=build /work/mssql-jdbc-otel/target/test-classes ./target/test-classes
COPY --from=build /work/mssql-jdbc-otel/target/dependency ./target/dependency
COPY --from=build /work/LICENSE ./LICENSE
USER 10001:10001
ENTRYPOINT ["java", "-cp", "target/test-classes:target/classes:target/dependency/*", "com.microsoft.sqlserver.jdbc.otel.ConnectionErrorDemo"]

# Explicit opt-in only; keep the standalone driver/module artifacts from runtime.
FROM runtime AS azure-cli
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl gnupg lsb-release util-linux \
 && install -m 0755 -d /etc/apt/keyrings \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg \
 && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/repos/azure-cli/ $(lsb_release -cs) main" > /etc/apt/sources.list.d/azure-cli.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends azure-cli \
 && rm -rf /var/lib/apt/lists/*
USER 10001:10001

# Default builds remain CLI-free even though the optional target appears above.
FROM runtime AS default