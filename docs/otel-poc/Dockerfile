# Builder/runner image for the mssql-jdbc OpenTelemetry POC.
#
# The host only needs Docker. This image supplies the entire build + run
# toolchain (JDK 11 + Maven), a warm /root/.m2 cache (driver + test + OpenTelemetry
# + exec-plugin artifacts baked at build time), and curl/jq for the in-network
# health, metric, and trace probes that dev.sh runs.
#
# The live repository is bind-mounted over /work at run time, so edits on the host
# are what actually get compiled — the baked target/ is discarded and rebuilt
# incrementally against the warm cache.
#
# Build context is the repository root (see docker-compose.yml), so the COPY paths
# below are relative to the repo root.
FROM maven:3.9-eclipse-temurin-11

# Pinned so the load-generator runner resolves offline from the baked cache
# instead of doing a "latest" metadata lookup at run time.
ARG EXEC_PLUGIN_VERSION=3.1.0
ENV EXEC_PLUGIN_VERSION=${EXEC_PLUGIN_VERSION}

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl jq \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /work

# Build against Maven Central only — the project's default feed denies anonymous
# access, but every dependency + plugin the POC needs is on Central.
COPY docs/otel-poc/settings.xml /root/.m2/settings.xml

# Warm the local Maven repository in a layer that only busts when the build
# definition or sources change. `test-compile` pulls every compile + test + OTel
# dependency; the exec-plugin `help` goal pre-fetches the load-generator runner.
COPY pom.xml maven-version-rules.xml mssql-jdbc-checkstyle.xml mssql-jdbc_formatter.xml ./
COPY src ./src
RUN mvn -B -Pjre11 -DskipTests test-compile \
 && mvn -B -q -N "org.codehaus.mojo:exec-maven-plugin:${EXEC_PLUGIN_VERSION}:help" \
 && rm -rf /work/target

CMD ["mvn", "-version"]
