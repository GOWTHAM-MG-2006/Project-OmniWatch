# OmniWatch — Entity Resolution Layer
# Component: Flink job Docker image
# Phase: 3
# Purpose: Builds the omniwatch-entity-resolution-job.jar and packages it
#          into a Flink 1.17 runtime image for the docker-compose stack.
# Inputs: Gradle project sources
# Outputs: Flink image with the entity resolution job jar on the classpath

# ---- Stage 1: build the shadow jar ----
FROM gradle:8.5-jdk11 AS builder
WORKDIR /app

# Copy build definitions first for layer caching
COPY build.gradle settings.gradle ./
COPY gradle ./gradle
COPY gradlew gradlew.bat ./

# Copy sources
COPY src ./src

RUN gradle clean shadowJar --no-daemon

# ---- Stage 2: Flink runtime ----
FROM flink:1.17-scala_2.12

# Copy the built job jar into the jobs directory (NOT /opt/flink/lib/).
# Putting the fat jar in lib/ pollutes the global 'app' classpath, so
# `flink run` on the same jar triggers a ChildFirstClassLoader LinkageError
# (duplicate kafka-clients / flink-connector-kafka classes).
# Use unique jar name to force new blob in Flink jobmanager blob storage.
COPY --from=builder /app/build/libs/omniwatch-entity-resolution-job.jar /opt/flink/jobs/omniwatch-entity-resolution-job-v2.jar

ENTRYPOINT ["/docker-entrypoint.sh"]
