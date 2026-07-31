FROM gradle:8.5-jdk11 AS builder
WORKDIR /app
COPY build.gradle settings.gradle ./
COPY src ./src
RUN gradle clean shadowJar --no-daemon

FROM flink:1.17-scala_2.12
COPY --from=builder /app/build/libs/*.jar /opt/flink/lib/
ENTRYPOINT ["/docker-entrypoint.sh"]
