# Stage 1: Build the application using Gradle
FROM gradle:8.5.0-jdk17 AS builder

WORKDIR /app

# Copy only necessary files for dependency download first
COPY build.gradle.kts settings.gradle.kts gradlew ./
COPY gradle ./gradle

# Download dependencies (this layer is cached if build files don't change)
# RUN gradle build --no-daemon || return 0  # Allow build to continue to copy source
RUN gradle dependencies --no-daemon

# Copy the source code
COPY src ./src

# Build the application JAR
RUN gradle bootJar --no-daemon


# Stage 2: Create the final lightweight image
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

# Copy the executable JAR from the builder stage
COPY --from=builder /app/build/libs/*.jar /app/app.jar

# Expose the application port
EXPOSE 8090

# Environment variables (can be overridden at runtime)
ENV KAFKA_BROKERS="kafka:29092"
ENV CLICKHOUSE_HOST="clickhouse"
ENV CLICKHOUSE_PORT="8123"
ENV CLICKHOUSE_DATABASE="default"
ENV CLICKHOUSE_USER="default"
ENV CLICKHOUSE_PASSWORD="" # Provide via docker run -e or docker-compose
ENV KAFKA_INGEST_TOPIC="ingest-topic"
ENV CLICKHOUSE_INGEST_TABLE="ingest_data"
ENV CLICKHOUSE_BATCH_SIZE="100"
ENV KAFKA_CONSUMER_CONCURRENCY="1"
# Add any other necessary environment variables

# Run the application
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

# Optional: Add health check
# HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
#   CMD wget --quiet --tries=1 --spider http://localhost:8090/actuator/health || exit 1