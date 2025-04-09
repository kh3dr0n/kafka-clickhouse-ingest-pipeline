# Stage 1: Build the application using Gradle
FROM gradle:8.5-jdk17-alpine AS builder

WORKDIR /app

# Copy only necessary files first to leverage Docker cache
COPY build.gradle.kts settings.gradle.kts ./
COPY gradle ./gradle
COPY gradlew .

# Download dependencies (cacheable layer)
RUN gradle --no-daemon dependencies || true # Allow failure? Maybe just build? Let's try build directly.
RUN gradle --no-daemon :shadowJar --info # Build the shadow JAR

# Copy the rest of the source code
COPY src ./src

# Build the shadow JAR (this will use cached dependencies if possible)
# Ensure shadowJar task is run
RUN gradle --no-daemon clean :shadowJar --info


# Stage 2: Create the final lightweight image
FROM amazoncorretto:17-alpine-jdk

WORKDIR /app

# Copy the built JAR from the builder stage
COPY --from=builder /app/build/libs/kotlin-kafka-clickhouse-*.jar ./app.jar

# Expose port if your app had an HTTP interface (not needed for this consumer)
# EXPOSE 8080

# Set the entrypoint command to run the application
# Use exec form for proper signal handling
ENTRYPOINT ["java", "-jar", "./app.jar"]

# Optional: Add a non-root user for security
# RUN addgroup -S appgroup && adduser -S appuser -G appgroup
# USER appuser