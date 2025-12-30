# ===============================
# Stage 1: Build the Application
# ===============================
FROM gradle:jdk21-alpine AS builder

WORKDIR /app

# 1. Copy dependency definitions first (Layer Caching)
#    This speeds up future builds if dependencies haven't changed.
COPY build.gradle.kts settings.gradle.kts gradlew ./
COPY gradle ./gradle

# 2. Copy the source code
COPY src ./src

# 3. Build the JAR (Skip tests to speed up deployment)
RUN ./gradlew bootJar --no-daemon -x test

# ===============================
# Stage 2: Run the Application
# ===============================
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

# 1. Copy the built JAR from the 'builder' stage
#    We use a wildcard (*.jar) so we don't need to know the exact version number
COPY --from=builder /app/build/libs/*.jar app.jar

# 2. Expose the standard Spring Boot port
EXPOSE 8080

# 3. Increase Netty Memory limits (Critical for your specific Streaming App)
#    -XX:MaxDirectMemorySize is vital for Netty's off-heap buffers
ENV JAVA_OPTS="-XX:MaxDirectMemorySize=512m -Xms512m -Xmx512m"

# 4. Start the app
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]