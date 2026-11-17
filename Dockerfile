# Stage 1: Build fat JAR
FROM maven:3.9.6-eclipse-temurin-21-alpine AS builder
WORKDIR /build

COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src ./src
RUN mvn clean package -DskipTests

# Stage 2: Lightweight runtime
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

COPY --from=builder /build/target/message-relay-1.0.0.jar app.jar

ENV PORT=8080
ENV DATA_DIR=/data
VOLUME ["/data"]

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
