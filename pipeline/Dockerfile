# Stage 1: build the fat jar (tests run in CI, so they are skipped here to keep startup fast).
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q package -DskipTests

# Stage 2: Flink runtime with our job on the user classpath.
FROM flink:1.19.1-java17
COPY --from=build /src/target/health-pipeline.jar /opt/flink/usrlib/health-pipeline.jar
