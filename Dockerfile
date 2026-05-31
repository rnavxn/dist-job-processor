# Stage 1: Build the application securely
FROM maven:3.9.6-eclipse-temurin-21 AS build
WORKDIR /build

# Copy the pom.xml and source code
COPY pom.xml .
COPY src ./src

# Compile the jar inside the container
RUN mvn clean package -DskipTests

# Stage 2: Minimal runtime environment
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

# Extract the compiled jar from Stage 1
COPY --from=build /build/target/*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]