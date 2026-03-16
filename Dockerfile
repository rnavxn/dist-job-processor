# Stage 1: Build the application
FROM maven:3.9.6-eclipse-temurin-21 AS build
COPY . .
RUN mvn clean package -DskipTests

# Stage 2: Run the application
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /target/*.jar app.jar

# Expose the dashboard port
EXPOSE 8080

# Run the app
ENTRYPOINT ["java", "-jar", "app.jar"]