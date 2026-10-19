FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src src
RUN mvn -q -B package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/lockstep.jar lockstep.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/lockstep.jar"]
CMD ["all"]
