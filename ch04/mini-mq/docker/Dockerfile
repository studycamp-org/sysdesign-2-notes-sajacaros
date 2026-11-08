# 로컬에서 ./gradlew bootJar 로 만든 jar 를 담는다 (run.sh 가 함께 해 준다)
FROM eclipse-temurin:21-jre
ARG MODULE
COPY ${MODULE}/build/libs/${MODULE}.jar /app/app.jar
ENV JAVA_TOOL_OPTIONS="-Xmx256m -XX:+UseSerialGC"
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
