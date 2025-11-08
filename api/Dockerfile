FROM eclipse-temurin:17-jdk

# 컨테이너 내부 작업 디렉토리 설정
WORKDIR /app

# 빌드된 Spring Boot 실행 파일(app.jar)을 도커 안으로 이동
COPY target/ev-api-0.0.1-SNAPSHOT.jar app.jar

# 포트 노출 
EXPOSE 8080

# 컨테이너 실행 시 자동 실행 
ENTRYPOINT ["java", "-jar", "app.jar"]