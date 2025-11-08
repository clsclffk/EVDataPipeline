package api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// Swagger 문서화 
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.servers.Server; // 서버 URL 고정용

@OpenAPIDefinition(
    info = @Info(
        title = "EV Data Pipeline API",
        version = "1.0",
        description = """
            서울시 전기차 충전소 공공데이터를 활용한 데이터 파이프라인 미니 프로젝트입니다.

            - MySQL → Sqoop → HDFS → Spark ETL → Postgres DW 적재
            - Spring Boot를 통한 REST API 서비스
            - Tableau를 이용한 시각화 대시보드 구축

            주요 제공 기능:
            ① 구별 충전소 통계
            ② 시간대별 충전 이용 패턴
            ③ 충전유형별 이용 현황
            ④ 충전소 위치 지도 데이터
        """
    ),
    servers = {
        @Server(url = "/", description = "EV Data API Server") // ✅ 서버 주소 고정
    }
)
@SpringBootApplication
public class ApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiApplication.class, args);
    }
}
