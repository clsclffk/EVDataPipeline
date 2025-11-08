package api.repository;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public class UsageRepository {

    @PersistenceContext
    private EntityManager entityManager;

    /** 1. 구별 통계 */
    public List<Object[]> getStatsByGu() {
        return entityManager.createNativeQuery("""
            SELECT 
                s.gu,
                COUNT(DISTINCT s.station_id) AS station_count,
                COALESCE(SUM(f.energy_kwh)::float, 0) AS total_kwh,
                ROUND(
                    COALESCE(SUM(f.energy_kwh)::numeric, 0) / NULLIF(COUNT(DISTINCT s.station_id), 0),
                    2
                ) AS avg_kwh_per_station
            FROM fact_sessions f
            JOIN dim_stations s ON f.station_id = s.station_id
            WHERE s.gu IS NOT NULL
            GROUP BY s.gu
            ORDER BY total_kwh DESC
        """).getResultList();
    }

    /** 2. 시간대별 총 충전량 */
    public List<Object[]> getStatsByHour() {
        return entityManager.createNativeQuery("""
            SELECT 
                t.hour,
                COALESCE(SUM(f.energy_kwh), 0) AS total_kwh
            FROM fact_times f
            JOIN dim_time t ON f.hour_id = t.hour_id
            GROUP BY t.hour
            ORDER BY t.hour
        """).getResultList();
    }

    /** 2-2. 시간대별/충전유형별 세션수 */
    public List<Object[]> getHourlyUsageByType() {
        // fact_sessions에는 hour_id가 없기 때문에,
        // 실제로 시간대별 유형 통계를 만들려면 fact_times에 charger_type_id가 있어야 합니다.
        // 현재 모델에서는 hour 정보가 fact_times에, charger_type이 fact_sessions에 따로 있으므로,
        // 아래 쿼리는 시각화용으로 "임시 세션 수 분포"를 제공합니다.
        return entityManager.createNativeQuery("""
            SELECT 
                t.hour,
                c.charger_type,
                COALESCE(SUM(f.session_count), 0) AS total_sessions
            FROM fact_sessions f
            JOIN dim_charger_type c ON f.charger_type_id = c.charger_type_id
            JOIN dim_date d ON f.date_id = d.date_id
            JOIN dim_time t ON EXTRACT(HOUR FROM d.date::timestamp) = t.hour
            GROUP BY t.hour, c.charger_type
            ORDER BY t.hour, c.charger_type
        """).getResultList();
    }

    /** 3. 충전유형별 통계 */
    public List<Object[]> getStatsByChargerType() {
        return entityManager.createNativeQuery("""
            SELECT 
                c.charger_type,
                COALESCE(SUM(f.energy_kwh), 0) AS total_energy,
                COALESCE(SUM(f.session_count), 0) AS total_sessions
            FROM fact_sessions f
            JOIN dim_charger_type c ON f.charger_type_id = c.charger_type_id
            GROUP BY c.charger_type
            ORDER BY total_energy DESC
        """).getResultList();
    }

    /** 3-2. 구별/충전유형별 통계 */
    public List<Object[]> getStatsByGuAndType() {
        return entityManager.createNativeQuery("""
            SELECT 
                s.gu,
                c.charger_type,
                COALESCE(SUM(f.energy_kwh), 0) AS total_kwh
            FROM fact_sessions f
            JOIN dim_stations s ON f.station_id = s.station_id
            JOIN dim_charger_type c ON f.charger_type_id = c.charger_type_id
            WHERE s.gu IS NOT NULL
            GROUP BY s.gu, c.charger_type
            ORDER BY total_kwh DESC
        """).getResultList();
    }

    /** 4. 지도용 충전소 데이터 */
    public List<Object[]> getStationMapData() {
        return entityManager.createNativeQuery("""
            SELECT 
                station_name,
                gu,
                latitude,
                longitude,
                capacity_kw,
                total_chargers
            FROM dim_stations
            WHERE latitude IS NOT NULL 
              AND longitude IS NOT NULL
        """).getResultList();
    }
}
