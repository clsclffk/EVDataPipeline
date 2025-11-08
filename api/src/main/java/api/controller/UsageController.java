package api.controller;

import api.dto.StatsDTOs.ChargerTypeStatsDTO;
import api.dto.StatsDTOs.GuStatsDTO;
import api.dto.StatsDTOs.GuTypeStatsDTO;
import api.dto.StatsDTOs.HourStatsDTO;
import api.dto.StatsDTOs.HourTypeStatsDTO;
import api.dto.StatsDTOs.StationMapDTO;
import api.repository.UsageRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

// swagger annotation
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

// 통합 DTO import
import static api.dto.StatsDTOs.*;

@Tag(name = "Usage Statistics API", description = "전기차 충전소 통계 데이터 조회 API")
@RestController
public class UsageController {

    private final UsageRepository usageRepository;

    public UsageController(UsageRepository usageRepository) {
        this.usageRepository = usageRepository;
    }

    /** 1. 구별 통계 */
    @Operation(
        summary = "구별 충전소 통계 조회",
        description = "각 구별 충전소 수, 총 충전량(kWh), 1개소당 평균 충전량을 반환합니다."
    )
    @GetMapping("/api/stats/by-gu")
    public List<GuStatsDTO> getStatsByGu() {
        return usageRepository.getStatsByGu().stream()
                .map(r -> new GuStatsDTO(
                        (String) r[0],               // gu
                        ((Number) r[1]).intValue(),  // station_count
                        ((Number) r[2]).doubleValue(), // total_kwh
                        ((Number) r[3]).doubleValue()  // avg_kwh_per_station
                ))
                .collect(Collectors.toList());
    }

    /** 2. 시간대별 총 충전량 */
    @Operation(
        summary = "시간대별 총 충전량 조회",
        description = "0~23시별 충전량(kWh)을 집계해 반환합니다."
    )
    @GetMapping("/api/stats/by-hour")
    public List<HourStatsDTO> getStatsByHour() {
        return usageRepository.getStatsByHour().stream()
                .map(r -> new HourStatsDTO(
                        ((Number) r[0]).intValue(),   // hour
                        ((Number) r[1]).doubleValue() // total_kwh
                ))
                .collect(Collectors.toList());
    }

    /** 2-2. 시간대별/충전유형별 세션수 */
    @Operation(
        summary = "시간대별 충전유형별 이용 추이 조회",
        description = "시간대별로 급속/완속 충전 횟수를 반환합니다."
    )
    @GetMapping("/api/stats/by-hour/type")
    public List<HourTypeStatsDTO> getHourlyUsageByType() {
        return usageRepository.getHourlyUsageByType().stream()
                .map(r -> new HourTypeStatsDTO(
                        ((Number) r[0]).intValue(),   // hour
                        (String) r[1],                // charger_type
                        ((Number) r[2]).intValue()    // session_count
                ))
                .collect(Collectors.toList());
    }

    /** 3. 충전유형별 통계 */
    @Operation(
        summary = "충전유형별 이용 통계 조회",
        description = "급속/완속별 총 충전량(kWh)과 세션 수를 반환합니다."
    )
    @GetMapping("/api/stats/by-charger-type")
    public List<ChargerTypeStatsDTO> getStatsByChargerType() {
        return usageRepository.getStatsByChargerType().stream()
                .map(r -> new ChargerTypeStatsDTO(
                        (String) r[0],                // charger_type
                        ((Number) r[1]).doubleValue(), // total_energy
                        ((Number) r[2]).intValue()     // total_sessions
                ))
                .collect(Collectors.toList());
    }

    /** 3-2. 구별/충전유형별 통계 */
    @Operation(
        summary = "구별/충전유형별 총 충전량 조회",
        description = "각 구와 충전유형(급속/완속)별 총 충전량(kWh)을 반환합니다."
    )
    @GetMapping("/api/stats/by-charger-type/gu")
    public List<GuTypeStatsDTO> getStatsByGuAndType() {
        return usageRepository.getStatsByGuAndType().stream()
                .map(r -> new GuTypeStatsDTO(
                        (String) r[0],                // gu
                        (String) r[1],                // charger_type
                        ((Number) r[2]).doubleValue() // total_kwh
                ))
                .collect(Collectors.toList());
    }

    /** 4. 지도용 데이터 */
    @Operation(
        summary = "충전소 지도 데이터 조회",
        description = "지도 시각화를 위한 충전소 이름, 구, 위도/경도, 용량, 충전기 수 정보를 반환합니다."
    )
    @GetMapping("/api/map/stations")
    public List<StationMapDTO> getStationMapData() {
        return usageRepository.getStationMapData().stream()
                .map(r -> new StationMapDTO(
                        (String) r[0],                // station_name
                        (String) r[1],                // gu
                        ((Number) r[2]).doubleValue(), // latitude
                        ((Number) r[3]).doubleValue(), // longitude
                        ((Number) r[4]).doubleValue(), // capacity_kw
                        ((Number) r[5]).intValue()     // total_chargers
                ))
                .collect(Collectors.toList());
    }
}
