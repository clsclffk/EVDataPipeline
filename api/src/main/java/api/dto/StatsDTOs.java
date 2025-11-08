package api.dto;

public class StatsDTOs {

    // 1. 구별 통계 DTO
    public record GuStatsDTO(String gu, int stationCount, double totalKwh, double avgKwhPerStation) {}

    // 2. 시간대별 총 충전량 DTO
    public record HourStatsDTO(int hour, double totalKwh) {}

    // 2-2. 시간대별 × 충전유형별 세션수 DTO
    public record HourTypeStatsDTO(int hour, String chargerType, int sessionCount) {}

    // 3. 충전유형별 통계 DTO
    public record ChargerTypeStatsDTO(String chargerType, double totalEnergy, int totalSessions) {}

    // 3-2. 구별/충전유형별 총 충전량 DTO
    public record GuTypeStatsDTO(String gu, String chargerType, double totalKwh) {}

    // 4. 지도용 충전소 DTO
    public record StationMapDTO(String stationName, String gu, double latitude, double longitude,
                                double capacityKw, int totalChargers) {}
}
