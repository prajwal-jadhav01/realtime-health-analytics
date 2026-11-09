package dev.prajwal.health.job;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EventParserTest {

    static String vitals(int heartRate, int spo2) {
        return """
                {"eventId":"e1","patientId":"P1","hospitalId":"H001","department":"icu",
                 "eventType":"VITALS","heartRate":%d,"spo2":%d,"systolicBp":120,"temperature":36.8,
                 "eventTime":1700000000000}
                """.formatted(heartRate, spo2);
    }

    @Test
    void parsesNormalVitals() {
        EventParser.Result r = EventParser.parse(vitals(72, 98));
        assertThat(r.isValid()).isTrue();
        assertThat(r.event().department).isEqualTo("icu");
        assertThat(r.event().abnormal).isFalse();
    }

    @Test
    void flagsAbnormalVitals() {
        assertThat(EventParser.parse(vitals(140, 98)).event().abnormal).isTrue();
        assertThat(EventParser.parse(vitals(72, 88)).event().abnormal).isTrue();
    }

    @Test
    void rejectsTruncatedJson() {
        EventParser.Result r = EventParser.parse("{\"patientId\":\"P1\"");
        assertThat(r.isValid()).isFalse();
        assertThat(r.error()).startsWith("not valid JSON");
    }

    @Test
    void rejectsUnknownEventType() {
        EventParser.Result r = EventParser.parse(
                "{\"eventId\":\"e\",\"patientId\":\"P\",\"department\":\"icu\",\"eventType\":\"TELEPORT\",\"eventTime\":1}");
        assertThat(r.isValid()).isFalse();
        assertThat(r.error()).isEqualTo("invalid value for field eventType");
    }

    @Test
    void rejectsImpossibleHeartRate() {
        EventParser.Result r = EventParser.parse(vitals(900, 98));
        assertThat(r.error()).isEqualTo("heartRate out of physical range");
    }

    @Test
    void rejectsMissingFields() {
        assertThat(EventParser.parse("{}").error()).isEqualTo("missing eventId");
        assertThat(EventParser.parse("").error()).isEqualTo("empty message");
    }
}
