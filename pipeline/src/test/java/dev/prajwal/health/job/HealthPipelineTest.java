package dev.prajwal.health.job;

import dev.prajwal.health.model.DepartmentStats;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs the real pipeline on a local Flink mini-cluster with in-memory input. */
class HealthPipelineTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(2)
                    .build());

    private static final long T0 = 1_700_000_040_000L; // aligned to a minute boundary

    private static String event(String id, String dept, String type, Integer hr, Integer spo2, long t) {
        String vitals = hr == null ? "" : ",\"heartRate\":" + hr + ",\"spo2\":" + spo2;
        return "{\"eventId\":\"" + id + "\",\"patientId\":\"P" + id + "\",\"department\":\"" + dept
                + "\",\"eventType\":\"" + type + "\"" + vitals + ",\"eventTime\":" + t + "}";
    }

    @Test
    void aggregatesPerDepartmentPerMinute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> input = env.fromData(
                event("1", "icu", "ADMISSION", null, null, T0 + 1_000),
                event("2", "icu", "VITALS", 80, 97, T0 + 2_000),
                event("3", "icu", "VITALS", 140, 90, T0 + 3_000),       // abnormal
                event("4", "cardiology", "VITALS", 70, 99, T0 + 4_000),
                event("5", "icu", "DISCHARGE", null, null, T0 + 61_000), // next minute
                "not json at all");

        List<DepartmentStats> stats = collect(HealthPipeline.build(input).departmentStats());
        stats.sort(Comparator.comparing((DepartmentStats s) -> s.windowStart).thenComparing(s -> s.department));

        assertThat(stats).hasSize(3);

        DepartmentStats cardio = stats.get(0);
        assertThat(cardio.department).isEqualTo("cardiology");
        assertThat(cardio.events).isEqualTo(1);

        DepartmentStats icu = stats.get(1);
        assertThat(icu.department).isEqualTo("icu");
        assertThat(icu.windowStart).isEqualTo(T0);
        assertThat(icu.events).isEqualTo(3);
        assertThat(icu.admissions).isEqualTo(1);
        assertThat(icu.abnormalReadings).isEqualTo(1);
        assertThat(icu.avgHeartRate).isEqualTo(110.0);
        assertThat(icu.minSpo2).isEqualTo(90);

        DepartmentStats icuNextMinute = stats.get(2);
        assertThat(icuNextMinute.windowStart).isEqualTo(T0 + 60_000);
        assertThat(icuNextMinute.discharges).isEqualTo(1);
        assertThat(icuNextMinute.avgHeartRate).isNull();
    }

    @Test
    void sendsBadRecordsToDeadLetters() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> input = env.fromData(
                event("1", "icu", "VITALS", 80, 97, T0),
                "{\"broken\":",
                event("2", "icu", "VITALS", 900, 97, T0));

        List<String> dead = collect(HealthPipeline.build(input).deadLetters());

        assertThat(dead).hasSize(2);
        assertThat(dead).anySatisfy(d -> assertThat(d).contains("not valid JSON"));
        assertThat(dead).anySatisfy(d -> assertThat(d).contains("heartRate out of physical range"));
    }

    private static <T> List<T> collect(DataStream<T> stream) throws Exception {
        List<T> out = new ArrayList<>();
        try (CloseableIterator<T> it = stream.executeAndCollect()) {
            it.forEachRemaining(out::add);
        }
        return out;
    }
}
