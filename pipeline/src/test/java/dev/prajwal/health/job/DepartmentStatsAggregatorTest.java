package dev.prajwal.health.job;

import dev.prajwal.health.model.PatientEvent;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DepartmentStatsAggregatorTest {

    private final DepartmentStatsAggregator agg = new DepartmentStatsAggregator();

    private static PatientEvent vitals(int hr, int spo2) {
        PatientEvent e = new PatientEvent();
        e.eventType = PatientEvent.EventType.VITALS;
        e.heartRate = hr;
        e.spo2 = spo2;
        e.abnormal = VitalsRules.isAbnormal(e);
        return e;
    }

    @Test
    void mergeCombinesPartialResults() {
        DepartmentStatsAggregator.Acc a = agg.add(vitals(80, 97), agg.createAccumulator());
        DepartmentStatsAggregator.Acc b = agg.add(vitals(130, 91), agg.createAccumulator());

        DepartmentStatsAggregator.Acc m = agg.merge(a, b);

        assertThat(m.events).isEqualTo(2);
        assertThat(m.vitalsCount).isEqualTo(2);
        assertThat(m.heartRateSum).isEqualTo(210);
        assertThat(m.minSpo2).isEqualTo(91);
        assertThat(m.abnormal).isEqualTo(1);
    }

    @Test
    void mergeHandlesEmptySide() {
        DepartmentStatsAggregator.Acc a = agg.add(vitals(80, 97), agg.createAccumulator());
        assertThat(agg.merge(a, agg.createAccumulator()).minSpo2).isEqualTo(97);
        assertThat(agg.merge(agg.createAccumulator(), a).minSpo2).isEqualTo(97);
    }
}
