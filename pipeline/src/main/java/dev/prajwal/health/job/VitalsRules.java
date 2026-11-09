package dev.prajwal.health.job;

import dev.prajwal.health.model.PatientEvent;

/**
 * Simple, explainable thresholds for flagging a vitals reading for attention.
 * These are illustrative only, not clinical guidance.
 */
public final class VitalsRules {

    static final int HEART_RATE_LOW = 50;
    static final int HEART_RATE_HIGH = 120;
    static final int SPO2_LOW = 92;
    static final int SYSTOLIC_HIGH = 180;
    static final double FEVER_C = 39.0;

    private VitalsRules() {
    }

    public static boolean isAbnormal(PatientEvent e) {
        if (e.eventType != PatientEvent.EventType.VITALS) {
            return false;
        }
        return (e.heartRate != null && (e.heartRate < HEART_RATE_LOW || e.heartRate > HEART_RATE_HIGH))
                || (e.spo2 != null && e.spo2 < SPO2_LOW)
                || (e.systolicBp != null && e.systolicBp > SYSTOLIC_HIGH)
                || (e.temperature != null && e.temperature >= FEVER_C);
    }
}
