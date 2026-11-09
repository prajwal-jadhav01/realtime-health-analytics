package dev.prajwal.health.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * One event from a hospital system: an admission, a vitals reading or a discharge.
 * Plain public fields keep it a valid Flink POJO, so Flink serialises it efficiently.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PatientEvent {

    public String eventId;
    public String patientId;
    public String hospitalId;
    public String department;
    public EventType eventType;
    /** Beats per minute. Null for admission/discharge events. */
    public Integer heartRate;
    /** Blood oxygen saturation, percent. */
    public Integer spo2;
    public Integer systolicBp;
    /** Body temperature in Celsius. */
    public Double temperature;
    /** Epoch millis when the event happened at the hospital (event time). */
    public long eventTime;

    /** Set by the pipeline, not by the source system. */
    public boolean abnormal;

    public PatientEvent() {
    }

    public enum EventType {
        ADMISSION, VITALS, DISCHARGE
    }

    @Override
    public String toString() {
        return "PatientEvent{" + eventId + ", " + department + ", " + eventType + ", t=" + eventTime + "}";
    }
}
