package dev.prajwal.health.job;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import dev.prajwal.health.model.PatientEvent;

import java.util.Optional;

/**
 * Turns raw JSON into a validated {@link PatientEvent}.
 * Anything that can't be trusted is rejected with a reason, so it can go to the dead-letter topic
 * instead of crashing the job or silently polluting the analytics tables.
 */
public final class EventParser {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private EventParser() {
    }

    /** Result of parsing: either an event or a rejection reason, never both. */
    public record Result(PatientEvent event, String error) {
        static Result ok(PatientEvent e) {
            return new Result(e, null);
        }

        static Result reject(String reason) {
            return new Result(null, reason);
        }

        public boolean isValid() {
            return event != null;
        }
    }

    public static Result parse(String json) {
        if (json == null || json.isBlank()) {
            return Result.reject("empty message");
        }
        PatientEvent e;
        try {
            e = MAPPER.readValue(json, PatientEvent.class);
        } catch (InvalidFormatException ex) {
            // Well-formed JSON, but a field holds a value we don't accept (e.g. an unknown eventType).
            String field = ex.getPath().isEmpty() ? "?" : ex.getPath().get(0).getFieldName();
            return Result.reject("invalid value for field " + field);
        } catch (Exception ex) {
            return Result.reject("not valid JSON: " + ex.getClass().getSimpleName());
        }
        return validate(e).map(Result::reject).orElseGet(() -> {
            e.abnormal = VitalsRules.isAbnormal(e);
            return Result.ok(e);
        });
    }

    /** Returns the first problem found, or empty if the event is usable. */
    static Optional<String> validate(PatientEvent e) {
        if (isBlank(e.eventId)) return Optional.of("missing eventId");
        if (isBlank(e.patientId)) return Optional.of("missing patientId");
        if (isBlank(e.department)) return Optional.of("missing department");
        if (e.eventType == null) return Optional.of("missing or unknown eventType");
        if (e.eventTime <= 0) return Optional.of("missing eventTime");
        if (e.eventType == PatientEvent.EventType.VITALS) {
            if (e.heartRate == null || e.spo2 == null) return Optional.of("vitals event without heartRate/spo2");
            if (e.heartRate < 20 || e.heartRate > 250) return Optional.of("heartRate out of physical range");
            if (e.spo2 < 50 || e.spo2 > 100) return Optional.of("spo2 out of physical range");
        }
        return Optional.empty();
    }

    public static String toJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception ex) {
            throw new IllegalStateException("could not serialise " + value, ex);
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
