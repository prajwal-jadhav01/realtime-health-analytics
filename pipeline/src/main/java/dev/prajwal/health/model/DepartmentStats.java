package dev.prajwal.health.model;

/** One-minute summary of activity in a single department. */
public class DepartmentStats {

    public String department;
    public long windowStart;
    public long windowEnd;
    public long events;
    public long admissions;
    public long discharges;
    public long abnormalReadings;
    /** Average heart rate over the vitals readings in the window, or null if there were none. */
    public Double avgHeartRate;
    public Integer minSpo2;

    public DepartmentStats() {
    }

    @Override
    public String toString() {
        return "DepartmentStats{" + department + " [" + windowStart + "," + windowEnd + ") events=" + events
                + " admissions=" + admissions + " discharges=" + discharges
                + " abnormal=" + abnormalReadings + " avgHr=" + avgHeartRate + " minSpo2=" + minSpo2 + "}";
    }
}
