package dev.prajwal.health.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.prajwal.health.model.PatientEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * Produces synthetic hospital events to Kafka. All data is random; no real patients are involved.
 *
 * <p>Env vars: KAFKA_BROKERS (default localhost:29092), EVENTS_PER_SECOND (default 20),
 * BAD_RECORD_PERCENT (default 1), LATE_EVENT_PERCENT (default 2).
 */
public class EventGenerator {

    static final List<String> DEPARTMENTS = List.of("cardiology", "emergency", "icu", "pediatrics", "oncology");
    static final List<String> HOSPITALS = List.of("H001", "H002", "H003");

    private final Random random;
    private final ObjectMapper mapper = new ObjectMapper();
    private final int badRecordPercent;
    private final int lateEventPercent;

    EventGenerator(long seed, int badRecordPercent, int lateEventPercent) {
        this.random = new Random(seed);
        this.badRecordPercent = badRecordPercent;
        this.lateEventPercent = lateEventPercent;
    }

    public static void main(String[] args) throws Exception {
        String brokers = env("KAFKA_BROKERS", "localhost:29092");
        int eps = Integer.parseInt(env("EVENTS_PER_SECOND", "20"));
        String topic = env("TOPIC", "patient-events");
        EventGenerator gen = new EventGenerator(System.nanoTime(),
                Integer.parseInt(env("BAD_RECORD_PERCENT", "1")),
                Integer.parseInt(env("LATE_EVENT_PERCENT", "2")));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        System.out.printf("Sending ~%d events/s to %s on %s%n", eps, topic, brokers);
        long sent = 0;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                long tickStart = System.currentTimeMillis();
                for (int i = 0; i < eps; i++) {
                    Keyed msg = gen.next(System.currentTimeMillis());
                    // Key by patient so one patient's events stay ordered within a partition.
                    producer.send(new ProducerRecord<>(topic, msg.key(), msg.json()));
                    sent++;
                }
                if (sent % (eps * 30L) == 0) {
                    System.out.printf("sent %d events%n", sent);
                }
                long sleep = 1000 - (System.currentTimeMillis() - tickStart);
                if (sleep > 0) {
                    Thread.sleep(sleep);
                }
            }
        }
    }

    record Keyed(String key, String json) {
    }

    Keyed next(long now) throws Exception {
        String patientId = "P" + (10000 + random.nextInt(5000));
        if (random.nextInt(100) < badRecordPercent) {
            return new Keyed(patientId, badRecord(patientId));
        }
        PatientEvent e = new PatientEvent();
        e.eventId = UUID.randomUUID().toString();
        e.patientId = patientId;
        e.hospitalId = pick(HOSPITALS);
        e.department = pick(DEPARTMENTS);
        e.eventTime = random.nextInt(100) < lateEventPercent
                ? now - 5_000 - random.nextInt(20_000)   // arrives late, sometimes after its window closed
                : now - random.nextInt(2_000);

        int roll = random.nextInt(100);
        if (roll < 10) {
            e.eventType = PatientEvent.EventType.ADMISSION;
        } else if (roll < 18) {
            e.eventType = PatientEvent.EventType.DISCHARGE;
        } else {
            e.eventType = PatientEvent.EventType.VITALS;
            boolean unwell = random.nextInt(100) < ("icu".equals(e.department) ? 25 : 6);
            e.heartRate = unwell ? 125 + random.nextInt(40) : 60 + random.nextInt(40);
            e.spo2 = unwell ? 85 + random.nextInt(7) : 95 + random.nextInt(5);
            e.systolicBp = 100 + random.nextInt(unwell ? 95 : 40);
            e.temperature = Math.round((36.4 + random.nextDouble() * (unwell ? 3.2 : 1.0)) * 10) / 10.0;
        }
        return new Keyed(patientId, mapper.writeValueAsString(e));
    }

    private String badRecord(String patientId) {
        return switch (random.nextInt(3)) {
            case 0 -> "{\"patientId\":\"" + patientId + "\",\"eventType\":\"VITALS\""; // truncated JSON
            case 1 -> "{\"eventId\":\"" + UUID.randomUUID() + "\",\"patientId\":\"" + patientId
                    + "\",\"department\":\"icu\",\"eventType\":\"VITALS\",\"heartRate\":900,\"spo2\":97,"
                    + "\"eventTime\":" + System.currentTimeMillis() + "}"; // impossible heart rate
            default -> "{\"eventId\":\"" + UUID.randomUUID() + "\",\"patientId\":\"" + patientId
                    + "\",\"eventType\":\"TELEPORT\",\"eventTime\":" + System.currentTimeMillis() + "}"; // unknown type
        };
    }

    private String pick(List<String> values) {
        return values.get(random.nextInt(values.size()));
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? fallback : v;
    }
}
