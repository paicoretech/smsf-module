package com.paicbd.smsc.smsf.common.e2e;

import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.RedisManager;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Real Kafka and Redis for the end-to-end tests, started once per JVM and shared by the three
 * modules through the {@code smsf-common} test-jar.
 *
 * <p>These are actual brokers and an actual Redis listening on real ports, so the traffic the tests
 * generate is observable from outside the JVM — the point of an end-to-end test here is that the
 * message really crosses the wire, not that a mock was called.</p>
 *
 * <p>Redis runs standalone rather than clustered: it is the mode {@code redis.standalone.enabled}
 * enables, it boots in a second, and the module code path is the same {@link RedisManager} either
 * way.</p>
 */
public final class SmsfE2EInfrastructure {

    private static final int REDIS_CONTAINER_PORT = 6379;

    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                    .withExposedPorts(REDIS_CONTAINER_PORT)
                    .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));

    private static volatile boolean started;

    private SmsfE2EInfrastructure() {
    }

    /** Boots the containers on first use. Subsequent calls are cheap. */
    public static synchronized void start() {
        if (started) {
            return;
        }
        try {
            KAFKA.start();
            REDIS.start();
            started = true;
        } catch (RuntimeException ex) {
            REDIS.stop();
            KAFKA.stop();
            throw ex;
        }
        // Testcontainers' reaper already removes the containers when the JVM dies; this makes the
        // release deterministic so the ports are free the moment the suite finishes.
        Runtime.getRuntime().addShutdownHook(new Thread(SmsfE2EInfrastructure::stop, "smsf-e2e-teardown"));
        announcePorts();
    }

    /** Stops the containers and gives their ports back. Idempotent. */
    public static synchronized void stop() {
        if (!started) {
            return;
        }
        started = false;
        try {
            KAFKA.stop();
        } finally {
            REDIS.stop();
        }
    }

    /**
     * Prints the ports the containers landed on. They are dynamic, so this is what makes the
     * traffic findable in a packet capture: everything runs over loopback, and these are the
     * ports to point a `tcp.port` filter at.
     */
    private static void announcePorts() {
        System.out.printf("%n=== SMSF e2e infrastructure (dynamic loopback ports) ===%n"
                        + "  Kafka   127.0.0.1:%d%n"
                        + "  Redis   127.0.0.1:%d%n%n",
                KAFKA.getMappedPort(9093), REDIS.getMappedPort(REDIS_CONTAINER_PORT));
    }

    public static String kafkaBootstrapServers() {
        start();
        return KAFKA.getBootstrapServers();
    }

    public static String redisHostAndPort() {
        start();
        return REDIS.getHost() + ":" + REDIS.getMappedPort(REDIS_CONTAINER_PORT);
    }

    /** A {@link RedisManager} on the real container, in standalone mode. */
    public static RedisManager redisManager() {
        return Converter.paramsToRedisManager(new UtilsRecords.JedisConfigParams(
                List.of(redisHostAndPort()), 20, 20, 1, true, 2000, 2000, 20, "", "", true));
    }

    /**
     * Creates the topics up front. The modules consume with {@code auto.offset.reset=earliest}, so a
     * topic that only appears when the first record is produced would let a consumer miss it.
     */
    public static void createTopics(String... topics) {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", kafkaBootstrapServers()))) {
            Set<String> existing = admin.listTopics().names().get();
            List<NewTopic> missing = java.util.Arrays.stream(topics)
                    .filter(topic -> !existing.contains(topic))
                    .map(topic -> new NewTopic(topic, 1, (short) 1))
                    .toList();
            if (!missing.isEmpty()) {
                admin.createTopics(missing).all().get();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while creating the topics", ex);
        } catch (ExecutionException ex) {
            throw new IllegalStateException("could not create the topics", ex);
        }
    }

    /** Produces one record and waits for the broker to acknowledge it. */
    public static void produce(String topic, String key, String value) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBootstrapServers());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, key, value)).get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while producing to " + topic, ex);
        } catch (ExecutionException ex) {
            throw new IllegalStateException("could not produce to " + topic, ex);
        }
    }

    /** Reads whatever is on the topic from the beginning, for asserting what a module published. */
    public static List<String> consumeAll(String topic, String groupId, Duration timeout) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBootstrapServers());
        props.put("group.id", groupId);
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            return java.util.stream.StreamSupport
                    .stream(consumer.poll(timeout).spliterator(), false)
                    .map(org.apache.kafka.clients.consumer.ConsumerRecord::value)
                    .toList();
        }
    }
}
