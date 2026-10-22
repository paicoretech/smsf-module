package com.paicbd.smsc.smsf.core.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaProducerConfigTest {

    private Map<String, Object> configsFor(int lingerMs, int batchSize, String compression,
                                           boolean idempotence, String acks) {
        AppProperties appProperties = mock(AppProperties.class);
        when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        when(appProperties.getKafkaProducerLingerMs()).thenReturn(lingerMs);
        when(appProperties.getKafkaProducerBatchSize()).thenReturn(batchSize);
        when(appProperties.getKafkaProducerCompressionType()).thenReturn(compression);
        when(appProperties.isKafkaProducerEnableIdempotence()).thenReturn(idempotence);
        when(appProperties.getKafkaProducerAcks()).thenReturn(acks);
        return new KafkaProducerConfig(appProperties).producerConfigs();
    }

    @Test
    @DisplayName("the throughput knobs reach the producer configuration")
    void appliesThroughputKnobs() {
        Map<String, Object> configs = configsFor(10, 65536, "lz4", false, "1");

        assertEquals(10, configs.get(ProducerConfig.LINGER_MS_CONFIG));
        assertEquals(65536, configs.get(ProducerConfig.BATCH_SIZE_CONFIG));
        assertEquals("lz4", configs.get(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        assertEquals(false, configs.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("1", configs.get(ProducerConfig.ACKS_CONFIG));
    }

    @Test
    @DisplayName("acks stays at all while idempotence is enabled, so the producer cannot fail to start")
    void keepsAcksAllWhileIdempotent() {
        Map<String, Object> configs = configsFor(0, 16384, "none", true, "1");

        assertEquals(true, configs.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("all", configs.get(ProducerConfig.ACKS_CONFIG));
    }

    @Test
    @DisplayName("the defaults reproduce the current behaviour")
    void defaultsMatchCurrentBehaviour() {
        Map<String, Object> configs = configsFor(0, 16384, "none", true, "all");

        assertEquals(0, configs.get(ProducerConfig.LINGER_MS_CONFIG));
        assertEquals(16384, configs.get(ProducerConfig.BATCH_SIZE_CONFIG));
        assertEquals("none", configs.get(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        assertEquals("all", configs.get(ProducerConfig.ACKS_CONFIG));
        assertEquals("localhost:9092", configs.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }
}
