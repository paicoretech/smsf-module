package com.paicbd.smsc.smsf.core.config;

import com.paicbd.smsc.utils.Generated;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * The core is producer-only: it publishes MO messages, delivery reports and registration events.
 * The consumer side that used to live here moved to {@code smsf-n21-registration} and
 * {@code smsf-service-mt}.
 */
@Generated
@Configuration
@RequiredArgsConstructor
public class KafkaProducerConfig {
    private final AppProperties appProperties;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    Map<String, Object> producerConfigs() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appProperties.getKafkaBootstrapServers());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, appProperties.getKafkaProducerLingerMs());
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, appProperties.getKafkaProducerBatchSize());
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, appProperties.getKafkaProducerCompressionType());
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, appProperties.isKafkaProducerEnableIdempotence());
        configProps.put(ProducerConfig.ACKS_CONFIG,
                appProperties.isKafkaProducerEnableIdempotence() ? "all" : appProperties.getKafkaProducerAcks());
        return configProps;
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
