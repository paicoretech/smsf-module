package com.paicbd.smsc.smsf.mt.config;

import com.paicbd.smsc.kafka.KafkaConsumerFactory;
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
 * The MT module consumes through {@link KafkaConsumerFactory} rather than Spring listener
 * containers, because the priority handling drives its own poll loop. The producer is still needed
 * to publish delivery reports and retries.
 */
@Generated
@Configuration
@RequiredArgsConstructor
public class KafkaConfig {
    private final AppProperties appProperties;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appProperties.getKafkaBootstrapServers());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }


    /**
     * Deliberately not named {@code kafkaConsumerFactory}: this is the smsc-utils factory, not a
     * Spring {@code ConsumerFactory}, so Spring Boot's KafkaAutoConfiguration does not back off and
     * would clash on that bean name. The monolith only avoided the clash because it also declared a
     * Spring {@code ConsumerFactory}, which this module has no use for.
     */
    @Bean
    public KafkaConsumerFactory smscKafkaConsumerFactory() {
        return new KafkaConsumerFactory();
    }
}
