package com.paicbd.smsc.smsf.core.processors.multipart;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.Converter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.Objects;

@Slf4j
public class MultiPartsHandler {

    private static final long DEFAULT_CACHE_MAX_SIZE = 50_000L;
    private static final int CACHE_TTL_GRACE_SECONDS = 5;
    private static final int CACHE_MIN_TTL_SECONDS = 30;

    private final ScyllaManager scyllaManager;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final int messagePartsTtl;
    private final Cache<String, ConcatenationAppender> concatenationAppenders;

    public MultiPartsHandler(ScyllaManager scyllaManager,
                             KafkaTemplate<String, String> kafkaTemplate,
                             int messagePartsTtl,
                             int cacheTtlSeconds,
                             long cacheMaxSize) {
        this.scyllaManager = scyllaManager;
        this.kafkaTemplate = kafkaTemplate;
        this.messagePartsTtl = messagePartsTtl;
        long effectiveMaxSize = cacheMaxSize > 0 ? cacheMaxSize : DEFAULT_CACHE_MAX_SIZE;
        long effectiveTtl = cacheTtlSeconds > 0
                ? cacheTtlSeconds
                : Math.max(CACHE_MIN_TTL_SECONDS, messagePartsTtl + CACHE_TTL_GRACE_SECONDS);
        this.concatenationAppenders = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(effectiveTtl))
                .maximumSize(effectiveMaxSize)
                .removalListener((String key, ConcatenationAppender appender, RemovalCause cause) -> {
                    if (cause == RemovalCause.EXPIRED || cause == RemovalCause.SIZE) {
                        int received = appender == null ? 0 : appender.getPartsBuffer().size();
                        int expected = appender == null ? 0 : appender.getTotalParts();
                        log.warn("Multipart appender evicted before completion key={} cause={} parts={}/{}",
                                key, cause, received, expected);
                        try {
                            scyllaManager.deleteSmsfMessagePartsByKey(key);
                        } catch (Exception ex) {
                            log.warn("Failed to clean Scylla after eviction key={}", key, ex);
                        }
                    }
                })
                .build();
    }

    public void messagePartsProcessor(MessageEvent incomingEvent,
                                      UtilsRecords.ConcatenatedMessageDetails details,
                                      String kafkaTopic) {
        String key = generateKey(incomingEvent.getSource(), details.msgReferenceNumber());
        log.debug("Processing part: {} - key: {}", Converter.valueAsString(details), key);

        concatenationAppenders.asMap().compute(key, (k, existing) -> handlePart(k, existing, incomingEvent, details, kafkaTopic));
    }

    private ConcatenationAppender handlePart(String key,
                                             ConcatenationAppender existing,
                                             MessageEvent incomingEvent,
                                             UtilsRecords.ConcatenatedMessageDetails details,
                                             String kafkaTopic) {
        ConcatenationAppender appender = existing;
        boolean existsInScylla = Objects.nonNull(scyllaManager.getSmsfMessagePartsByKey(key));

        if (existsInScylla && Objects.isNull(appender)) {
            log.warn("Found orphaned message part in ScyllaDB for key: {}. Deleting stale entry and treating as new.", key);
            scyllaManager.deleteSmsfMessagePartsByKey(key);
            existsInScylla = false;
        }

        if (!existsInScylla) {
            scyllaManager.insertIntoSmsfMessageParts(key, details.text(), messagePartsTtl);
            appender = new ConcatenationAppender(details.totalSegments(), incomingEvent);
            ConcatenationAppender.AppendResult result = appender.initialize(incomingEvent, details);
            if (result == ConcatenationAppender.AppendResult.INVALID_SEQUENCE) {
                scyllaManager.deleteSmsfMessagePartsByKey(key);
                return null;
            }
            log.info("Created new multipart message with key: {}, TTL: {} seconds", key, messagePartsTtl);
        } else {
            log.debug("Appending to existing multipart message with key: {}", key);
            ConcatenationAppender.AppendResult result = appender.addMessagePart(details, incomingEvent);
            if (result != ConcatenationAppender.AppendResult.ADDED) {
                return appender;
            }
        }

        if (appender.isComplete()) {
            scyllaManager.deleteSmsfMessagePartsByKey(key);
            log.debug("Multipart message completed for key: {}", key);
            kafkaTemplate.send(kafkaTopic, appender.getIncomingEvent().toString());
            log.debug("Appender removed for completed multipart message with key: {}", key);
            return null;
        }

        return appender;
    }

    private String generateKey(String sourceSupi, int msgReferenceNumber) {
        return sourceSupi + "|" + msgReferenceNumber;
    }
}
