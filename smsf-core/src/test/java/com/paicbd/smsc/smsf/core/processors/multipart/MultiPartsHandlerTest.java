package com.paicbd.smsc.smsf.core.processors.multipart;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.scylla.ScyllaManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.timeout;

class MultiPartsHandlerTest {

    private static final String TOPIC = "pre.message.medium";
    private static final String SUPI = "imsi-460000203924736";
    private static final String SECOND_SUPI = "imsi-460000203924737";
    private static final int REF = 0xF2;
    private static final int TTL = 30;

    private ScyllaManager scyllaManager;
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    private MultiPartsHandler handler;

    @BeforeEach
    void setUp() {
        scyllaManager = mock(ScyllaManager.class);
        when(scyllaManager.getSmsfMessagePartsByKey(any())).thenReturn(null);
        handler = new MultiPartsHandler(scyllaManager, kafkaTemplate, TTL, TTL + 5, 1_000);
    }

    @Test
    @DisplayName("first part arriving triggers Scylla insert and does not publish to Kafka")
    void firstPartInsertsAndDoesNotPublish() {
        MessageEvent event = event();
        UtilsRecords.ConcatenatedMessageDetails details = details(1, 2, "hello ");

        handler.messagePartsProcessor(event, details, TOPIC);

        verify(scyllaManager).insertIntoSmsfMessageParts(SUPI + "|" + REF, "hello ", TTL);
        verify(kafkaTemplate, never()).send(any(), any());
    }

    @Test
    @DisplayName("all parts arriving in order publish exactly one combined MessageEvent and clean Scylla")
    void allPartsCompletePublishesOnce() {
        UtilsRecords.ConcatenatedMessageDetails p1 = details(1, 2, "hello ");
        UtilsRecords.ConcatenatedMessageDetails p2 = details(2, 2, "world");

        handler.messagePartsProcessor(event(), p1, TOPIC);
        when(scyllaManager.getSmsfMessagePartsByKey(SUPI + "|" + REF)).thenReturn("hello ");
        handler.messagePartsProcessor(event(), p2, TOPIC);

        verify(kafkaTemplate, times(1)).send(eq(TOPIC), any(String.class));
        verify(scyllaManager).deleteSmsfMessagePartsByKey(SUPI + "|" + REF);
    }

    @Test
    @DisplayName("duplicate segment does not publish and Scylla is not double-inserted")
    void duplicateSegmentDoesNotPublish() {
        UtilsRecords.ConcatenatedMessageDetails p1 = details(1, 2, "a");

        handler.messagePartsProcessor(event(), p1, TOPIC);
        when(scyllaManager.getSmsfMessagePartsByKey(SUPI + "|" + REF)).thenReturn("a");
        handler.messagePartsProcessor(event(), p1, TOPIC);

        verify(scyllaManager, times(1)).insertIntoSmsfMessageParts(any(), any(), anyInt());
        verify(kafkaTemplate, never()).send(any(), any());
    }

    @Test
    @DisplayName("invalid sequence on first part clears Scylla and drops appender")
    void invalidSequenceOnFirstDropsAppender() {
        UtilsRecords.ConcatenatedMessageDetails bad = details(0, 2, "x");

        handler.messagePartsProcessor(event(), bad, TOPIC);

        verify(scyllaManager).insertIntoSmsfMessageParts(any(), any(), anyInt());
        verify(scyllaManager).deleteSmsfMessagePartsByKey(SUPI + "|" + REF);
        verify(kafkaTemplate, never()).send(any(), any());
    }

    @Test
    @DisplayName("orphan Scylla entry with no in-memory appender is cleaned and the part is treated as new")
    void orphanScyllaEntryIsCleaned() {
        when(scyllaManager.getSmsfMessagePartsByKey(SUPI + "|" + REF)).thenReturn("stale");
        UtilsRecords.ConcatenatedMessageDetails p1 = details(1, 2, "first");

        handler.messagePartsProcessor(event(), p1, TOPIC);

        verify(scyllaManager).deleteSmsfMessagePartsByKey(SUPI + "|" + REF);
        verify(scyllaManager).insertIntoSmsfMessageParts(SUPI + "|" + REF, "first", TTL);
        verify(kafkaTemplate, never()).send(any(), any());
    }

    @Test
    @DisplayName("2-part message where second arrives first still completes when the first one lands")
    void outOfOrderPartsStillComplete() {
        UtilsRecords.ConcatenatedMessageDetails p1 = details(1, 2, "hello ");
        UtilsRecords.ConcatenatedMessageDetails p2 = details(2, 2, "world");

        handler.messagePartsProcessor(event(), p2, TOPIC);
        when(scyllaManager.getSmsfMessagePartsByKey(SUPI + "|" + REF)).thenReturn("world");
        handler.messagePartsProcessor(event(), p1, TOPIC);

        verify(kafkaTemplate, times(1)).send(eq(TOPIC), any(String.class));
    }

    @Test
    @DisplayName("keys are scoped per (sourceSupi, refNumber) so parallel messages do not collide")
    void keysAreScopedPerSourceAndRef() {
        int otherRefNumber = 0xAB;
        UtilsRecords.ConcatenatedMessageDetails otherRef =
                new UtilsRecords.ConcatenatedMessageDetails(otherRefNumber, 2, 1, "z");

        handler.messagePartsProcessor(event(), details(1, 2, "a"), TOPIC);
        handler.messagePartsProcessor(event(), otherRef, TOPIC);

        verify(scyllaManager).insertIntoSmsfMessageParts(SUPI + "|" + REF, "a", TTL);
        verify(scyllaManager).insertIntoSmsfMessageParts(SUPI + "|" + otherRefNumber, "z", TTL);
    }

    @Test
    @DisplayName("constructor uses fallback ttl and max size when non-positive values are provided")
    void constructorUsesFallbackTtlAndMaxSizeWhenInputsAreNonPositive() throws Exception {
        handler = new MultiPartsHandler(scyllaManager, kafkaTemplate, TTL, 0, 0);

        Cache<String, ConcatenationAppender> cache = extractCache(handler);

        assertNotNull(cache);
        handler.messagePartsProcessor(event(), details(1, 2, "hello "), TOPIC);
        verify(scyllaManager).insertIntoSmsfMessageParts(SUPI + "|" + REF, "hello ", TTL);
    }

    @Test
    @DisplayName("cache eviction by size removes multipart state from Scylla")
    void cacheEvictionBySizeRemovesMultipartStateFromScylla() {
        handler = new MultiPartsHandler(scyllaManager, kafkaTemplate, TTL, TTL + 5, 1);

        handler.messagePartsProcessor(event(), details(1, 2, "first"), TOPIC);
        handler.messagePartsProcessor(secondEvent(), secondDetails(1, 2, "other"), TOPIC);

        verify(scyllaManager, timeout(1_000))
                .deleteSmsfMessagePartsByKey(SUPI + "|" + REF);
    }

    @Test
    @DisplayName("cache eviction cleanup failure is swallowed")
    void cacheEvictionCleanupFailureIsSwallowed() {
        doThrow(new RuntimeException("cleanup-failed"))
                .when(scyllaManager).deleteSmsfMessagePartsByKey(SUPI + "|" + REF);
        ConcatenationAppender appender = new ConcatenationAppender(2, event());
        appender.initialize(event(), details(1, 2, "first"));

        assertDoesNotThrow(() ->
                invokeRemovalListener(handler, SUPI + "|" + REF, appender, RemovalCause.SIZE));

        verify(scyllaManager, times(1)).deleteSmsfMessagePartsByKey(SUPI + "|" + REF);
    }

    @Test
    @DisplayName("removal listener handles null appender during eviction cleanup")
    void removalListenerHandlesNullAppenderDuringEvictionCleanup() {
        assertDoesNotThrow(() ->
                invokeRemovalListener(handler, SUPI + "|" + REF, null, RemovalCause.SIZE));

        verify(scyllaManager).deleteSmsfMessagePartsByKey(SUPI + "|" + REF);
    }

    private MessageEvent event() {
        return MessageEvent.builder()
                .id("1720701000000-100001")
                .messageId("1720701000000-200001")
                .parentId("1720701000000-300001")
                .source(SUPI)
                .build();
    }

    private MessageEvent secondEvent() {
        return MessageEvent.builder()
                .id("1720701000000-100002")
                .messageId("1720701000000-200002")
                .parentId("1720701000000-300002")
                .source(SECOND_SUPI)
                .build();
    }

    private UtilsRecords.ConcatenatedMessageDetails details(int seq, int total, String text) {
        return new UtilsRecords.ConcatenatedMessageDetails(REF, total, seq, text);
    }

    private UtilsRecords.ConcatenatedMessageDetails secondDetails(int seq, int total, String text) {
        return new UtilsRecords.ConcatenatedMessageDetails(0xAB, total, seq, text);
    }

    @SuppressWarnings("unchecked")
    private Cache<String, ConcatenationAppender> extractCache(MultiPartsHandler target) throws Exception {
        Field field = MultiPartsHandler.class.getDeclaredField("concatenationAppenders");
        field.setAccessible(true);
        return (Cache<String, ConcatenationAppender>) field.get(target);
    }

    @SuppressWarnings("unchecked")
    private void invokeRemovalListener(MultiPartsHandler target,
                                       String key,
                                       ConcatenationAppender appender,
                                       RemovalCause cause) throws Exception {
        Cache<String, ConcatenationAppender> cache = extractCache(target);
        Method cacheMethod = cache.getClass().getDeclaredMethod("cache");
        cacheMethod.setAccessible(true);
        Object localCache = cacheMethod.invoke(cache);
        Method removalListenerMethod = localCache.getClass().getMethod("removalListener");
        removalListenerMethod.setAccessible(true);
        RemovalListener<String, ConcatenationAppender> removalListener =
                (RemovalListener<String, ConcatenationAppender>) removalListenerMethod.invoke(localCache);
        removalListener.onRemoval(key, appender, cause);
    }
}
