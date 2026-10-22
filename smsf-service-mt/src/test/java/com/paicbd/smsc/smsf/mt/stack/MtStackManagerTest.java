package com.paicbd.smsc.smsf.mt.stack;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.kafka.KafkaConsumerCustomImpl;
import com.paicbd.smsc.kafka.KafkaConsumerFactory;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenCache;
import com.paicbd.smsc.smsf.common.utils.Http2GatewayCreator;
import com.paicbd.smsc.smsf.mt.config.AppProperties;
import com.paicbd.smsc.smsf.mt.http2.Http2ClientConnection;
import com.paicbd.smsc.smsf.mt.processors.MessageProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.kafka.core.KafkaTemplate;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.paicbd.smsc.smsf.common.constants.Constants.ENABLED;
import static com.paicbd.smsc.smsf.common.constants.Constants.NF_TYPE_AMF;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

/**
 * MT slice of the stack that used to be a single {@code Http2StackManager}: the priority consumers,
 * the poll loop and the AMF client, plus the Redis-sourced credential that replaced the in-process
 * token refresher.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MtStackManagerTest {

    private static final long MARGIN = 30L;

    @Mock
    private AppProperties appProperties;

    @Mock
    private KafkaConsumerFactory kafkaConsumerFactory;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private ScyllaManager scyllaManager;

    @Mock
    private SmsfTokenCache tokenCache;

    private Http2Gateway gateway;

    @BeforeEach
    void setUp() {
        // The MT host is the full sendsms URL template — the request path is derived from it.
        gateway = Http2GatewayCreator.buildHttp2GatewayWithMtls("smsf-gw", 10,
                "https://smsf.local:8443/nsmsf-sms/v2/ue-contexts/{supi}/sendsms", ENABLED);
        when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        when(appProperties.getOutboundTokenRefreshMarginSeconds()).thenReturn(MARGIN);
        when(appProperties.getTokenCheckIntervalSeconds()).thenReturn(10L);
    }

    // --- construction ---------------------------------------------------------

    @Test
    @DisplayName("constructor, when called, then exposes the gateway, the AMF client and the processors")
    void constructorExposesCollaborators() {
        MtStackManager manager = newManager();

        assertEquals(gateway, manager.getGateway());
        assertNotNull(manager.getClientConnection());
        assertNotNull(manager.getMessageFactory());
        assertNotNull(manager.getMessageProcessing());
        assertNotNull(manager.getCdrProcessor());
        assertFalse(manager.isStarted());
    }

    @Test
    @DisplayName("the bearer handed to the processor comes from Redis, not from a local refresher")
    void bearerSupplierReadsRedis() throws Exception {
        when(tokenCache.authorizationHeader(gateway.getNetworkId(), NF_TYPE_AMF))
                .thenReturn(Optional.of("Bearer from-redis"));

        MtStackManager manager = newManager();

        assertEquals(Optional.of("Bearer from-redis"), invokeBearerSupplier(manager));
        verify(tokenCache).authorizationHeader(gateway.getNetworkId(), NF_TYPE_AMF);
    }

    @Test
    @DisplayName("with no usable token in Redis the bearer is empty rather than a stale value")
    void bearerSupplierEmptyWhenNoToken() throws Exception {
        when(tokenCache.authorizationHeader(anyInt(), any())).thenReturn(Optional.empty());

        assertEquals(Optional.empty(), invokeBearerSupplier(newManager()));
    }

    @Test
    @DisplayName("the bearer is resolved per request, so a token refreshed by smsf-core is picked up")
    void bearerSupplierResolvesPerCall() throws Exception {
        when(tokenCache.authorizationHeader(anyInt(), any()))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of("Bearer refreshed"));

        MtStackManager manager = newManager();

        assertEquals(Optional.empty(), invokeBearerSupplier(manager));
        assertEquals(Optional.of("Bearer refreshed"), invokeBearerSupplier(manager));
    }

    // --- lifecycle ------------------------------------------------------------

    @Test
    @DisplayName("start, when called the first time, then connects the AMF client and starts the consumers")
    void startStartsCollaborators() throws Exception {
        MtStackManager manager = newManager();
        Http2ClientConnection clientMock = mock(Http2ClientConnection.class);
        replaceField(manager, "clientConnection", clientMock);

        try (MockedConstruction<KafkaConsumerCustomImpl> mocked =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();

            assertTrue(manager.isStarted());
            verify(clientMock).connect();
            assertEquals(3, mocked.constructed().size());
            mocked.constructed().forEach(c -> verify(c).startConsumer());
            manager.stop();
        }
    }

    @Test
    @DisplayName("start, when the host carries no scheme, then it is rejected instead of sending to a bogus path")
    void startRejectsHostWithoutScheme() {
        // Without a scheme there is no way to tell the authority from the path, so parsePath would
        // hand back the whole string and every delivery would go out to a nonsensical URI.
        gateway.setHost("smsf.local:8443");
        MtStackManager manager = newManager();

        assertThatThrownBy(manager::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be an absolute URL including the scheme")
                .hasMessageContaining("smsf.local:8443");

        assertFalse(manager.isStarted());
    }

    @Test
    @DisplayName("start, when already started, then it is a no-op")
    void startWhenAlreadyStartedThenNoOp() throws Exception {
        MtStackManager manager = newManager();
        replaceField(manager, "clientConnection", mock(Http2ClientConnection.class));

        try (MockedConstruction<KafkaConsumerCustomImpl> mocked =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();
            int afterFirst = mocked.constructed().size();
            manager.start();

            assertEquals(afterFirst, mocked.constructed().size());
            assertTrue(manager.isStarted());
            manager.stop();
        }
    }

    @Test
    @DisplayName("start, when only high MPS is configured, then only the high consumer is created")
    void startWhenOnlyHighMpsThenSingleConsumer() throws Exception {
        gateway.setMessagesPerSecondHigh(50);
        gateway.setMessagesPerSecondMedium(0);
        gateway.setMessagesPerSecondLow(0);

        MtStackManager manager = newManager();
        replaceField(manager, "clientConnection", mock(Http2ClientConnection.class));

        try (MockedConstruction<KafkaConsumerCustomImpl> mocked =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();

            assertEquals(1, mocked.constructed().size());
            verify(mocked.constructed().getFirst()).startConsumer();
            manager.stop();
        }
    }

    @Test
    @DisplayName("start, when every MPS is zero, then no kafka consumer is created")
    void startWhenAllMpsZeroThenNoConsumers() throws Exception {
        gateway.setMessagesPerSecond(0);
        gateway.setMessagesPerSecondHigh(0);
        gateway.setMessagesPerSecondMedium(0);
        gateway.setMessagesPerSecondLow(0);

        MtStackManager manager = newManager();
        replaceField(manager, "clientConnection", mock(Http2ClientConnection.class));

        try (MockedConstruction<KafkaConsumerCustomImpl> mocked =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();

            assertTrue(mocked.constructed().isEmpty());
            assertTrue(manager.isStarted());
            manager.stop();
        }
    }

    @Test
    @DisplayName("start, when the AMF client fails to connect, then the started flag is reset and it propagates")
    void startWhenClientConnectFailsThenResetState() throws Exception {
        MtStackManager manager = newManager();
        Http2ClientConnection clientMock = mock(Http2ClientConnection.class);
        doThrow(new RuntimeException("connect-failed")).when(clientMock).connect();
        replaceField(manager, "clientConnection", clientMock);

        RuntimeException ex = assertThrows(RuntimeException.class, manager::start);

        assertEquals("connect-failed", ex.getMessage());
        assertFalse(manager.isStarted());
        verify(clientMock).disconnect();
    }

    @Test
    @DisplayName("stop, when not started, then it is a no-op and does not throw")
    void stopWhenNotStartedThenNoOp() {
        MtStackManager manager = newManager();

        assertDoesNotThrow(manager::stop);
        assertFalse(manager.isStarted());
    }

    @Test
    @DisplayName("stop, when started, then consumers are dropped and the AMF client disconnected")
    void stopReleasesResources() throws Exception {
        MtStackManager manager = newManager();
        Http2ClientConnection clientMock = mock(Http2ClientConnection.class);
        replaceField(manager, "clientConnection", clientMock);

        try (MockedConstruction<KafkaConsumerCustomImpl> mocked =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();
            manager.stop();

            assertFalse(manager.isStarted());
            mocked.constructed().forEach(c -> verify(c).stopConsumer());
            verify(clientMock).disconnect();
            assertNull(getField(manager, "kafkaConsumerHigh"));
            assertNull(getField(manager, "dispatchExecutor"));
        }
    }

    @Test
    @DisplayName("isStarted, on a fresh manager, then false")
    void isStartedInitialStateThenFalse() {
        assertFalse(newManager().isStarted());
    }

    // --- consumers and dispatch ----------------------------------------------

    @Test
    @DisplayName("pauseConsumers and startConsumers toggle every priority consumer")
    void pauseAndResumeConsumersTogglesKafkaConsumers() throws Exception {
        MtStackManager manager = newManager();
        replaceField(manager, "clientConnection", mock(Http2ClientConnection.class));

        try (MockedConstruction<KafkaConsumerCustomImpl> mocked =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();
            invokePrivate(manager, "pauseConsumers");
            for (KafkaConsumerCustomImpl consumer : mocked.constructed()) {
                verify(consumer, atLeastOnce()).stopConsumer();
            }

            invokePrivate(manager, "startConsumers");
            for (KafkaConsumerCustomImpl consumer : mocked.constructed()) {
                verify(consumer, atLeast(2)).startConsumer();
            }
            manager.stop();
        }
    }

    @Test
    @DisplayName("pauseConsumers and startConsumers, when there are no consumers, then no-op")
    void pauseAndResumeConsumersWhenNullThenNoOp() {
        MtStackManager manager = newManager();

        assertDoesNotThrow(() -> invokePrivate(manager, "pauseConsumers"));
        assertDoesNotThrow(() -> invokePrivate(manager, "startConsumers"));
    }

    @Test
    @DisplayName("pollMessages, when not started, then nothing is fetched or dispatched")
    void pollMessagesWhenNotStartedThenNoDispatch() throws Exception {
        MtStackManager manager = spy(newManager());
        ExecutorService dispatchMock = mock(ExecutorService.class);
        replaceField(manager, "dispatchExecutor", dispatchMock);

        assertDoesNotThrow(() -> invokePrivate(manager, "pollMessages"));

        verify(dispatchMock, never()).execute(any(Runnable.class));
        verify(manager, never()).fetchMessages(anyInt(), anyInt(), anyInt(), anyInt(), any(), any(), any());
    }

    @Test
    @DisplayName("pollMessages, while paused for a missing token, then nothing is fetched")
    void pollMessagesWhilePausedForTokenThenNoFetch() throws Exception {
        MtStackManager manager = spy(newManager());
        setStarted(manager);
        pausedForToken(manager).set(true);

        assertDoesNotThrow(() -> invokePrivate(manager, "pollMessages"));

        verify(manager, never()).fetchMessages(anyInt(), anyInt(), anyInt(), anyInt(), any(), any(), any());
    }

    @Test
    @DisplayName("pollMessages, when kafka returns nothing, then nothing is dispatched")
    void pollMessagesWhenNoMessagesThenNoDispatch() throws Exception {
        MtStackManager manager = spy(newManager());
        setStarted(manager);
        ExecutorService dispatchMock = mock(ExecutorService.class);
        replaceField(manager, "dispatchExecutor", dispatchMock);
        doReturn(List.of()).when(manager)
                .fetchMessages(anyInt(), anyInt(), anyInt(), anyInt(), any(), any(), any());

        assertDoesNotThrow(() -> invokePrivate(manager, "pollMessages"));

        verify(dispatchMock, never()).execute(any(Runnable.class));
    }

    @Test
    @DisplayName("pollMessages, when fetching throws, then the error is swallowed and the loop survives")
    void pollMessagesWhenFetchThrowsThenNoPropagation() throws Exception {
        MtStackManager manager = spy(newManager());
        setStarted(manager);
        doThrow(new RuntimeException("poll-failed")).when(manager)
                .fetchMessages(anyInt(), anyInt(), anyInt(), anyInt(), any(), any(), any());

        assertDoesNotThrow(() -> invokePrivate(manager, "pollMessages"));
    }

    @Test
    @DisplayName("pollMessages, when messages are fetched, then each one is dispatched")
    void pollMessagesWhenMessagesFetchedThenDispatched() throws Exception {
        MtStackManager manager = spy(newManager());
        setStarted(manager);
        ExecutorService dispatchMock = mock(ExecutorService.class);
        replaceField(manager, "dispatchExecutor", dispatchMock);
        doReturn(List.of(event(1), event(2))).when(manager)
                .fetchMessages(anyInt(), anyInt(), anyInt(), anyInt(), any(), any(), any());

        assertDoesNotThrow(() -> invokePrivate(manager, "pollMessages"));

        verify(dispatchMock, times(2)).execute(any(Runnable.class));
    }

    @Test
    @DisplayName("dispatchAll submits every event to the processor through the dispatch executor")
    void dispatchAllSubmitsEachEvent() throws Exception {
        MtStackManager manager = newManager();
        MessageProcessor processingMock = mock(MessageProcessor.class);
        ExecutorService dispatchMock = mock(ExecutorService.class);
        replaceField(manager, "messageProcessing", processingMock);
        replaceField(manager, "dispatchExecutor", dispatchMock);
        doAnswer(inv -> {
            inv.getArgument(0, Runnable.class).run();
            return null;
        }).when(dispatchMock).execute(any(Runnable.class));

        MessageEvent first = event(1);
        MessageEvent second = event(2);

        var batch = List.of(first, second);

        assertDoesNotThrow(() -> invokePrivate(manager, "dispatchAll", new Class[]{List.class}, batch));

        verify(dispatchMock, times(2)).execute(any(Runnable.class));
        verify(processingMock).processOutgoingRequest(first);
        verify(processingMock).processOutgoingRequest(second);
    }

    // --- token tracking -------------------------------------------------------

    @Test
    @DisplayName("with OAuth2 disabled the consumers start straight away and the cache is not involved")
    void noTrackingWhenOauth2Disabled() throws Exception {
        gateway.getOutboundSecurity().setOauth2Enabled(false);
        MtStackManager manager = newManager();
        replaceField(manager, "clientConnection", mock(Http2ClientConnection.class));

        try (MockedConstruction<KafkaConsumerCustomImpl> mocked =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();

            verify(tokenCache, never()).register(anyInt(), any(), any(), any());
            mocked.constructed().forEach(c -> verify(c).startConsumer());
            assertFalse(pausedForToken(manager).get());
            manager.stop();
        }
    }

    @Test
    @DisplayName("with OAuth2 enabled the consumers wait for the cache instead of starting blind")
    void consumersWaitForTheCache() throws Exception {
        enableOauth2();
        MtStackManager manager = newManager();
        replaceField(manager, "clientConnection", mock(Http2ClientConnection.class));

        try (MockedConstruction<KafkaConsumerCustomImpl> mocked =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();

            captureTokenCallbacks();
            mocked.constructed().forEach(c -> verify(c, never()).startConsumer());
            manager.stop();
        }
    }

    @Test
    @DisplayName("the consumers start when the cache reports the token available")
    void consumersStartWhenTokenAvailable() throws Exception {
        enableOauth2();
        MtStackManager manager = newManager();
        replaceField(manager, "clientConnection", mock(Http2ClientConnection.class));

        try (MockedConstruction<KafkaConsumerCustomImpl> mocked =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();
            captureTokenCallbacks()[0].run();

            mocked.constructed().forEach(c -> verify(c).startConsumer());
            assertFalse(pausedForToken(manager).get());
            manager.stop();
        }
    }

    @Test
    @DisplayName("the consumers are paused when the cache reports the credential lost")
    void consumersPauseWhenTokenLost() throws Exception {
        enableOauth2();
        MtStackManager manager = newManager();
        replaceField(manager, "clientConnection", mock(Http2ClientConnection.class));

        try (MockedConstruction<KafkaConsumerCustomImpl> mocked =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();
            Runnable[] callbacks = captureTokenCallbacks();
            callbacks[0].run();

            callbacks[1].run();

            assertTrue(pausedForToken(manager).get());
            mocked.constructed().forEach(c -> verify(c, atLeastOnce()).stopConsumer());
            manager.stop();
        }
    }

    @Test
    @DisplayName("the consumers are resumed when a fresh token lands")
    void consumersResumeWhenTokenReturns() throws Exception {
        enableOauth2();
        MtStackManager manager = newManager();
        replaceField(manager, "clientConnection", mock(Http2ClientConnection.class));

        try (MockedConstruction<KafkaConsumerCustomImpl> mocked =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();
            Runnable[] callbacks = captureTokenCallbacks();
            callbacks[0].run();
            callbacks[1].run();
            assertTrue(pausedForToken(manager).get());

            callbacks[0].run();

            assertFalse(pausedForToken(manager).get());
            mocked.constructed().forEach(c -> verify(c, atLeast(2)).startConsumer());
            manager.stop();
        }
    }

    @Test
    @DisplayName("stop unregisters the gateway from the cache so no refresh cycle is left behind")
    void stopUnregistersFromTheCache() throws Exception {
        enableOauth2();
        MtStackManager manager = newManager();
        replaceField(manager, "clientConnection", mock(Http2ClientConnection.class));

        try (MockedConstruction<KafkaConsumerCustomImpl> ignored =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();
            manager.stop();

            verify(tokenCache).unregister(gateway.getNetworkId(), NF_TYPE_AMF);
            assertFalse(pausedForToken(manager).get());
        }
    }

    // --- helpers --------------------------------------------------------------

    /** Captures the callbacks the stack hands to the cache, which is what drives the consumers. */
    private Runnable[] captureTokenCallbacks() {
        ArgumentCaptor<Runnable> onAvailable = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<Runnable> onUnavailable = ArgumentCaptor.forClass(Runnable.class);
        verify(tokenCache).register(eq(gateway.getNetworkId()), eq(NF_TYPE_AMF),
                onAvailable.capture(), onUnavailable.capture());
        return new Runnable[]{onAvailable.getValue(), onUnavailable.getValue()};
    }

    /** The shared gateway fixture ships OAuth2 disabled; the token gate only applies when it is on. */
    private void enableOauth2() {
        gateway.getOutboundSecurity().setOauth2Enabled(true);
    }

    private MtStackManager newManager() {
        return new MtStackManager(gateway, appProperties, kafkaConsumerFactory, kafkaTemplate,
                scyllaManager, tokenCache, new ConcurrentHashMap<>());
    }

    private static MessageEvent event(int sequence) {
        MessageEvent event = new MessageEvent();
        event.setMessageId("1720569600000-9876543210" + sequence);
        return event;
    }

    @SuppressWarnings("unchecked")
    // --- watcher lifecycle ----------------------------------------------------

    @Test
    @DisplayName("stopping a running gateway releases its throughput watchers")
    void stopReleasesTheWatchers() throws Exception {
        MtStackManager manager = newManager();
        MessageProcessor processor = mock(MessageProcessor.class);
        replaceField(manager, "clientConnection", mock(Http2ClientConnection.class));
        replaceField(manager, "messageProcessing", processor);

        try (MockedConstruction<KafkaConsumerCustomImpl> ignored =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();
            manager.stop();
        }

        verify(processor).stopWatchers();
    }

    @Test
    @DisplayName("stopping a gateway that never started still releases its watchers")
    void stopReleasesTheWatchersEvenIfNeverStarted() throws Exception {
        // The watchers are started by the MessageProcessor constructor, so a manager that was built
        // and never started owns two live threads. Returning early here used to leak them.
        MtStackManager manager = newManager();
        MessageProcessor processor = mock(MessageProcessor.class);
        replaceField(manager, "messageProcessing", processor);

        manager.stop();

        verify(processor).stopWatchers();
    }

    // --- AMF connection watchdog ----------------------------------------------

    @Test
    @DisplayName("watchdog, when the AMF link drops, then it pauses the consumers, logs it and retries")
    void watchdogPausesAndReconnectsWhenTheLinkDrops() throws Exception {
        MtStackManager manager = newManager();
        Http2ClientConnection client = mock(Http2ClientConnection.class);
        when(client.isConnected()).thenReturn(false);
        replaceField(manager, "clientConnection", client);

        try (MockedConstruction<KafkaConsumerCustomImpl> mocked =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();
            // start() connects once by itself; from here on the counts are the watchdog's alone.
            mocked.constructed().forEach(Mockito::reset);
            reset(client);
            when(client.isConnected()).thenReturn(false);

            invokePrivate(manager, "checkConnection");

            // Paused, so messages stay on their topics instead of being drained into a dead client.
            mocked.constructed().forEach(consumer -> verify(consumer).stopConsumer());
            mocked.constructed().forEach(consumer -> verify(consumer, never()).startConsumer());
            verify(client).connect();
            manager.stop();
        }
    }

    @Test
    @DisplayName("watchdog, when the AMF link comes back, then the consumers resume without a restart")
    void watchdogResumesWhenTheLinkComesBack() throws Exception {
        MtStackManager manager = newManager();
        Http2ClientConnection client = mock(Http2ClientConnection.class);
        when(client.isConnected()).thenReturn(false);
        replaceField(manager, "clientConnection", client);

        try (MockedConstruction<KafkaConsumerCustomImpl> mocked =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();
            invokePrivate(manager, "checkConnection");
            mocked.constructed().forEach(Mockito::reset);

            when(client.isConnected()).thenReturn(true);
            invokePrivate(manager, "checkConnection");

            mocked.constructed().forEach(consumer -> verify(consumer).startConsumer());
            manager.stop();
        }
    }

    @Test
    @DisplayName("watchdog, while the AMF token is missing, then a restored link does not resume on its own")
    void watchdogDoesNotOverrideTheTokenGate() throws Exception {
        gateway.getOutboundSecurity().setOauth2Enabled(true);
        MtStackManager manager = newManager();
        Http2ClientConnection client = mock(Http2ClientConnection.class);
        when(client.isConnected()).thenReturn(false);
        replaceField(manager, "clientConnection", client);

        try (MockedConstruction<KafkaConsumerCustomImpl> mocked =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();
            invokePrivate(manager, "onTokenUnavailable");
            invokePrivate(manager, "checkConnection");
            mocked.constructed().forEach(Mockito::reset);

            when(client.isConnected()).thenReturn(true);
            invokePrivate(manager, "checkConnection");

            // The link is back but the credential is still gone: the other gate keeps them paused.
            mocked.constructed().forEach(consumer -> verify(consumer, never()).startConsumer());
            manager.stop();
        }
    }

    @Test
    @DisplayName("watchdog, on a cold start with no token published, then a reconnection does not start the consumers")
    void watchdogDoesNotStartConsumersWithoutAnyToken() throws Exception {
        // OAuth2 required and smsf-core has published nothing yet: the consumers were never started
        // and nothing was ever "lost", so the paused-for-token flag is still false.
        gateway.getOutboundSecurity().setOauth2Enabled(true);
        MtStackManager manager = newManager();
        Http2ClientConnection client = mock(Http2ClientConnection.class);
        when(client.isConnected()).thenReturn(false);
        replaceField(manager, "clientConnection", client);

        try (MockedConstruction<KafkaConsumerCustomImpl> mocked =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();
            invokePrivate(manager, "checkConnection");
            mocked.constructed().forEach(Mockito::reset);

            when(client.isConnected()).thenReturn(true);
            invokePrivate(manager, "checkConnection");

            mocked.constructed().forEach(consumer -> verify(consumer, never()).startConsumer());
            manager.stop();
        }
    }

    @Test
    @DisplayName("watchdog, when the token is there, then a reconnection does start the consumers")
    void watchdogStartsConsumersWhenTheTokenIsCached() throws Exception {
        gateway.getOutboundSecurity().setOauth2Enabled(true);
        MtStackManager manager = newManager();
        Http2ClientConnection client = mock(Http2ClientConnection.class);
        when(client.isConnected()).thenReturn(false);
        replaceField(manager, "clientConnection", client);

        try (MockedConstruction<KafkaConsumerCustomImpl> mocked =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();
            invokePrivate(manager, "onTokenAvailable");
            invokePrivate(manager, "checkConnection");
            mocked.constructed().forEach(Mockito::reset);

            when(client.isConnected()).thenReturn(true);
            invokePrivate(manager, "checkConnection");

            mocked.constructed().forEach(consumer -> verify(consumer).startConsumer());
            manager.stop();
        }
    }

    @Test
    @DisplayName("watchdog, when the outage lasts, then the error is logged once and not on every retry")
    void watchdogLogsTheOutageOnce() throws Exception {
        MtStackManager manager = newManager();
        Http2ClientConnection client = mock(Http2ClientConnection.class);
        when(client.isConnected()).thenReturn(false);
        replaceField(manager, "clientConnection", client);

        try (MockedConstruction<KafkaConsumerCustomImpl> mocked =
                     mockConstruction(KafkaConsumerCustomImpl.class)) {
            manager.start();
            mocked.constructed().forEach(Mockito::reset);
            reset(client);
            when(client.isConnected()).thenReturn(false);

            invokePrivate(manager, "checkConnection");
            invokePrivate(manager, "checkConnection");
            invokePrivate(manager, "checkConnection");

            // Paused once on the first tick; the later ticks only retry the connection.
            mocked.constructed().forEach(consumer -> verify(consumer, times(1)).stopConsumer());
            verify(client, times(3)).connect();
            manager.stop();
        }
    }

    private Optional<String> invokeBearerSupplier(MtStackManager manager) throws Exception {
        Method method = MtStackManager.class.getDeclaredMethod("currentBearerHeader");
        method.setAccessible(true);
        return (Optional<String>) method.invoke(manager);
    }






    private static void replaceField(MtStackManager manager, String name, Object value) throws Exception {
        Field field = MtStackManager.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(manager, value);
    }

    private static Object getField(MtStackManager manager, String name) throws Exception {
        Field field = MtStackManager.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(manager);
    }

    private static void assertNull(Object value) {
        org.junit.jupiter.api.Assertions.assertNull(value);
    }

    private static AtomicBoolean pausedForToken(MtStackManager manager) throws Exception {
        return (AtomicBoolean) getField(manager, "pausedForToken");
    }

    private static void setStarted(MtStackManager manager) throws Exception {
        ((AtomicBoolean) getField(manager, "started")).set(true);
    }

    private static void invokePrivate(MtStackManager manager, String name) throws Exception {
        invokePrivate(manager, name, new Class[]{});
    }

    private static void invokePrivate(MtStackManager manager, String name, Class<?>[] types, Object... args)
            throws Exception {
        Method method = MtStackManager.class.getDeclaredMethod(name, types);
        method.setAccessible(true);
        try {
            method.invoke(manager, args);
        } catch (InvocationTargetException ex) {
            if (ex.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw ex;
        }
    }
}
