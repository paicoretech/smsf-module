package com.paicbd.smsc.smsf.n21.stack;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.Http2GatewayUDMSecurity;
import com.paicbd.smsc.smsf.common.http2.Http2ClientNrfConnection;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenCache;
import com.paicbd.smsc.smsf.common.utils.Http2GatewayCreator;
import com.paicbd.smsc.smsf.n21.config.AppProperties;
import com.paicbd.smsc.utils.Converter;
import com.paicore.sba.stack.api.n21.N21Client;
import com.paicore.sba.stack.api.n21.N21Configuration;
import com.paicore.sba.stack.api.n21.SmsfContext;
import com.paicore.sba.stack.api.n21.SmsfRegistrationResponse;
import com.paicore.sba.stack.api.security.AuthorizationProvider;
import com.paicbd.smsc.smsf.common.event.SmsfRegistrationEvent;
import com.paicbd.smsc.smsf.common.event.RegistrationAction;
import com.paicore.sba.stack.openapi.model.ProblemDetails;
import com.paicore.sba.stack.openapi.model.UeSmsContextData;
import com.paicore.sba.stack.transport.http2.NettyHttp2Client;
import com.paicore.sba.stack.transport.http2.n21.N21ClientFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.paicbd.smsc.smsf.common.constants.Constants.ENABLED;
import static com.paicbd.smsc.smsf.common.constants.Constants.NF_TYPE_UDM;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;

/**
 * N21 slice of the stack that used to be a single {@code Http2StackManager}: the UDM client and the
 * forwarding, now driven by a Kafka event instead of an in-process call, and gated on the token
 * that {@code smsf-core} publishes to Redis.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class N21StackManagerTest {

    private static final String TOPIC = "10.smsf.udm.register";
    private static final long MARGIN = 30L;

    @Mock
    private AppProperties appProperties;

    @Mock
    private ConsumerFactory<String, String> consumerFactory;

    @Mock
    private SmsfTokenCache tokenCache;

    private Http2Gateway gateway;

    @BeforeEach
    void setUp() {
        gateway = Http2GatewayCreator.buildHttp2GatewayWithMtls("smsf-gw", 10, "smsf.local:8443", ENABLED);
        when(appProperties.getOutboundTokenRefreshMarginSeconds()).thenReturn(MARGIN);
        when(appProperties.getTokenCheckIntervalSeconds()).thenReturn(10L);
        when(appProperties.getKafkaListenerConcurrency()).thenReturn(1);
        when(appProperties.getN21RestartMinIntervalSeconds()).thenReturn(30L);
    }

    // --- lifecycle ------------------------------------------------------------

    @Test
    @DisplayName("constructor, when called, then exposes the gateway and its NRF connection")
    void constructorExposesCollaborators() {
        N21StackManager manager = newManager();

        assertEquals(gateway, manager.getGateway());
        assertNotNull(manager.getNrfConnection());
        assertNull(manager.getN21Client());
        assertFalse(manager.isStarted());
    }

    @Test
    @DisplayName("start, when OAuth2 is not required, then the consumer starts straight away")
    void startStartsConsumerWhenNoOauth2() throws Exception {
        udm(false, false);
        N21StackManager manager = newManager();
        replaceField(manager, "nrfConnection", mock(Http2ClientNrfConnection.class));

        try (MockedConstruction<ConcurrentMessageListenerContainer> mocked = mockContainer()) {
            manager.start();

            assertTrue(manager.isStarted());
            assertEquals(1, mocked.constructed().size());
            verify(mocked.constructed().getFirst()).start();
            // No credential to wait for, so the cache is not involved at all.
            verify(tokenCache, never()).register(anyInt(), any(), any(), any());
            manager.stop();
        }
    }

    @Test
    @DisplayName("start, when already started, then it is a no-op")
    void startWhenAlreadyStartedThenNoOp() throws Exception {
        udm(false, false);
        N21StackManager manager = newManager();
        replaceField(manager, "nrfConnection", mock(Http2ClientNrfConnection.class));

        try (MockedConstruction<ConcurrentMessageListenerContainer> mocked = mockContainer()) {
            manager.start();
            manager.start();

            assertEquals(1, mocked.constructed().size());
            manager.stop();
        }
    }

    @Test
    @DisplayName("start, when the NRF connection throws, then the started flag is reset and it propagates")
    void startWhenNrfConnectFailsThenResetState() throws Exception {
        N21StackManager manager = newManager();
        Http2ClientNrfConnection nrfMock = mock(Http2ClientNrfConnection.class);
        doThrow(new RuntimeException("nrf-down")).when(nrfMock).connect();
        replaceField(manager, "nrfConnection", nrfMock);

        RuntimeException ex = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, manager::start);

        assertEquals("nrf-down", ex.getMessage());
        assertFalse(manager.isStarted());
        verify(nrfMock).disconnect();
    }

    @Test
    @DisplayName("stop, when not started, then it is a no-op and does not throw")
    void stopWhenNotStartedThenNoOp() {
        assertDoesNotThrow(newManager()::stop);
    }

    @Test
    @DisplayName("stop, when started, then the consumer, the client and the NRF connection are released")
    void stopReleasesResources() throws Exception {
        udm(false, false);
        N21StackManager manager = newManager();
        Http2ClientNrfConnection nrfMock = mock(Http2ClientNrfConnection.class);
        replaceField(manager, "nrfConnection", nrfMock);

        try (MockedConstruction<ConcurrentMessageListenerContainer> mocked = mockContainer()) {
            manager.start();
            manager.stop();

            assertFalse(manager.isStarted());
            verify(mocked.constructed().getFirst()).stop();
            verify(nrfMock).disconnect();
            assertNull(getField(manager, "registrationContainer"));
        }
    }

    // --- token gate -----------------------------------------------------------

    @Test
    @DisplayName("a gateway requiring OAuth2 does not start its consumer until the cache reports a token")
    void consumerDoesNotStartWithoutToken() throws Exception {
        udm(true, true);
        noToken();
        N21StackManager manager = newManager();
        replaceField(manager, "nrfConnection", mock(Http2ClientNrfConnection.class));

        try (MockedConstruction<ConcurrentMessageListenerContainer> mocked = mockContainer()) {
            manager.start();

            // Tracking is handed to the cache; nothing is consumed until it says the token is there.
            captureTokenCallbacks();
            assertTrue(mocked.constructed().isEmpty());
            assertNull(getField(manager, "registrationContainer"));
            manager.stop();
        }
    }

    @Test
    @DisplayName("the consumer starts as soon as the cache reports the token available")
    void consumerStartsWhenTokenAppears() throws Exception {
        udm(true, true);
        N21StackManager manager = newManager();
        replaceField(manager, "nrfConnection", mock(Http2ClientNrfConnection.class));

        try (MockedConstruction<ConcurrentMessageListenerContainer> mocked = mockContainer()) {
            manager.start();
            Runnable onAvailable = captureTokenCallbacks()[0];
            assertTrue(mocked.constructed().isEmpty());

            onAvailable.run();

            assertEquals(1, mocked.constructed().size());
            verify(mocked.constructed().getFirst()).start();
            assertNotNull(getField(manager, "registrationContainer"));
            manager.stop();
        }
    }

    @Test
    @DisplayName("the consumer is stopped again if the credential is lost, leaving the events in Kafka")
    void consumerStopsWhenTokenDisappears() throws Exception {
        udm(true, true);
        N21StackManager manager = newManager();
        replaceField(manager, "nrfConnection", mock(Http2ClientNrfConnection.class));

        try (MockedConstruction<ConcurrentMessageListenerContainer> mocked = mockContainer()) {
            manager.start();
            Runnable[] callbacks = captureTokenCallbacks();
            callbacks[0].run();
            assertEquals(1, mocked.constructed().size());

            callbacks[1].run();

            verify(mocked.constructed().getFirst()).stop();
            assertNull(getField(manager, "registrationContainer"));
            manager.stop();
        }
    }

    @Test
    @DisplayName("onTokenAvailable is idempotent: a second call does not open a second consumer")
    void onTokenAvailableIsIdempotent() throws Exception {
        udm(true, true);
        N21StackManager manager = newManager();
        replaceField(manager, "nrfConnection", mock(Http2ClientNrfConnection.class));

        try (MockedConstruction<ConcurrentMessageListenerContainer> mocked = mockContainer()) {
            manager.start();
            Runnable onAvailable = captureTokenCallbacks()[0];

            onAvailable.run();
            onAvailable.run();

            assertEquals(1, mocked.constructed().size());
            manager.stop();
        }
    }

    @Test
    @DisplayName("stop unregisters the gateway from the cache so no refresh cycle is left behind")
    void stopUnregistersFromTheCache() throws Exception {
        udm(true, true);
        N21StackManager manager = newManager();
        replaceField(manager, "nrfConnection", mock(Http2ClientNrfConnection.class));

        try (MockedConstruction<ConcurrentMessageListenerContainer> ignored = mockContainer()) {
            manager.start();
            manager.stop();

            verify(tokenCache).unregister(10, NF_TYPE_UDM);
        }
    }

    @Test
    @DisplayName("a gateway without OAuth2 is never registered with the cache")
    void noTrackingWhenOauth2Disabled() throws Exception {
        udm(true, false);
        N21StackManager manager = newManager();
        replaceField(manager, "nrfConnection", mock(Http2ClientNrfConnection.class));

        try (MockedConstruction<ConcurrentMessageListenerContainer> mocked = mockContainer()) {
            manager.start();

            verify(tokenCache, never()).register(anyInt(), any(), any(), any());
            assertEquals(1, mocked.constructed().size());
            manager.stop();
        }
    }

    @Test
    @DisplayName("forwardRegistration refuses to send when the gateway needs a token and Redis has none")
    void forwardRegistrationRefusesWithoutCredential() throws Exception {
        udm(true, true);
        noToken();
        N21StackManager manager = newManagerWithClient();
        N21Client client = manager.getN21Client();

        manager.forwardRegistration(ueContext("imsi-1"));

        verify(client, never()).registerSubscriber(any(), any());
    }

    @Test
    @DisplayName("forwardDeregistration refuses to send when the gateway needs a token and Redis has none")
    void forwardDeregistrationRefusesWithoutCredential() throws Exception {
        udm(true, true);
        noToken();
        N21StackManager manager = newManagerWithClient();
        N21Client client = manager.getN21Client();

        manager.forwardDeregistration("imsi-1");

        verify(client, never()).deregisterSubscriber(any(), any());
    }

    // --- kafka event ----------------------------------------------------------

    @Test
    @DisplayName("a REGISTER event forwards its UE context to the UDM")
    void registerEventIsForwarded() throws Exception {
        udm(true, false);
        N21StackManager manager = newManagerWithClient();
        N21Client client = manager.getN21Client();
        when(client.registerSubscriber(any(), any()))
                .thenReturn(SmsfRegistrationResponse.builder().statusCode(204).success(true).build());

        onEvent(manager, SmsfRegistrationEvent.builder()
                .networkId(10).supi("imsi-1").action(RegistrationAction.REGISTER)
                .ueContext(ueContext("imsi-1")).timestamp(Instant.now().toEpochMilli()).build());

        verify(client).registerSubscriber(any(), any());
        verify(client, never()).deregisterSubscriber(any(), any());
    }

    @Test
    @DisplayName("a DEREGISTER event deregisters the subscriber, no UE context needed")
    void deregisterEventIsForwarded() throws Exception {
        udm(true, false);
        N21StackManager manager = newManagerWithClient();
        N21Client client = manager.getN21Client();
        when(client.deregisterSubscriber(any(), any()))
                .thenReturn(SmsfRegistrationResponse.builder().statusCode(204).success(true).build());

        onEvent(manager, SmsfRegistrationEvent.builder()
                .networkId(10).supi("imsi-1").action(RegistrationAction.DEREGISTER)
                .timestamp(Instant.now().toEpochMilli()).build());

        verify(client).deregisterSubscriber(any(), any());
        verify(client, never()).registerSubscriber(any(), any());
    }

    @Test
    @DisplayName("a malformed payload is discarded instead of killing the listener")
    void malformedEventIsDiscarded() throws Exception {
        udm(true, false);
        N21StackManager manager = newManagerWithClient();

        assertDoesNotThrow(() -> onRawEvent(manager, "{not-json"));

        verify(manager.getN21Client(), never()).registerSubscriber(any(), any());
    }

    @Test
    @DisplayName("an event without an action or a SUPI is discarded")
    void incompleteEventIsDiscarded() throws Exception {
        udm(true, false);
        N21StackManager manager = newManagerWithClient();

        onEvent(manager, SmsfRegistrationEvent.builder().networkId(10).supi("imsi-1").build());
        onEvent(manager, SmsfRegistrationEvent.builder()
                .networkId(10).action(RegistrationAction.REGISTER).build());

        verify(manager.getN21Client(), never()).registerSubscriber(any(), any());
        verify(manager.getN21Client(), never()).deregisterSubscriber(any(), any());
    }

    @Test
    @DisplayName("a REGISTER event without a UE context is discarded rather than sent empty to the UDM")
    void registerEventWithoutContextIsDiscarded() throws Exception {
        udm(true, false);
        N21StackManager manager = newManagerWithClient();

        onEvent(manager, SmsfRegistrationEvent.builder()
                .networkId(10).supi("imsi-1").action(RegistrationAction.REGISTER).build());

        verify(manager.getN21Client(), never()).registerSubscriber(any(), any());
    }

    // --- N21 client -----------------------------------------------------------

    @Test
    @DisplayName("startN21Client builds the UDM config and context, and takes the credential from Redis")
    void startN21ClientBuildsConfigAndUsesRedisCredential() throws Exception {
        Http2GatewayUDMSecurity udm = udm(true, true);
        udm.setNrfDiscoveryEnabled(true);
        udm.setApiRoot("http://udm.local");
        udm.setRegistrationPath("/nudm-uecm/v1/imsi-{supi}/registrations/smsf-3gpp-access");
        gateway.setNfInstanceId(UUID.fromString("11111111-2222-3333-4444-555555555555"));

        N21StackManager manager = newManager();
        replaceField(manager, "nrfConnection", nrfConnectionMock());

        N21Client n21Mock = mock(N21Client.class);
        AtomicReference<N21Configuration> config = new AtomicReference<>();
        AtomicReference<SmsfContext> context = new AtomicReference<>();
        AtomicReference<AuthorizationProvider> provider = new AtomicReference<>();

        try (MockedStatic<N21ClientFactory> factory = mockStatic(N21ClientFactory.class)) {
            factory.when(() -> N21ClientFactory.createWithProvider(any(), any(), any(), any())).thenAnswer(inv -> {
                config.set(inv.getArgument(0));
                context.set(inv.getArgument(1));
                provider.set(inv.getArgument(3));
                return n21Mock;
            });

            invokePrivate(manager, "startN21Client");

            verify(n21Mock).connect();
            assertSame(n21Mock, manager.getN21Client());
            assertEquals("UDM", config.get().getTargetNfType());
            assertEquals("nudm-uecm", config.get().getTargetServiceName());
            assertEquals("SMSF", config.get().getRequesterNfType());
            assertEquals(gateway.getNfInstanceId().toString(), config.get().getRequesterNfInstanceId());

            // The UDM must know this SMSF can be selected for SBI-based MT delivery.
            assertEquals(Boolean.TRUE, context.get().getSmsfSbiSupInd());
            assertEquals(gateway.getNfInstanceId().toString(), context.get().getSmsfInstanceId());

            // The credential is resolved from Redis at request time, not captured at build time.
            noToken();
            assertNull(provider.get().authorizationHeader());
            when(tokenCache.authorizationHeader(10, NF_TYPE_UDM)).thenReturn(Optional.of("Bearer udm"));
            assertEquals("Bearer udm", provider.get().authorizationHeader());
        }
    }

    @Test
    @DisplayName("startN21Client, when forwarding is disabled, then no client is created")
    void startN21ClientSkippedWhenForwardingDisabled() throws Exception {
        udm(false, false);
        N21StackManager manager = newManager();

        invokePrivate(manager, "startN21Client");

        assertNull(manager.getN21Client());
    }

    @Test
    @DisplayName("startN21Client, when the factory throws, then the failure is swallowed and no client is set")
    void startN21ClientWhenFactoryThrowsThenSwallows() throws Exception {
        udm(true, false);
        N21StackManager manager = newManager();
        replaceField(manager, "nrfConnection", nrfConnectionMock());

        try (MockedStatic<N21ClientFactory> factory = mockStatic(N21ClientFactory.class)) {
            factory.when(() -> N21ClientFactory.createWithProvider(any(), any(), any(), any()))
                    .thenThrow(new RuntimeException("factory boom"));

            assertDoesNotThrow(() -> invokePrivate(manager, "startN21Client"));
            assertNull(manager.getN21Client());
        }
    }

    @Test
    @DisplayName("forwardRegistration, when the client failed to start, then it is retried after the cool-down")
    void forwardRegistrationRetriesAfterCoolDown() throws Exception {
        Http2GatewayUDMSecurity udm = udm(true, false);
        udm.setApiRoot("https://udm.local:8443");

        N21StackManager manager = newManager();
        setStarted(manager);
        replaceField(manager, "nrfConnection", nrfConnectionMock());

        N21Client n21Mock = mock(N21Client.class);
        when(n21Mock.registerSubscriber(any(), any()))
                .thenReturn(SmsfRegistrationResponse.builder().statusCode(204).success(true).build());

        try (MockedStatic<N21ClientFactory> factory = mockStatic(N21ClientFactory.class)) {
            factory.when(() -> N21ClientFactory.createWithProvider(any(), any(), any(), any()))
                    .thenReturn(n21Mock);

            // A recent failed attempt keeps the client unavailable, so nothing is forwarded.
            replaceField(manager, "lastN21StartAttemptMs", System.currentTimeMillis());
            manager.forwardRegistration(ueContext("imsi-1"));
            verify(n21Mock, never()).registerSubscriber(any(), any());

            // Once the cool-down has elapsed the startup is retried and the registration goes out.
            replaceField(manager, "lastN21StartAttemptMs", System.currentTimeMillis() - 31_000L);
            manager.forwardRegistration(ueContext("imsi-1"));
            verify(n21Mock).registerSubscriber(any(), any());
        }
    }

    @Test
    @DisplayName("forwardRegistration, when there is no client, then it is a no-op")
    void forwardRegistrationWhenNoClientThenNoOp() {
        var manager = newManager();
        var argument = ueContext("imsi-1");
        assertDoesNotThrow(() -> manager.forwardRegistration(argument));
    }

    @Test
    @DisplayName("forwardRegistration, when the UDM accepts, then the call is made")
    void forwardRegistrationAccepted() throws Exception {
        udm(true, false);
        N21StackManager manager = newManagerWithClient();
        SmsfRegistrationResponse res = mock(SmsfRegistrationResponse.class);
        when(res.isSuccess()).thenReturn(true);
        when(manager.getN21Client().registerSubscriber(any(), any())).thenReturn(res);

        var ueContext = ueContext("imsi-ok");

        assertDoesNotThrow(() -> manager.forwardRegistration(ueContext));
        verify(manager.getN21Client()).registerSubscriber(any(), any());
    }

    @Test
    @DisplayName("forwardRegistration, when the UDM rejects with problem details, then it is handled")
    void forwardRegistrationRejectedWithProblem() throws Exception {
        udm(true, false);
        N21StackManager manager = newManagerWithClient();
        ProblemDetails problem = mock(ProblemDetails.class);
        when(problem.getDetail()).thenReturn("subscriber not allowed");
        SmsfRegistrationResponse res = mock(SmsfRegistrationResponse.class);
        when(res.isSuccess()).thenReturn(false);
        when(res.getProblem()).thenReturn(problem);
        when(manager.getN21Client().registerSubscriber(any(), any())).thenReturn(res);

        var ueContext = ueContext("imsi-rej");

        assertDoesNotThrow(() -> manager.forwardRegistration(ueContext));
    }

    @Test
    @DisplayName("forwardRegistration, when the UDM rejects without problem details, then the raw body is used")
    void forwardRegistrationRejectedWithoutProblem() throws Exception {
        udm(true, false);
        N21StackManager manager = newManagerWithClient();
        SmsfRegistrationResponse res = mock(SmsfRegistrationResponse.class);
        when(res.isSuccess()).thenReturn(false);
        when(res.getProblem()).thenReturn(null);
        when(res.getRawBody()).thenReturn("raw error");
        when(manager.getN21Client().registerSubscriber(any(), any())).thenReturn(res);

        var ueContext = ueContext("imsi-raw");

        assertDoesNotThrow(() -> manager.forwardRegistration(ueContext));
    }

    @Test
    @DisplayName("forwardRegistration, when the N21 call throws, then the exception is swallowed")
    void forwardRegistrationSwallowsExceptions() throws Exception {
        udm(true, false);
        N21StackManager manager = newManagerWithClient();
        when(manager.getN21Client().registerSubscriber(any(), any())).thenThrow(new RuntimeException("n21 down"));

        var ueContext = ueContext("imsi-err");

        assertDoesNotThrow(() -> manager.forwardRegistration(ueContext));
    }

    @Test
    @DisplayName("forwardDeregistration, when there is no client, then it is a no-op")
    void forwardDeregistrationWhenNoClientThenNoOp() {
        var manager = newManager();
        assertDoesNotThrow(() -> manager.forwardDeregistration("imsi-1"));
    }

    @Test
    @DisplayName("forwardDeregistration, when the UDM accepts, then the call is made")
    void forwardDeregistrationAccepted() throws Exception {
        udm(true, false);
        N21StackManager manager = newManagerWithClient();
        SmsfRegistrationResponse res = mock(SmsfRegistrationResponse.class);
        when(res.isSuccess()).thenReturn(true);
        when(manager.getN21Client().deregisterSubscriber(any(), any())).thenReturn(res);

        assertDoesNotThrow(() -> manager.forwardDeregistration("imsi-ok"));
        verify(manager.getN21Client()).deregisterSubscriber(any(), any());
    }

    @Test
    @DisplayName("forwardDeregistration, when the UDM rejects, then it is handled")
    void forwardDeregistrationRejected() throws Exception {
        udm(true, false);
        N21StackManager manager = newManagerWithClient();
        SmsfRegistrationResponse res = mock(SmsfRegistrationResponse.class);
        when(res.isSuccess()).thenReturn(false);
        when(manager.getN21Client().deregisterSubscriber(any(), any())).thenReturn(res);

        assertDoesNotThrow(() -> manager.forwardDeregistration("imsi-rej"));
    }

    @Test
    @DisplayName("forwardDeregistration, when the N21 call throws, then the exception is swallowed")
    void forwardDeregistrationSwallowsExceptions() throws Exception {
        udm(true, false);
        N21StackManager manager = newManagerWithClient();
        when(manager.getN21Client().deregisterSubscriber(any(), any()))
                .thenThrow(new RuntimeException("n21 down"));

        assertDoesNotThrow(() -> manager.forwardDeregistration("imsi-err"));
    }

    @Test
    @DisplayName("closeN21Client closes and clears the client")
    void closeN21ClientClosesAndClears() throws Exception {
        udm(true, false);
        N21StackManager manager = newManagerWithClient();
        N21Client client = manager.getN21Client();

        invokePrivate(manager, "closeN21Client");

        verify(client).close();
        assertNull(manager.getN21Client());
    }

    @Test
    @DisplayName("closeN21Client, when close throws, then the reference is cleared anyway")
    void closeN21ClientClearsEvenWhenCloseThrows() throws Exception {
        udm(true, false);
        N21StackManager manager = newManagerWithClient();
        doThrow(new RuntimeException("close boom")).when(manager.getN21Client()).close();

        assertDoesNotThrow(() -> invokePrivate(manager, "closeN21Client"));
        assertNull(manager.getN21Client());
    }

    // --- helpers --------------------------------------------------------------

    private N21StackManager newManager() {
        return new N21StackManager(gateway, appProperties, tokenCache, consumerFactory);
    }

    private N21StackManager newManagerWithClient() throws Exception {
        N21StackManager manager = newManager();
        setStarted(manager);
        replaceField(manager, "n21Client", mock(N21Client.class));
        return manager;
    }

    /** Configures the gateway's UDM block; the shared fixture does not carry one. */
    private Http2GatewayUDMSecurity udm(boolean forwarding, boolean oauth2) {
        Http2GatewayUDMSecurity udm = new Http2GatewayUDMSecurity();
        udm.setForwardRegistrationEnabled(forwarding);
        udm.setOauth2Enabled(oauth2);
        udm.setRegistrationPath("/nudm-uecm/v1/{supi}/registrations/smsf-3gpp-access");
        udm.setDeregistrationPath("/nudm-uecm/v1/{supi}/registrations/smsf-3gpp-access");
        gateway.setUdmSecurity(udm);
        return udm;
    }

    private void noToken() {
        when(tokenCache.isAvailable(anyInt(), any())).thenReturn(false);
        when(tokenCache.authorizationHeader(anyInt(), any())).thenReturn(Optional.empty());
    }

    /** Captures the callbacks the stack hands to the cache, which is what drives the consumer. */
    private Runnable[] captureTokenCallbacks() {
        ArgumentCaptor<Runnable> onAvailable = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<Runnable> onUnavailable = ArgumentCaptor.forClass(Runnable.class);
        verify(tokenCache).register(eq(10), eq(NF_TYPE_UDM), onAvailable.capture(), onUnavailable.capture());
        return new Runnable[]{onAvailable.getValue(), onUnavailable.getValue()};
    }

    private Http2ClientNrfConnection nrfConnectionMock() {
        Http2ClientNrfConnection nrfMock = mock(Http2ClientNrfConnection.class);
        when(nrfMock.getClient()).thenReturn(mock(NettyHttp2Client.class));
        return nrfMock;
    }

    @SuppressWarnings("rawtypes")
    private MockedConstruction<ConcurrentMessageListenerContainer> mockContainer() {
        return mockConstruction(ConcurrentMessageListenerContainer.class);
    }

    private static UeSmsContextData ueContext(String supi) {
        UeSmsContextData ue = new UeSmsContextData();
        ue.setSupi(supi);
        return ue;
    }

    private static void onEvent(N21StackManager manager, SmsfRegistrationEvent event) throws Exception {
        onRawEvent(manager, Converter.valueAsString(event));
    }

    private static void onRawEvent(N21StackManager manager, String payload) throws Exception {
        Method method = N21StackManager.class.getDeclaredMethod("onRegistrationEvent", ConsumerRecord.class);
        method.setAccessible(true);
        try {
            method.invoke(manager, new ConsumerRecord<>(TOPIC, 0, 0L, "imsi-1", payload));
        } catch (InvocationTargetException ex) {
            if (ex.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw ex;
        }
    }

    private static void replaceField(N21StackManager manager, String name, Object value) throws Exception {
        Field field = N21StackManager.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(manager, value);
    }

    private static Object getField(N21StackManager manager, String name) throws Exception {
        Field field = N21StackManager.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(manager);
    }

    private static void setStarted(N21StackManager manager) throws Exception {
        ((AtomicBoolean) getField(manager, "started")).set(true);
    }

    private static void invokePrivate(N21StackManager manager, String name) throws Exception {
        Method method = N21StackManager.class.getDeclaredMethod(name);
        method.setAccessible(true);
        try {
            method.invoke(manager);
        } catch (InvocationTargetException ex) {
            if (ex.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw ex;
        }
    }
}
