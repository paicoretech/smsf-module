package com.paicbd.smsc.smsf.core.stack;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.Http2GatewayOutboundSecurity;
import com.paicbd.smsc.dto.Http2GatewayUDMSecurity;
import com.paicbd.smsc.smsf.common.http2.Http2ClientNrfConnection;
import com.paicbd.smsc.smsf.common.redis.SmsfSecurityToken;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenRedisStore;
import com.paicbd.smsc.smsf.common.utils.Http2GatewayCreator;
import com.paicbd.smsc.smsf.core.config.AppProperties;
import com.paicbd.smsc.smsf.core.http2.Http2ClientTokenConnection;
import com.paicbd.smsc.smsf.core.security.nrf.NrfRegistrationManager;
import com.paicbd.smsc.smsf.core.security.outbound.OAuth2ClientCredentialsFetcher;
import com.paicbd.smsc.smsf.core.security.outbound.OAuth2Token;
import com.paicbd.smsc.smsf.core.security.outbound.OutboundTokenRefresher;
import com.paicore.sba.stack.transport.http2.NettyHttp2Client;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.paicbd.smsc.smsf.common.constants.Constants.ENABLED;
import static com.paicbd.smsc.smsf.common.constants.Constants.NF_TYPE_AMF;
import static com.paicbd.smsc.smsf.common.constants.Constants.NF_TYPE_UDM;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;

/**
 * Core slice of the stack that used to be a single {@code Http2StackManager}: the NRF integration
 * and the OAuth2 tokens. The core mints both audiences — the AMF one it does not use itself and the
 * UDM one — because it owns the NRF credentials, and republishes each to Redis.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CoreStackManagerTest {

    /** Index of the token listener in the OutboundTokenRefresher constructor. */
    private static final int TOKEN_LISTENER_ARG = 5;

    @Mock
    private AppProperties appProperties;

    @Mock
    private SmsfTokenRedisStore tokenStore;

    private Http2Gateway gateway;

    @BeforeEach
    void setUp() {
        gateway = Http2GatewayCreator.buildHttp2GatewayWithMtls("smsf-gw", 10, "smsf.local:8443", ENABLED);
        when(appProperties.getN21Oauth2Scope()).thenReturn("nudm-uecm");
        // NfProfileBuilder runs as soon as the NRF connection reports connected.
        mockNfProfileProperties();
        // NRF registration is exercised by its own tests; without a register path start() skips it,
        // which keeps the token tests focused on the refreshers.
        gateway.getNrfSecurity().setNrfRegisterPath(null);
    }

    /** Re-arms the register path the setUp cleared, for the tests that do exercise registration. */
    private void withNrfRegisterPath() {
        gateway.getNrfSecurity().setNrfRegisterPath("/nnrf-nfm/v1/nf-instances");
    }

    // --- construction and lifecycle -------------------------------------------

    @Test
    @DisplayName("constructor, when called, then exposes the NRF and token connections and the message factory")
    void constructorExposesCollaborators() {
        CoreStackManager manager = newManager();

        assertEquals(gateway, manager.getGateway());
        assertNotNull(manager.getNrfConnection());
        assertNotNull(manager.getTokenConnection());
        // The MO flow resolves the factory per gateway to decode and map incoming messages.
        assertNotNull(manager.getMessageFactory());
        assertNull(manager.getOutboundTokenRefresher());
        assertNull(manager.getUdmTokenRefresher());
        assertFalse(manager.isStarted());
    }

    @Test
    @DisplayName("start, when called the first time, then the NRF connection is opened")
    void startConnectsNrf() throws Exception {
        CoreStackManager manager = newManager();
        Http2ClientNrfConnection nrfMock = mock(Http2ClientNrfConnection.class);
        replaceField(manager, "nrfConnection", nrfMock);

        manager.start();

        assertTrue(manager.isStarted());
        verify(nrfMock).connect();
        manager.stop();
    }

    @Test
    @DisplayName("start, when already started, then it is a no-op")
    void startWhenAlreadyStartedThenNoOp() throws Exception {
        CoreStackManager manager = newManager();
        Http2ClientNrfConnection nrfMock = mock(Http2ClientNrfConnection.class);
        replaceField(manager, "nrfConnection", nrfMock);

        manager.start();
        manager.start();

        verify(nrfMock, times(1)).connect();
        manager.stop();
    }

    @Test
    @DisplayName("start, when the NRF connection throws, then the started flag is reset and it propagates")
    void startWhenNrfConnectFailsThenResetState() throws Exception {
        CoreStackManager manager = newManager();
        Http2ClientNrfConnection nrfMock = mock(Http2ClientNrfConnection.class);
        doThrow(new RuntimeException("connect-failed")).when(nrfMock).connect();
        replaceField(manager, "nrfConnection", nrfMock);

        RuntimeException ex = assertThrows(RuntimeException.class, manager::start);

        assertEquals("connect-failed", ex.getMessage());
        assertFalse(manager.isStarted());
        verify(nrfMock).disconnect();
    }

    @Test
    @DisplayName("stop, when not started, then it is a no-op and does not throw")
    void stopWhenNotStartedThenNoOp() {
        CoreStackManager manager = newManager();

        assertDoesNotThrow(manager::stop);
        assertFalse(manager.isStarted());
    }

    @Test
    @DisplayName("stop, when started, then both connections are closed")
    void stopReleasesResources() throws Exception {
        CoreStackManager manager = newManager();
        Http2ClientNrfConnection nrfMock = mock(Http2ClientNrfConnection.class);
        Http2ClientTokenConnection tokenMock = mock(Http2ClientTokenConnection.class);
        replaceField(manager, "nrfConnection", nrfMock);
        replaceField(manager, "tokenConnection", tokenMock);

        manager.start();
        manager.stop();

        assertFalse(manager.isStarted());
        verify(nrfMock).disconnect();
        verify(tokenMock).disconnect();
    }

    @Test
    @DisplayName("isStarted, on a fresh manager, then false")
    void isStartedInitialStateThenFalse() {
        assertFalse(newManager().isStarted());
    }

    // --- AMF-audience token ---------------------------------------------------

    @Test
    @DisplayName("start, when the NRF token_path is missing, then the refresher is skipped")
    void amfRefresherSkippedWhenTokenPathMissing() throws Exception {
        enableNrfOauth2();
        gateway.getNrfSecurity().setTokenPath(null);
        CoreStackManager manager = startedManagerWithNrf(true);

        assertNull(manager.getOutboundTokenRefresher());
        manager.stop();
    }

    @Test
    @DisplayName("start, when the NRF client is not connected, then the refresher is skipped")
    void amfRefresherSkippedWhenNrfNotConnected() throws Exception {
        enableNrfOauth2();
        CoreStackManager manager = startedManagerWithNrf(false);

        assertNull(manager.getOutboundTokenRefresher());
        manager.stop();
    }

    @Test
    @DisplayName("start, when OAuth2 is disabled, then no refresher is created at all")
    void amfRefresherSkippedWhenOauth2Disabled() throws Exception {
        CoreStackManager manager = newManager();
        replaceField(manager, "nrfConnection", mock(Http2ClientNrfConnection.class));

        try (MockedConstruction<OutboundTokenRefresher> ctor =
                     mockConstruction(OutboundTokenRefresher.class)) {
            manager.start();

            assertTrue(ctor.constructed().isEmpty());
            manager.stop();
        }
    }

    @Test
    @DisplayName("start, when the bootstrap succeeds, then the AMF refresher is started")
    void amfRefresherStartedOnSuccessfulBootstrap() throws Exception {
        enableNrfOauth2();
        CoreStackManager manager = newManager();
        replaceField(manager, "nrfConnection", connectedNrf());

        try (MockedConstruction<OutboundTokenRefresher> ctor =
                     mockConstruction(OutboundTokenRefresher.class)) {
            manager.start();

            assertEquals(1, ctor.constructed().size());
            OutboundTokenRefresher built = ctor.constructed().getFirst();
            verify(built).bootstrap();
            verify(built).start();
            assertSame(built, manager.getOutboundTokenRefresher());

            manager.stop();
            verify(built).stop();
            assertNull(manager.getOutboundTokenRefresher());
        }
    }

    @Test
    @DisplayName("start, when the AMF bootstrap fails, then the refresher is stopped and start propagates")
    void amfBootstrapFailureAbortsStart() throws Exception {
        enableNrfOauth2();
        CoreStackManager manager = newManager();
        replaceField(manager, "nrfConnection", connectedNrf());

        try (MockedConstruction<OutboundTokenRefresher> ctor =
                     mockConstruction(OutboundTokenRefresher.class,
                             (mock, ctx) -> doThrow(new RuntimeException("nrf-down")).when(mock).bootstrap())) {
            IllegalStateException ex = assertThrows(IllegalStateException.class, manager::start);

            assertTrue(ex.getMessage().contains("Token bootstrap failed"));
            assertFalse(manager.isStarted());
            verify(ctor.constructed().getFirst()).stop();
            assertNull(manager.getOutboundTokenRefresher());
        }
    }

    @Test
    @DisplayName("start, when the AMF bootstrap is interrupted, then it is stopped and start propagates")
    void amfBootstrapInterruptedAbortsStart() throws Exception {
        enableNrfOauth2();
        CoreStackManager manager = newManager();
        replaceField(manager, "nrfConnection", connectedNrf());

        try (MockedConstruction<OutboundTokenRefresher> ctor =
                     mockConstruction(OutboundTokenRefresher.class,
                             (mock, ctx) -> doThrow(new InterruptedException("interrupted")).when(mock).bootstrap())) {
            assertThrows(IllegalStateException.class, manager::start);

            verify(ctor.constructed().getFirst()).stop();
            assertFalse(manager.isStarted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    @DisplayName("start, with an external token endpoint that connects, then the refresher is started")
    void amfRefresherStartedFromExternalEndpoint() throws Exception {
        enableExternalOauth2("https://token.local:8443/oauth2/token");
        CoreStackManager manager = newManager();
        replaceField(manager, "nrfConnection", mock(Http2ClientNrfConnection.class));
        Http2ClientTokenConnection tokenMock = mock(Http2ClientTokenConnection.class);
        when(tokenMock.isConnected()).thenReturn(true);
        when(tokenMock.getClient()).thenReturn(mock(NettyHttp2Client.class));
        replaceField(manager, "tokenConnection", tokenMock);

        try (MockedConstruction<OutboundTokenRefresher> ctor =
                     mockConstruction(OutboundTokenRefresher.class)) {
            manager.start();

            verify(tokenMock).connect();
            assertEquals(1, ctor.constructed().size());
            assertNotNull(manager.getOutboundTokenRefresher());
            manager.stop();
        }
    }

    @Test
    @DisplayName("start, with an external token endpoint that does not connect, then the refresher is skipped")
    void amfRefresherSkippedWhenExternalEndpointNotConnected() throws Exception {
        enableExternalOauth2("https://token.local:8443/oauth2/token");
        CoreStackManager manager = newManager();
        replaceField(manager, "nrfConnection", mock(Http2ClientNrfConnection.class));
        Http2ClientTokenConnection tokenMock = mock(Http2ClientTokenConnection.class);
        when(tokenMock.isConnected()).thenReturn(false);
        replaceField(manager, "tokenConnection", tokenMock);

        manager.start();

        assertNull(manager.getOutboundTokenRefresher());
        manager.stop();
    }

    @Test
    @DisplayName("start, with a blank external token endpoint, then the refresher is skipped")
    void amfRefresherSkippedWhenExternalEndpointBlank() throws Exception {
        enableExternalOauth2("   ");
        CoreStackManager manager = startedManagerWithNrf(true);

        assertNull(manager.getOutboundTokenRefresher());
        manager.stop();
    }

    @Test
    @DisplayName("start, when neither NRF nor an external endpoint is enabled, then the refresher is skipped")
    void amfRefresherSkippedWhenNoSource() throws Exception {
        Http2GatewayOutboundSecurity outbound = gateway.getOutboundSecurity();
        outbound.setOauth2Enabled(true);
        outbound.setUsingNrf(false);
        outbound.setUsingExternalEndpoint(false);
        CoreStackManager manager = startedManagerWithNrf(true);

        assertNull(manager.getOutboundTokenRefresher());
        manager.stop();
    }

    @Test
    @DisplayName("stop, when the refresher throws, then it is cleared and the Redis entry is evicted")
    void stopClearsRefresherEvenWhenStopThrows() throws Exception {
        CoreStackManager manager = newManager();
        OutboundTokenRefresher refresher = mock(OutboundTokenRefresher.class);
        doThrow(new RuntimeException("stop boom")).when(refresher).stop();
        replaceField(manager, "outboundTokenRefresher", refresher);
        replaceField(manager, "nrfConnection", mock(Http2ClientNrfConnection.class));
        setStarted(manager);

        assertDoesNotThrow(manager::stop);

        assertNull(manager.getOutboundTokenRefresher());
        verify(tokenStore).evict(gateway.getNetworkId(), NF_TYPE_AMF);
    }

    // --- UDM-audience token (minted here, consumed by smsf-n21-registration) ---

    @Test
    @DisplayName("the UDM refresher is started when the gateway forwards registrations with OAuth2")
    void udmRefresherStartedWhenForwardingWithOauth2() throws Exception {
        enableNrfOauth2();
        udmSecurity(true, true);
        CoreStackManager manager = newManager();
        replaceField(manager, "nrfConnection", connectedNrf());

        try (MockedConstruction<OutboundTokenRefresher> ctor =
                     mockConstruction(OutboundTokenRefresher.class)) {
            manager.start();

            // One for the AMF audience and one for the UDM audience.
            assertEquals(2, ctor.constructed().size());
            assertNotNull(manager.getUdmTokenRefresher());
            manager.stop();
        }
    }

    @Test
    @DisplayName("no UDM refresher when the gateway does not forward registrations, so the NRF is not asked in vain")
    void udmRefresherSkippedWhenForwardingDisabled() throws Exception {
        enableNrfOauth2();
        udmSecurity(false, true);
        CoreStackManager manager = newManager();
        replaceField(manager, "nrfConnection", connectedNrf());

        try (MockedConstruction<OutboundTokenRefresher> ctor =
                     mockConstruction(OutboundTokenRefresher.class)) {
            manager.start();

            assertEquals(1, ctor.constructed().size());
            assertNull(manager.getUdmTokenRefresher());
            manager.stop();
        }
    }

    @Test
    @DisplayName("no UDM refresher when the UDM does not require OAuth2")
    void udmRefresherSkippedWhenOauth2Disabled() throws Exception {
        enableNrfOauth2();
        udmSecurity(true, false);
        CoreStackManager manager = newManager();
        replaceField(manager, "nrfConnection", connectedNrf());

        try (MockedConstruction<OutboundTokenRefresher> ctor =
                     mockConstruction(OutboundTokenRefresher.class)) {
            manager.start();

            assertEquals(1, ctor.constructed().size());
            assertNull(manager.getUdmTokenRefresher());
            manager.stop();
        }
    }

    @Test
    @DisplayName("a UDM token that cannot be minted does not stop the core: the SBI keeps serving the AMF")
    void udmRefresherFailureIsNotFatal() throws Exception {
        enableNrfOauth2();
        udmSecurity(true, true);
        CoreStackManager manager = newManager();
        replaceField(manager, "nrfConnection", connectedNrf());

        // The AMF refresher is built first and must succeed; only the UDM one fails.
        java.util.concurrent.atomic.AtomicInteger built = new java.util.concurrent.atomic.AtomicInteger();
        try (MockedConstruction<OutboundTokenRefresher> ctor =
                     mockConstruction(OutboundTokenRefresher.class, (mock, ctx) -> {
                         if (built.incrementAndGet() == 2) {
                             doThrow(new RuntimeException("udm token down")).when(mock).bootstrap();
                         }
                     })) {
            assertDoesNotThrow(manager::start);

            assertTrue(manager.isStarted());
            assertEquals(2, ctor.constructed().size());
            assertNotNull(manager.getOutboundTokenRefresher());
            assertNull(manager.getUdmTokenRefresher());
            manager.stop();
        }
    }

    @Test
    @DisplayName("no UDM refresher when outbound_security carries no credentials, and the core still starts")
    void udmRefresherSkippedWithoutCredentials() throws Exception {
        enableNrfOauth2();
        udmSecurity(true, true);
        gateway.setOutboundSecurity(null);
        CoreStackManager manager = newManager();
        replaceField(manager, "nrfConnection", connectedNrf());

        assertDoesNotThrow(manager::start);

        assertTrue(manager.isStarted());
        assertNull(manager.getUdmTokenRefresher());
        manager.stop();
    }

    @Test
    @DisplayName("no UDM refresher when there is no usable token source, and the core still starts")
    void udmRefresherSkippedWithoutTokenSource() throws Exception {
        Http2GatewayOutboundSecurity outbound = gateway.getOutboundSecurity();
        outbound.setOauth2Enabled(false);
        outbound.setUsingNrf(false);
        outbound.setUsingExternalEndpoint(false);
        udmSecurity(true, true);
        CoreStackManager manager = newManager();
        replaceField(manager, "nrfConnection", connectedNrf());

        assertDoesNotThrow(manager::start);

        assertTrue(manager.isStarted());
        assertNull(manager.getUdmTokenRefresher());
        manager.stop();
    }

    // --- Redis publication ----------------------------------------------------

    @Test
    @DisplayName("every token the AMF refresher obtains is republished to Redis for smsf-service-mt")
    void amfTokenIsPublishedToRedis() throws Exception {
        enableNrfOauth2();
        CoreStackManager manager = newManager();
        replaceField(manager, "nrfConnection", connectedNrf());

        AtomicReference<Consumer<OAuth2Token>> listener = new AtomicReference<>();
        try (MockedConstruction<OutboundTokenRefresher> ignored =
                     mockConstruction(OutboundTokenRefresher.class,
                             (mock, ctx) -> listener.set(tokenListener(ctx.arguments())))) {
            manager.start();

            Instant issuedAt = Instant.now();
            listener.get().accept(new OAuth2Token("amf-token", 300, issuedAt));

            ArgumentCaptor<SmsfSecurityToken> captor = ArgumentCaptor.forClass(SmsfSecurityToken.class);
            verify(tokenStore).publish(captor.capture());
            SmsfSecurityToken published = captor.getValue();
            assertEquals(gateway.getNetworkId(), published.networkId());
            assertEquals(NF_TYPE_AMF, published.targetNfType());
            assertEquals("amf-token", published.accessToken());
            assertEquals("Bearer", published.tokenType());
            assertEquals(issuedAt.plusSeconds(300).getEpochSecond(), published.expiresAtEpochSecond());
            manager.stop();
        }
    }

    @Test
    @DisplayName("the UDM token is published under its own audience, so it never overwrites the AMF one")
    void udmTokenIsPublishedUnderItsOwnAudience() throws Exception {
        enableNrfOauth2();
        udmSecurity(true, true);
        CoreStackManager manager = newManager();
        replaceField(manager, "nrfConnection", connectedNrf());

        AtomicReference<Consumer<OAuth2Token>> udmListener = new AtomicReference<>();
        try (MockedConstruction<OutboundTokenRefresher> ctor =
                     mockConstruction(OutboundTokenRefresher.class,
                             (mock, ctx) -> udmListener.set(tokenListener(ctx.arguments())))) {
            manager.start();
            assertEquals(2, ctor.constructed().size());

            // The last construction is the UDM one.
            udmListener.get().accept(new OAuth2Token("udm-token", 120, Instant.now()));

            ArgumentCaptor<SmsfSecurityToken> captor = ArgumentCaptor.forClass(SmsfSecurityToken.class);
            verify(tokenStore).publish(captor.capture());
            assertEquals(NF_TYPE_UDM, captor.getValue().targetNfType());
            assertEquals("nudm-uecm", captor.getValue().scope());
            manager.stop();
        }
    }

    @Test
    @DisplayName("a null token is not published, so a stale entry is never replaced by nothing")
    void nullTokenIsNotPublished() throws Exception {
        enableNrfOauth2();
        CoreStackManager manager = newManager();
        replaceField(manager, "nrfConnection", connectedNrf());

        AtomicReference<Consumer<OAuth2Token>> listener = new AtomicReference<>();
        try (MockedConstruction<OutboundTokenRefresher> ignored =
                     mockConstruction(OutboundTokenRefresher.class,
                             (mock, ctx) -> listener.set(tokenListener(ctx.arguments())))) {
            manager.start();

            listener.get().accept(null);

            verify(tokenStore, never()).publish(any());
            manager.stop();
        }
    }

    @Test
    @DisplayName("stopping a gateway evicts its Redis entries: a stopped gateway leaves no live credential")
    void stopEvictsBothAudiences() throws Exception {
        enableNrfOauth2();
        udmSecurity(true, true);
        CoreStackManager manager = newManager();
        replaceField(manager, "nrfConnection", connectedNrf());

        try (MockedConstruction<OutboundTokenRefresher> ignored =
                     mockConstruction(OutboundTokenRefresher.class)) {
            manager.start();
            manager.stop();

            verify(tokenStore).evict(gateway.getNetworkId(), NF_TYPE_AMF);
            verify(tokenStore).evict(gateway.getNetworkId(), NF_TYPE_UDM);
        }
    }

    // --- NRF registration -----------------------------------------------------

    @Test
    @DisplayName("startNrfRegistration, when the NRF security block is missing, then it is skipped")
    void nrfRegistrationSkippedWhenSecurityMissing() throws Exception {
        gateway.setNrfSecurity(null);
        CoreStackManager manager = newManager();

        invokePrivate(manager, "startNrfRegistration");

        assertNull(manager.getNrfRegistrationManager());
    }

    @Test
    @DisplayName("startNrfRegistration, when the NRF client is unavailable, then it is skipped")
    void nrfRegistrationSkippedWhenClientUnavailable() throws Exception {
        withNrfRegisterPath();
        CoreStackManager manager = newManager();
        Http2ClientNrfConnection nrfMock = mock(Http2ClientNrfConnection.class);
        when(nrfMock.isConnected()).thenReturn(false);
        replaceField(manager, "nrfConnection", nrfMock);

        invokePrivate(manager, "startNrfRegistration");

        assertNull(manager.getNrfRegistrationManager());
    }

    @Test
    @DisplayName("startNrfRegistration, when ready, then it registers and wires the bearer supplier")
    @SuppressWarnings("unchecked")
    void nrfRegistrationRegistersAndWiresBearer() throws Exception {
        withNrfRegisterPath();
        CoreStackManager manager = newManager();
        mockNfProfileProperties();
        replaceField(manager, "nrfConnection", connectedNrf());

        AtomicReference<Supplier<String>> bearerSupplier = new AtomicReference<>();
        try (MockedConstruction<NrfRegistrationManager> mocked =
                     mockConstruction(NrfRegistrationManager.class,
                             (mock, ctx) -> bearerSupplier.set((Supplier<String>) ctx.arguments().get(4)))) {
            invokePrivate(manager, "startNrfRegistration");

            NrfRegistrationManager built = mocked.constructed().getFirst();
            verify(built).register();
            verify(built).start();
            assertSame(built, manager.getNrfRegistrationManager());

            // The NRF registration authenticates with the AMF-audience token the core holds.
            assertNull(bearerSupplier.get().get());
            OutboundTokenRefresher refresher = mock(OutboundTokenRefresher.class);
            when(refresher.currentBearerHeader()).thenReturn(Optional.of("Bearer wired"));
            replaceField(manager, "outboundTokenRefresher", refresher);
            assertEquals("Bearer wired", bearerSupplier.get().get());
        }
    }

    @Test
    @DisplayName("startNrfRegistration, when registering fails, then the manager is stopped and it propagates")
    void nrfRegistrationFailureStopsTheManager() throws Exception {
        withNrfRegisterPath();
        CoreStackManager manager = newManager();
        mockNfProfileProperties();
        replaceField(manager, "nrfConnection", connectedNrf());

        try (MockedConstruction<NrfRegistrationManager> mocked =
                     mockConstruction(NrfRegistrationManager.class,
                             (mock, ctx) -> doThrow(new RuntimeException("nrf rejected")).when(mock).register())) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> invokePrivate(manager, "startNrfRegistration"));

            assertTrue(ex.getMessage().contains("NRF registration failed"));
            verify(mocked.constructed().getFirst()).stop();
            assertNull(manager.getNrfRegistrationManager());
        }
    }

    @Test
    @DisplayName("stopNrfRegistration stops and clears the manager")
    void stopNrfRegistrationStopsAndClears() throws Exception {
        CoreStackManager manager = newManager();
        NrfRegistrationManager nrfManager = mock(NrfRegistrationManager.class);
        replaceField(manager, "nrfRegistrationManager", nrfManager);

        invokePrivate(manager, "stopNrfRegistration");

        verify(nrfManager).stop();
        assertNull(manager.getNrfRegistrationManager());
    }

    @Test
    @DisplayName("stopNrfRegistration, when stopping throws, then the reference is cleared anyway")
    void stopNrfRegistrationClearsEvenWhenStopThrows() throws Exception {
        CoreStackManager manager = newManager();
        NrfRegistrationManager nrfManager = mock(NrfRegistrationManager.class);
        doThrow(new RuntimeException("stop boom")).when(nrfManager).stop();
        replaceField(manager, "nrfRegistrationManager", nrfManager);

        assertDoesNotThrow(() -> invokePrivate(manager, "stopNrfRegistration"));
        assertNull(manager.getNrfRegistrationManager());
    }

    // --- token fetcher --------------------------------------------------------

    @Test
    @DisplayName("the NRF fetcher is built with the SMSF as requester and the given audience")
    void nrfFetcherCarriesTheAudience() throws Exception {
        enableNrfOauth2();
        CoreStackManager manager = newManager();
        replaceField(manager, "nrfConnection", connectedNrf());

        Object fetcher = invokePrivate(manager, "buildTokenFetcher",
                new Class[]{Http2GatewayOutboundSecurity.class, String.class, String.class},
                gateway.getOutboundSecurity(), NF_TYPE_UDM, "nudm-uecm");

        assertNotNull(fetcher);
        assertTrue(fetcher instanceof OAuth2ClientCredentialsFetcher);
    }

    // --- helpers --------------------------------------------------------------

    private CoreStackManager newManager() {
        return new CoreStackManager(gateway, appProperties, tokenStore, new ConcurrentHashMap<>());
    }

    private CoreStackManager startedManagerWithNrf(boolean connected) throws Exception {
        CoreStackManager manager = newManager();
        Http2ClientNrfConnection nrfMock = mock(Http2ClientNrfConnection.class);
        when(nrfMock.isConnected()).thenReturn(connected);
        when(nrfMock.getClient()).thenReturn(mock(NettyHttp2Client.class));
        replaceField(manager, "nrfConnection", nrfMock);
        manager.start();
        return manager;
    }

    private Http2ClientNrfConnection connectedNrf() {
        Http2ClientNrfConnection nrfMock = mock(Http2ClientNrfConnection.class);
        when(nrfMock.isConnected()).thenReturn(true);
        when(nrfMock.getClient()).thenReturn(mock(NettyHttp2Client.class));
        return nrfMock;
    }

    private void enableNrfOauth2() {
        Http2GatewayOutboundSecurity outbound = gateway.getOutboundSecurity();
        outbound.setOauth2Enabled(true);
        outbound.setUsingNrf(true);
        outbound.setClientId("smsf-id");
        outbound.setClientSecret("secret");
        outbound.setScope("nsmsf-sms");
        gateway.getNrfSecurity().setTokenPath("/oauth2/token");
    }

    private void enableExternalOauth2(String tokenEndpoint) {
        Http2GatewayOutboundSecurity outbound = gateway.getOutboundSecurity();
        outbound.setOauth2Enabled(true);
        outbound.setUsingNrf(false);
        outbound.setUsingExternalEndpoint(true);
        outbound.setTokenEndpoint(tokenEndpoint);
        outbound.setClientId("smsf-id");
        outbound.setClientSecret("secret");
        outbound.setScope("nsmsf-sms");
    }

    private void udmSecurity(boolean forwarding, boolean oauth2) {
        Http2GatewayUDMSecurity udm = new Http2GatewayUDMSecurity();
        udm.setForwardRegistrationEnabled(forwarding);
        udm.setOauth2Enabled(oauth2);
        gateway.setUdmSecurity(udm);
    }

    private void mockNfProfileProperties() {
        when(appProperties.getNrfHeartbeatDefaultSeconds()).thenReturn(60);
        when(appProperties.getSmsfSbiPublicAddress()).thenReturn("127.0.0.1");
        when(appProperties.getSmsfSbiServerBasePath()).thenReturn("/nsmsf-sms/v2");
        when(appProperties.getNrfApiFullVersion()).thenReturn("2.0.0");
        when(appProperties.getServerPort()).thenReturn(9070);
    }

    @SuppressWarnings("unchecked")
    private static Consumer<OAuth2Token> tokenListener(java.util.List<?> constructorArguments) {
        return (Consumer<OAuth2Token>) constructorArguments.get(TOKEN_LISTENER_ARG);
    }

    private static void replaceField(CoreStackManager manager, String name, Object value) throws Exception {
        Field field = CoreStackManager.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(manager, value);
    }

    private static void setStarted(CoreStackManager manager) throws Exception {
        Field field = CoreStackManager.class.getDeclaredField("started");
        field.setAccessible(true);
        ((java.util.concurrent.atomic.AtomicBoolean) field.get(manager)).set(true);
    }

    private static Object invokePrivate(CoreStackManager manager, String name) throws Exception {
        return invokePrivate(manager, name, new Class[]{});
    }

    private static Object invokePrivate(CoreStackManager manager, String name, Class<?>[] types, Object... args)
            throws Exception {
        Method method = CoreStackManager.class.getDeclaredMethod(name, types);
        method.setAccessible(true);
        try {
            return method.invoke(manager, args);
        } catch (InvocationTargetException ex) {
            if (ex.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw ex;
        }
    }

    @Test
    @DisplayName("the MO watcher is per gateway and is released when the gateway stops")
    void moWatcherIsPerGatewayAndReleasedOnStop() {
        CoreStackManager manager = new CoreStackManager(gateway, appProperties, tokenStore,
                new ConcurrentHashMap<>());

        // Counted per tenant, not globally.
        manager.incrementMoProcessed();
        manager.incrementMoProcessed();
        assertEquals(2, manager.getProcessMoPerSecond());

        // The watcher starts with the manager, so stopping a gateway that never started must still
        // release it — otherwise every replaced gateway leaks a watcher thread.
        assertDoesNotThrow(manager::stop);
    }

    // --- NRF instance identity -------------------------------------------------

    @Test
    @DisplayName("the register path is the service path plus the nfInstanceId, never the configured id")
    void registerPathAppendsTheInstanceId() {
        UUID id = UUID.fromString("11111111-2222-3333-4444-555555555555");

        assertEquals("/nnrf-nfm/v1/nf-instances/" + id,
                CoreStackManager.registerPathFor("/nnrf-nfm/v1/nf-instances", id));
    }

    @Test
    @DisplayName("a configured path with trailing slashes still yields a single separator")
    void registerPathNormalisesTrailingSlashes() {
        UUID id = UUID.fromString("11111111-2222-3333-4444-555555555555");

        assertEquals("/nnrf-nfm/v1/nf-instances/" + id,
                CoreStackManager.registerPathFor("/nnrf-nfm/v1/nf-instances/", id));
        assertEquals("/nnrf-nfm/v1/nf-instances/" + id,
                CoreStackManager.registerPathFor("  /nnrf-nfm/v1/nf-instances///  ", id));
    }

    @Test
    @DisplayName("a gateway with no nf_instance_id does not start at all")
    void gatewayWithoutInstanceIdDoesNotStart() {
        // The identity comes from smsc-management-be through Redis. Without it the SMSF can neither
        // register on the NRF nor request a token, so starting would only produce a broken gateway.
        gateway.setNfInstanceId(null);
        CoreStackManager manager = new CoreStackManager(gateway, appProperties, tokenStore,
                new ConcurrentHashMap<>());

        assertThatThrownBy(manager::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("has no nf_instance_id");

        assertFalse(manager.isStarted());
    }
}
