package com.paicbd.smsc.smsf.core.security.nrf;

import com.paicbd.smsc.utils.Converter;
import com.paicore.sba.stack.openapi.model.NFProfile;
import com.paicore.sba.stack.openapi.model.NFStatus;
import com.paicore.sba.stack.openapi.model.NFType;
import com.paicore.sba.stack.transport.http2.NettyHttp2Client;
import com.paicore.sba.stack.transport.http2.dto.Http2Request;
import com.paicore.sba.stack.transport.http2.dto.Http2Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NrfRegistrationManagerTest {

    private NettyHttp2Client httpClient;
    private NrfRegistrationManager manager;

    @BeforeEach
    void setUp() {
        httpClient = mock(NettyHttp2Client.class);
        when(httpClient.isConnected()).thenReturn(true);
    }

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.stop();
        }
    }

    private static NFProfile profile() {
        NFProfile p = new NFProfile();
        p.setNfInstanceId(UUID.randomUUID());
        p.setNfType(NFType.SMSF);
        p.setNfStatus(NFStatus.REGISTERED);
        return p;
    }

    private static Http2Response response(int status, String body) {
        return Http2Response.builder()
                .status(status)
                .body(body == null ? null : body.getBytes(StandardCharsets.UTF_8))
                .build();
    }

    @Test
    void registerSends201AndAdoptsNegotiatedHeartbeat() throws Exception {
        String responseBody = "{\"nfInstanceId\":\"" + UUID.randomUUID()
                + "\",\"nfType\":\"SMSF\",\"nfStatus\":\"REGISTERED\",\"heartBeatTimer\":30}";
        when(httpClient.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(response(201, responseBody)));

        manager = new NrfRegistrationManager(11, httpClient, "/nnrf-nfm/v1/nf-instances/abc",
                profile(), () -> "Bearer token",
                new NrfRegistrationTimings(5, 1, 60));
        manager.register();

        verify(httpClient).send(argThat(req -> "PUT".equals(req.getMethod())));
    }

    @Test
    void constructorClampsMinimumValuesAndUsesNullBearerSupplier() throws Exception {
        when(httpClient.isConnected()).thenReturn(false);
        manager = new NrfRegistrationManager(10, httpClient, "/p", profile(),
                null,
                new NrfRegistrationTimings(0, 0, 0));

        Long initialRegisterTimeoutSeconds = readField(manager, "initialRegisterTimeoutSeconds");
        Long retryBackoffSeconds = readField(manager, "retryBackoffSeconds");
        assertEquals(1L, initialRegisterTimeoutSeconds.longValue());
        assertEquals(1L, retryBackoffSeconds.longValue());
        AtomicReference<Integer> heartbeatSecondsRef = readField(manager, "heartbeatSecondsRef");
        assertEquals(1, heartbeatSecondsRef.get());
        Supplier<String> bearerHeaderSupplier = readField(manager, "bearerHeaderSupplier");
        assertNull(bearerHeaderSupplier.get());
    }

    @Test
    void registerFailsOnNon2xxStatus() {
        when(httpClient.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(response(400, "{}")));

        manager = new NrfRegistrationManager(12, httpClient, "/path", profile(),
                () -> "Bearer t",
                new NrfRegistrationTimings(5, 1, 60));

        IllegalStateException ex = assertThrows(IllegalStateException.class, manager::register);
        assertTrue(ex.getMessage().contains("400"));
    }

    @Test
    void registerConnectsLazilyWhenClientDisconnected() throws Exception {
        when(httpClient.isConnected()).thenReturn(false);
        when(httpClient.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(response(201, "{}")));

        manager = new NrfRegistrationManager(13, httpClient, "/p", profile(),
                () -> null,
                new NrfRegistrationTimings(5, 1, 60));
        manager.register();

        verify(httpClient).connect();
    }

    @Test
    void registerSendsBearerAndJsonContentType() throws Exception {
        ArgumentCaptor<Http2Request> captor = ArgumentCaptor.forClass(Http2Request.class);
        when(httpClient.send(captor.capture()))
                .thenReturn(CompletableFuture.completedFuture(response(201, "{}")));

        manager = new NrfRegistrationManager(14, httpClient, "/path", profile(),
                () -> "Bearer xyz",
                new NrfRegistrationTimings(5, 1, 60));
        manager.register();

        Http2Request sent = captor.getValue();
        assertEquals("PUT", sent.getMethod());
        assertEquals("/path", sent.getPath());
        assertEquals("application/json", sent.getHeaders().get("content-type"));
        assertEquals("Bearer xyz", sent.getHeaders().get("authorization"));
    }

    @Test
    void registerAccepts200AndHandlesNullParsedProfile() throws Exception {
        String responseBody = "{\"heartBeatTimer\":99}";
        when(httpClient.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(response(200, responseBody)));

        try (MockedStatic<Converter> converterMock = mockStatic(Converter.class, org.mockito.Mockito.CALLS_REAL_METHODS)) {
            converterMock.when(() -> Converter.stringToObject(responseBody, NFProfile.class))
                    .thenReturn(null);

            manager = new NrfRegistrationManager(14, httpClient, "/path", profile(),
                    () -> "Bearer xyz",
                new NrfRegistrationTimings(5, 1, 60));
            manager.register();
        }

        verify(httpClient).send(argThat(req -> "PUT".equals(req.getMethod())));
    }

    @Test
    void schedulerEmitsHeartbeatAfterRegister() throws Exception {
        CountDownLatch heartbeat = new CountDownLatch(1);
        when(httpClient.send(any(Http2Request.class))).thenAnswer(inv -> {
            Http2Request req = inv.getArgument(0);
            if ("PATCH".equals(req.getMethod())) {
                heartbeat.countDown();
                return CompletableFuture.completedFuture(response(204, null));
            }
            return CompletableFuture.completedFuture(
                    response(201, "{\"heartBeatTimer\":1}"));
        });

        manager = new NrfRegistrationManager(15, httpClient, "/p", profile(),
                () -> "Bearer t",
                new NrfRegistrationTimings(5, 1, 60));
        manager.register();
        manager.start();

        assertTrue(heartbeat.await(5, TimeUnit.SECONDS), "heartbeat should fire");
    }

    @Test
    void startWhenAlreadyStartedKeepsSameScheduler() throws Exception {
        when(httpClient.isConnected()).thenReturn(false);
        manager = new NrfRegistrationManager(25, httpClient, "/p", profile(),
                () -> "Bearer t",
                new NrfRegistrationTimings(5, 1, 60));

        manager.start();
        ScheduledExecutorService firstScheduler = readField(manager, "scheduler");
        manager.start();
        ScheduledExecutorService secondScheduler = readField(manager, "scheduler");

        assertEquals(firstScheduler, secondScheduler);
    }

    @Test
    void heartbeatRetriesOnFailure() throws Exception {
        AtomicInteger patchCalls = new AtomicInteger();
        CountDownLatch retried = new CountDownLatch(2);
        when(httpClient.send(any(Http2Request.class))).thenAnswer(inv -> {
            Http2Request req = inv.getArgument(0);
            if ("PATCH".equals(req.getMethod())) {
                patchCalls.incrementAndGet();
                retried.countDown();
                return CompletableFuture.completedFuture(response(500, "boom"));
            }
            return CompletableFuture.completedFuture(
                    response(201, "{\"heartBeatTimer\":1}"));
        });

        manager = new NrfRegistrationManager(16, httpClient, "/p", profile(),
                () -> "Bearer t",
                new NrfRegistrationTimings(5, 1, 60));
        manager.register();
        manager.start();

        assertTrue(retried.await(6, TimeUnit.SECONDS),
                "heartbeat retry should happen after a failure");
        assertTrue(patchCalls.get() >= 2);
    }

    @Test
    void stopSendsDeleteAndIsIdempotent() throws Exception {
        AtomicInteger deletes = new AtomicInteger();
        when(httpClient.send(any(Http2Request.class))).thenAnswer(inv -> {
            Http2Request req = inv.getArgument(0);
            if ("DELETE".equals(req.getMethod())) {
                deletes.incrementAndGet();
                return CompletableFuture.completedFuture(response(204, null));
            }
            return CompletableFuture.completedFuture(
                    response(201, "{\"heartBeatTimer\":60}"));
        });

        manager = new NrfRegistrationManager(17, httpClient, "/p", profile(),
                () -> "Bearer t",
                new NrfRegistrationTimings(5, 1, 60));
        manager.register();
        manager.start();
        manager.stop();
        manager.stop();

        assertEquals(1, deletes.get(), "DELETE should be sent exactly once");
    }

    @Test
    void stopDoesNotSendDeleteWhenClientDisconnected() throws Exception {
        when(httpClient.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(response(201, "{}")));
        manager = new NrfRegistrationManager(18, httpClient, "/p", profile(),
                () -> "Bearer t",
                new NrfRegistrationTimings(5, 1, 60));
        manager.register();
        manager.start();

        when(httpClient.isConnected()).thenReturn(false);
        manager.stop();

        verify(httpClient, never()).send(argThat(req -> "DELETE".equals(req.getMethod())));
    }

    @ParameterizedTest
    @MethodSource("responsesWithoutNegotiatedHeartbeat")
    void registerKeepsDefaultHeartbeatWhenResponseCannotProvideHeartbeat(Http2Response response, int networkId) throws Exception {
        when(httpClient.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        manager = new NrfRegistrationManager(networkId, httpClient, "/p", profile(),
                () -> "Bearer t",
                new NrfRegistrationTimings(5, 1, 60));
        manager.register();

        verify(httpClient).send(argThat(req -> "PUT".equals(req.getMethod())));
    }

    @Test
    void deregisterSwallowsRuntimeExceptionFromHttpClient() throws Exception {
        when(httpClient.send(any(Http2Request.class))).thenAnswer(inv -> {
            Http2Request req = inv.getArgument(0);
            if ("DELETE".equals(req.getMethod())) {
                CompletableFuture<Http2Response> f = new CompletableFuture<>();
                f.completeExceptionally(new IllegalStateException("send failure"));
                return f;
            }
            return CompletableFuture.completedFuture(
                    response(201, "{\"heartBeatTimer\":60}"));
        });

        manager = new NrfRegistrationManager(21, httpClient, "/p", profile(),
                () -> "Bearer t",
                new NrfRegistrationTimings(5, 1, 60));
        manager.register();
        manager.start();
        manager.stop();

        verify(httpClient).send(argThat(req -> "DELETE".equals(req.getMethod())));
    }

    @Test
    void deregisterHandlesInterruptedException() throws Exception {
        when(httpClient.send(any(Http2Request.class))).thenAnswer(inv -> {
            Http2Request req = inv.getArgument(0);
            if ("DELETE".equals(req.getMethod())) {
                Thread.currentThread().interrupt();
                return new CompletableFuture<Http2Response>();
            }
            return CompletableFuture.completedFuture(
                    response(201, "{\"heartBeatTimer\":60}"));
        });

        manager = new NrfRegistrationManager(22, httpClient, "/p", profile(),
                () -> "Bearer t",
                new NrfRegistrationTimings(5, 1, 60));
        manager.register();
        manager.start();
        manager.stop();

        boolean wasInterrupted = Thread.interrupted();
        assertTrue(wasInterrupted, "interrupt status should be preserved on the caller thread");
    }

    @Test
    void heartbeatSwallowsRuntimeExceptionAndReschedulesBackoff() throws Exception {
        CountDownLatch firstFailureSeen = new CountDownLatch(1);
        CountDownLatch secondAttemptSeen = new CountDownLatch(2);
        when(httpClient.send(any(Http2Request.class))).thenAnswer(inv -> {
            Http2Request req = inv.getArgument(0);
            if ("PATCH".equals(req.getMethod())) {
                firstFailureSeen.countDown();
                secondAttemptSeen.countDown();
                CompletableFuture<Http2Response> f = new CompletableFuture<>();
                f.completeExceptionally(new IllegalStateException("network blip"));
                return f;
            }
            return CompletableFuture.completedFuture(
                    response(201, "{\"heartBeatTimer\":1}"));
        });

        manager = new NrfRegistrationManager(23, httpClient, "/p", profile(),
                () -> "Bearer t",
                new NrfRegistrationTimings(5, 1, 60));
        manager.register();
        manager.start();

        assertTrue(firstFailureSeen.await(5, TimeUnit.SECONDS), "first heartbeat should be attempted");
        assertTrue(secondAttemptSeen.await(6, TimeUnit.SECONDS),
                "second heartbeat should be re-scheduled after the failure");
    }

    @Test
    void heartbeatHandlesInterruptedException() throws Exception {
        CountDownLatch interruptedSeen = new CountDownLatch(1);
        when(httpClient.send(any(Http2Request.class))).thenAnswer(inv -> {
            Http2Request req = inv.getArgument(0);
            if ("PATCH".equals(req.getMethod())) {
                Thread.currentThread().interrupt();
                interruptedSeen.countDown();
                return new CompletableFuture<Http2Response>();
            }
            return CompletableFuture.completedFuture(
                    response(201, "{\"heartBeatTimer\":1}"));
        });

        manager = new NrfRegistrationManager(24, httpClient, "/p", profile(),
                () -> "Bearer t",
                new NrfRegistrationTimings(5, 1, 60));
        manager.register();
        manager.start();

        assertTrue(interruptedSeen.await(5, TimeUnit.SECONDS), "interrupted heartbeat should fire");
    }

    @SuppressWarnings("unchecked")
    private <T> T readField(Object target, String fieldName) throws Exception {
        Field field = NrfRegistrationManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(target);
    }

    private static Stream<org.junit.jupiter.params.provider.Arguments> responsesWithoutNegotiatedHeartbeat() {
        return Stream.of(
                org.junit.jupiter.params.provider.Arguments.of(response(201, null), 19),
                org.junit.jupiter.params.provider.Arguments.of(response(200, ""), 26),
                org.junit.jupiter.params.provider.Arguments.of(response(201, "{not-json"), 20)
        );
    }
}
