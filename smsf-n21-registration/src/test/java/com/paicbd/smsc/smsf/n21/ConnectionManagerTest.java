package com.paicbd.smsc.smsf.n21;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenCache;
import com.paicbd.smsc.smsf.common.utils.Http2GatewayCreator;
import com.paicbd.smsc.smsf.n21.config.AppProperties;
import com.paicbd.smsc.smsf.n21.stack.N21StackManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.RedisManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.ConsumerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.paicbd.smsc.smsf.common.constants.Constants.ENABLED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;

/**
 * Multi-tenant lifecycle: one {@link N21StackManager} per enabled gateway, driven by the same Redis
 * registry and the same STOMP notifications the other modules use.
 */
@ExtendWith(MockitoExtension.class)
class ConnectionManagerTest {

    private static final int NETWORK_ID = 10;

    @Mock
    private RedisManager redisManager;

    @Mock
    private AppProperties appProperties;

    @Mock
    private SmsfTokenCache tokenCache;

    @Mock
    private ConsumerFactory<String, String> consumerFactory;

    private final ConcurrentMap<Integer, N21StackManager> stacks = new ConcurrentHashMap<>();

    private ConnectionManager newConnectionManager() {
        return new ConnectionManager(redisManager, stacks, appProperties, tokenCache, consumerFactory);
    }

    private static String gatewayJson(int networkId, int enabled) {
        Http2Gateway gateway = Http2GatewayCreator.buildHttp2GatewayWithMtls(
                "smsf-gw", networkId, "smsf.local:8443", enabled);
        return Converter.valueAsString(gateway);
    }

    // --- startup --------------------------------------------------------------

    @Test
    @DisplayName("init, when Redis holds no gateway, then no stack is started")
    void initWithoutGateways() {
        when(redisManager.hgetAll(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME))
                .thenReturn(Collections.emptyMap());

        try (MockedConstruction<N21StackManager> mocked = mockConstruction(N21StackManager.class)) {
            newConnectionManager().init();

            assertTrue(mocked.constructed().isEmpty());
            assertTrue(stacks.isEmpty());
        }
    }

    @Test
    @DisplayName("init, when the gateways hash is null, then it is treated as empty")
    void initWithNullHash() {
        when(redisManager.hgetAll(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME)).thenReturn(null);

        var connectionManager = newConnectionManager();

        assertDoesNotThrow(connectionManager::init);
        assertTrue(stacks.isEmpty());
    }

    @Test
    @DisplayName("init, when an enabled gateway is present, then its stack is started and kept")
    void initStartsEnabledGateway() {
        when(redisManager.hgetAll(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME))
                .thenReturn(Map.of(String.valueOf(NETWORK_ID), gatewayJson(NETWORK_ID, ENABLED)));

        try (MockedConstruction<N21StackManager> mocked = mockConstruction(N21StackManager.class)) {
            newConnectionManager().init();

            assertEquals(1, mocked.constructed().size());
            verify(mocked.constructed().getFirst()).start();
            assertEquals(1, stacks.size());
        }
    }

    @Test
    @DisplayName("init, when the gateway is disabled, then no stack is started")
    void initSkipsDisabledGateway() {
        when(redisManager.hgetAll(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME))
                .thenReturn(Map.of(String.valueOf(NETWORK_ID), gatewayJson(NETWORK_ID, 0)));

        try (MockedConstruction<N21StackManager> mocked = mockConstruction(N21StackManager.class)) {
            newConnectionManager().init();

            assertTrue(mocked.constructed().isEmpty());
            assertTrue(stacks.isEmpty());
        }
    }

    @Test
    @DisplayName("init, when one gateway entry is malformed, then it is skipped without failing the rest")
    void initSkipsMalformedEntry() {
        when(redisManager.hgetAll(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME))
                .thenReturn(Map.of("1", "{not-json", String.valueOf(NETWORK_ID), gatewayJson(NETWORK_ID, ENABLED)));

        try (MockedConstruction<N21StackManager> mocked = mockConstruction(N21StackManager.class)) {
            var connectionManager = newConnectionManager();
            assertDoesNotThrow(connectionManager::init);

            assertEquals(1, mocked.constructed().size());
            assertEquals(1, stacks.size());
        }
    }

    @Test
    @DisplayName("init, when a stack fails to start, then the failure is contained and it is not kept")
    void initContainsStartFailure() {
        when(redisManager.hgetAll(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME))
                .thenReturn(Map.of(String.valueOf(NETWORK_ID), gatewayJson(NETWORK_ID, ENABLED)));

        try (MockedConstruction<N21StackManager> ignored = mockConstruction(N21StackManager.class,
                (mock, ctx) -> doThrow(new IllegalStateException("boom")).when(mock).start())) {
            var connectionManager = newConnectionManager();
            assertDoesNotThrow(connectionManager::init);

            assertTrue(stacks.isEmpty());
        }
    }

    @Test
    @DisplayName("init, when a lingering dead stack refuses to stop, then the gateway is skipped and the rest continue")
    void initContainsFailureReplacingDeadStack() {
        // The inner handler in startGatewayStack only guards start(). Replacing a stack that was
        // left behind stopped happens before it, so this is what the outer handler is there for.
        N21StackManager dead = mock(N21StackManager.class);
        when(dead.isStarted()).thenReturn(false);
        doThrow(new IllegalStateException("stop refused")).when(dead).stop();
        stacks.put(NETWORK_ID, dead);

        when(redisManager.hgetAll(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME))
                .thenReturn(Map.of(String.valueOf(NETWORK_ID), gatewayJson(NETWORK_ID, ENABLED)));

        var connectionManager = newConnectionManager();

        assertDoesNotThrow(connectionManager::init);

        // The dead stack is still there: it was never replaced, and nothing else blew up.
        assertEquals(dead, stacks.get(NETWORK_ID));
    }

    // --- runtime updates ------------------------------------------------------

    @Test
    @DisplayName("updateHttp2Gateway, when enabled, then the stack is started")
    void updateStartsEnabledGateway() {
        when(redisManager.hget(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME, String.valueOf(NETWORK_ID)))
                .thenReturn(gatewayJson(NETWORK_ID, ENABLED));

        try (MockedConstruction<N21StackManager> mocked = mockConstruction(N21StackManager.class)) {
            newConnectionManager().updateHttp2Gateway(String.valueOf(NETWORK_ID));

            verify(mocked.constructed().getFirst()).start();
            assertEquals(1, stacks.size());
        }
    }

    @Test
    @DisplayName("updateHttp2Gateway, when the gateway is already running, then it is not started twice")
    void updateSkipsRunningStack() {
        N21StackManager running = mock(N21StackManager.class);
        when(running.isStarted()).thenReturn(true);
        stacks.put(NETWORK_ID, running);
        when(redisManager.hget(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME, String.valueOf(NETWORK_ID)))
                .thenReturn(gatewayJson(NETWORK_ID, ENABLED));

        try (MockedConstruction<N21StackManager> mocked = mockConstruction(N21StackManager.class)) {
            newConnectionManager().updateHttp2Gateway(String.valueOf(NETWORK_ID));

            assertTrue(mocked.constructed().isEmpty());
            verify(running, never()).stop();
        }
    }

    @Test
    @DisplayName("updateHttp2Gateway, when a stopped stack exists, then it is replaced")
    void updateReplacesStoppedStack() {
        N21StackManager stopped = mock(N21StackManager.class);
        when(stopped.isStarted()).thenReturn(false);
        stacks.put(NETWORK_ID, stopped);
        when(redisManager.hget(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME, String.valueOf(NETWORK_ID)))
                .thenReturn(gatewayJson(NETWORK_ID, ENABLED));

        try (MockedConstruction<N21StackManager> mocked = mockConstruction(N21StackManager.class)) {
            newConnectionManager().updateHttp2Gateway(String.valueOf(NETWORK_ID));

            verify(stopped).stop();
            assertEquals(1, mocked.constructed().size());
            verify(mocked.constructed().getFirst()).start();
        }
    }

    @Test
    @DisplayName("updateHttp2Gateway, when disabled, then the running stack is stopped and dropped")
    void updateStopsDisabledGateway() {
        N21StackManager running = mock(N21StackManager.class);
        stacks.put(NETWORK_ID, running);
        when(redisManager.hget(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME, String.valueOf(NETWORK_ID)))
                .thenReturn(gatewayJson(NETWORK_ID, 0));

        newConnectionManager().updateHttp2Gateway(String.valueOf(NETWORK_ID));

        verify(running).stop();
        assertTrue(stacks.isEmpty());
    }

    @Test
    @DisplayName("updateHttp2Gateway, when the gateway vanished from Redis, then the stack is stopped")
    void updateStopsWhenGatewayIsGone() {
        N21StackManager running = mock(N21StackManager.class);
        stacks.put(NETWORK_ID, running);
        when(redisManager.hget(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME, String.valueOf(NETWORK_ID)))
                .thenReturn(null);

        newConnectionManager().updateHttp2Gateway(String.valueOf(NETWORK_ID));

        verify(running).stop();
        assertTrue(stacks.isEmpty());
    }

    @Test
    @DisplayName("updateHttp2Gateway, when the entry is malformed, then it is treated as absent")
    void updateTreatsMalformedEntryAsAbsent() {
        when(redisManager.hget(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME, String.valueOf(NETWORK_ID)))
                .thenReturn("{not-json");

        var connectionManager = newConnectionManager();

        var argument = String.valueOf(NETWORK_ID);

        assertDoesNotThrow(() -> connectionManager.updateHttp2Gateway(argument));
        assertTrue(stacks.isEmpty());
    }

    @Test
    @DisplayName("updateHttp2Gateway, when stopping throws, then the failure is contained")
    void updateContainsStopFailure() {
        N21StackManager running = mock(N21StackManager.class);
        doThrow(new IllegalStateException("boom")).when(running).stop();
        stacks.put(NETWORK_ID, running);
        when(redisManager.hget(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME, String.valueOf(NETWORK_ID)))
                .thenReturn(null);

        var connectionManager = newConnectionManager();

        var argument = String.valueOf(NETWORK_ID);

        assertDoesNotThrow(() -> connectionManager.updateHttp2Gateway(argument));
        assertTrue(stacks.isEmpty());
    }

    @Test
    @DisplayName("updateHttp2Gateway, with a non numeric payload, then it fails fast")
    void updateRejectsNonNumericPayload() {
        var connectionManager = newConnectionManager();

        assertThrows(NumberFormatException.class,
                () -> connectionManager.updateHttp2Gateway("notNumber"));
    }

    @Test
    @DisplayName("stopping a gateway that is not running is a no-op")
    void stoppingUnknownGatewayIsNoOp() {
        when(redisManager.hget(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME, String.valueOf(NETWORK_ID)))
                .thenReturn(gatewayJson(NETWORK_ID, 0));

        var connectionManager = newConnectionManager();

        var argument = String.valueOf(NETWORK_ID);

        assertDoesNotThrow(() -> connectionManager.updateHttp2Gateway(argument));
        assertTrue(stacks.isEmpty());
    }

    // --- shutdown -------------------------------------------------------------

    @Test
    @DisplayName("destroy stops every gateway and clears the registry")
    void destroyStopsEverything() {
        N21StackManager first = mock(N21StackManager.class);
        N21StackManager second = mock(N21StackManager.class);
        stacks.put(1, first);
        stacks.put(2, second);

        newConnectionManager().destroy();

        verify(first).stop();
        verify(second).stop();
        assertTrue(stacks.isEmpty());
    }

    @Test
    @DisplayName("destroy, when one gateway fails to stop, then the others are still stopped")
    void destroyContinuesAfterFailure() {
        N21StackManager failing = mock(N21StackManager.class);
        N21StackManager healthy = mock(N21StackManager.class);
        doThrow(new IllegalStateException("boom")).when(failing).stop();
        stacks.put(1, failing);
        stacks.put(2, healthy);

        var connectionManager = newConnectionManager();

        assertDoesNotThrow(connectionManager::destroy);

        verify(healthy).stop();
        assertTrue(stacks.isEmpty());
    }
}
