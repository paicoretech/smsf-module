package com.paicbd.smsc.smsf.core;

import com.paicbd.smsc.dto.ErrorCodeMapping;
import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.smsf.core.config.AppProperties;
import com.paicbd.smsc.smsf.core.security.GatewaySecurityLookup;
import com.paicbd.smsc.smsf.core.stack.CoreStackManager;
import com.paicbd.smsc.smsf.common.utils.Http2GatewayCreator;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenRedisStore;
import com.paicbd.smsc.utils.RedisManager;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.paicbd.smsc.smsf.common.constants.Constants.ENABLED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
class ConnectionManagerTest {

    @Mock
    RedisManager redisManager;

    @Mock
    AppProperties appProperties;

    @Mock
    SmsfTokenRedisStore tokenStore;

    @Mock
    GatewaySecurityLookup gatewaySecurityLookup;

    private ConnectionManager newConnectionManager(ConcurrentMap<String, List<ErrorCodeMapping>> errorMap,
                                                   ConcurrentMap<Integer, CoreStackManager> stacks) {
        return new ConnectionManager(redisManager, errorMap, stacks,
                appProperties, tokenStore, gatewaySecurityLookup);
    }

    @Test
    @DisplayName("Init, when error code mapping from redis is empty, then load but maps size is zero")
    void initWhenErrorCodeMappingFromRedisIsEmptyThenLoadButMapsSizeIsZero() {
        var spyErrorCodeMappingConcurrentHashMap = spy(new ConcurrentHashMap<String, List<ErrorCodeMapping>>());
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        ConnectionManager connectionManager = newConnectionManager(spyErrorCodeMappingConcurrentHashMap, gatewayStacks);

        when(redisManager.hgetAll(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME)).thenReturn(Collections.emptyMap());
        when(redisManager.hgetAll(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME)).thenReturn(Collections.emptyMap());

        connectionManager.init();

        assertEquals(0, spyErrorCodeMappingConcurrentHashMap.size());
        verify(gatewaySecurityLookup, never()).register(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("Init, when error code mapping from redis is not empty, then load maps and register gateways")
    void initWhenErrorCodeMappingNotEmptyThenLoadMaps() {
        var spyErrorCodeMappingConcurrentHashMap = spy(new ConcurrentHashMap<String, List<ErrorCodeMapping>>());
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        ConnectionManager connectionManager = newConnectionManager(spyErrorCodeMappingConcurrentHashMap, gatewayStacks);

        ErrorCodeMapping errorCodeMapping = new ErrorCodeMapping(121, 88, "UNDELIV");
        when(redisManager.hgetAll(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME)).thenReturn(
                Collections.singletonMap("1", Converter.valueAsString(List.of(errorCodeMapping))));
        when(redisManager.hgetAll(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME)).thenReturn(Collections.emptyMap());

        connectionManager.init();

        assertEquals(1, spyErrorCodeMappingConcurrentHashMap.size());

        ArgumentCaptor<String> mnoIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<List<ErrorCodeMapping>> errorCodeMappingCaptor = ArgumentCaptor.forClass(List.class);
        verify(spyErrorCodeMappingConcurrentHashMap).put(mnoIdCaptor.capture(), errorCodeMappingCaptor.capture());

        assertEquals("1", mnoIdCaptor.getValue());
        assertEquals(Converter.valueAsString(List.of(errorCodeMapping)),
                Converter.valueAsString(errorCodeMappingCaptor.getValue()));

        Http2Gateway http2Gateway = Http2GatewayCreator.buildHttp2Gateway("http2-gw", 1, "127.0.0.1", ENABLED);
        updateAndStartHttp2Gateway(connectionManager, http2Gateway, spyErrorCodeMappingConcurrentHashMap);
        stopHttp2Gateway(connectionManager, http2Gateway, spyErrorCodeMappingConcurrentHashMap);
        assertDoesNotThrow(connectionManager::destroy);
    }

    @Test
    @DisplayName("Init, when redis has enabled gateways, then registers and starts each")
    void initStartsGatewaysFromRedis() {
        var errorMap = new ConcurrentHashMap<String, List<ErrorCodeMapping>>();
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        ConnectionManager connectionManager = newConnectionManager(errorMap, gatewayStacks);

        Http2Gateway enabled = Http2GatewayCreator.buildHttp2Gateway("gw-1", 1, "127.0.0.1", ENABLED);
        Http2Gateway disabled = Http2GatewayCreator.buildHttp2Gateway("gw-2", 2, "127.0.0.2", 0);

        when(redisManager.hgetAll(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME)).thenReturn(Collections.emptyMap());
        when(redisManager.hgetAll(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME)).thenReturn(Map.of(
                "1", Converter.valueAsString(enabled),
                "2", Converter.valueAsString(disabled)));

        connectionManager.init();

        ArgumentCaptor<Http2Gateway> captor = ArgumentCaptor.forClass(Http2Gateway.class);
        verify(gatewaySecurityLookup, times(2)).register(captor.capture());
        assertEquals(2, captor.getAllValues().size());
        assertEquals(java.util.Set.of(1, 2),
                captor.getAllValues().stream().map(Http2Gateway::getNetworkId).collect(java.util.stream.Collectors.toSet()));
    }

    @Test
    @DisplayName("Init, when a lingering dead stack refuses to stop, then the gateway is skipped and the rest continue")
    void initContainsFailureReplacingDeadStack() {
        var errorMap = new ConcurrentHashMap<String, List<ErrorCodeMapping>>();
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        // The inner handler in startGatewayStack only guards start(). Replacing a stack that was
        // left behind stopped happens before it, so this is what the outer handler is there for.
        CoreStackManager dead = mock(CoreStackManager.class);
        when(dead.isStarted()).thenReturn(false);
        doThrow(new IllegalStateException("stop refused")).when(dead).stop();
        gatewayStacks.put(1, dead);
        ConnectionManager connectionManager = newConnectionManager(errorMap, gatewayStacks);

        Http2Gateway enabled = Http2GatewayCreator.buildHttp2Gateway("gw-1", 1, "127.0.0.1", ENABLED);
        when(redisManager.hgetAll(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME)).thenReturn(Collections.emptyMap());
        when(redisManager.hgetAll(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME))
                .thenReturn(Map.of("1", Converter.valueAsString(enabled)));

        assertDoesNotThrow(connectionManager::init);

        assertEquals(dead, gatewayStacks.get(1));
    }

    @Test
    @DisplayName("Init, when the stack for a gateway is already running, then it is left alone")
    void initSkipsAlreadyRunningStack() {
        var errorMap = new ConcurrentHashMap<String, List<ErrorCodeMapping>>();
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        CoreStackManager running = mock(CoreStackManager.class);
        when(running.isStarted()).thenReturn(true);
        gatewayStacks.put(1, running);
        ConnectionManager connectionManager = newConnectionManager(errorMap, gatewayStacks);

        Http2Gateway enabled = Http2GatewayCreator.buildHttp2Gateway("gw-1", 1, "127.0.0.1", ENABLED);
        when(redisManager.hgetAll(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME)).thenReturn(Collections.emptyMap());
        when(redisManager.hgetAll(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME))
                .thenReturn(Map.of("1", Converter.valueAsString(enabled)));

        connectionManager.init();

        verify(running, never()).stop();
        verify(running, never()).start();
        assertEquals(running, gatewayStacks.get(1));
    }

    @Test
    @DisplayName("destroy, when a stack fails to stop, then the remaining gateways are still stopped")
    void destroyContainsStopFailure() {
        var errorMap = new ConcurrentHashMap<String, List<ErrorCodeMapping>>();
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        CoreStackManager failing = mock(CoreStackManager.class);
        doThrow(new IllegalStateException("stop refused")).when(failing).stop();
        CoreStackManager healthy = mock(CoreStackManager.class);
        gatewayStacks.put(1, failing);
        gatewayStacks.put(2, healthy);
        ConnectionManager connectionManager = newConnectionManager(errorMap, gatewayStacks);

        assertDoesNotThrow(connectionManager::destroy);

        verify(healthy).stop();
        assertTrue(gatewayStacks.isEmpty());
    }

    @Test
    @DisplayName("updateHttp2Gateway, when gateway absent in Redis, then evict security and stop stack")
    void updateHttp2GatewayWhenAbsentEvicts() {
        var errorMap = new ConcurrentHashMap<String, List<ErrorCodeMapping>>();
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        ConnectionManager connectionManager = newConnectionManager(errorMap, gatewayStacks);

        when(redisManager.hget(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME, "1")).thenReturn(null);

        connectionManager.updateHttp2Gateway("1");

        verify(gatewaySecurityLookup).evict(1);
        verify(gatewaySecurityLookup, never()).register(any());
    }

    @Test
    @DisplayName("Init, when gateway map from Redis is null, then it exits without registering gateways")
    void initWhenGatewayMapFromRedisIsNullThenExits() {
        var errorMap = new ConcurrentHashMap<String, List<ErrorCodeMapping>>();
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        ConnectionManager connectionManager = newConnectionManager(errorMap, gatewayStacks);

        when(redisManager.hgetAll(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME)).thenReturn(Collections.emptyMap());
        when(redisManager.hgetAll(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME)).thenReturn(null);

        assertDoesNotThrow(connectionManager::init);
        verify(gatewaySecurityLookup, never()).register(any());
    }

    @Test
    @DisplayName("Init, when error code mapping payload is invalid, then it skips the broken entry")
    void initWhenErrorCodeMappingPayloadIsInvalidThenSkipsEntry() {
        var errorMap = spy(new ConcurrentHashMap<String, List<ErrorCodeMapping>>());
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        ConnectionManager connectionManager = newConnectionManager(errorMap, gatewayStacks);

        when(redisManager.hgetAll(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME))
                .thenReturn(Collections.singletonMap("1", "{invalid-json"));
        when(redisManager.hgetAll(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME)).thenReturn(Collections.emptyMap());

        assertDoesNotThrow(connectionManager::init);
        assertEquals(0, errorMap.size());
        verify(errorMap, never()).put(anyString(), anyList());
    }

    @Test
    @DisplayName("Init, when a gateway payload deserializes to null, then it skips that entry")
    void initWhenGatewayPayloadDeserializesToNullThenSkipsEntry() {
        var errorMap = new ConcurrentHashMap<String, List<ErrorCodeMapping>>();
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        ConnectionManager connectionManager = newConnectionManager(errorMap, gatewayStacks);

        when(redisManager.hgetAll(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME)).thenReturn(Collections.emptyMap());
        when(redisManager.hgetAll(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME))
                .thenReturn(Collections.singletonMap("1", "gateway-raw"));

        try (MockedStatic<Converter> converterMock = mockStatic(Converter.class)) {
            converterMock.when(() -> Converter.stringToObject("gateway-raw", Http2Gateway.class)).thenReturn(null);

            assertDoesNotThrow(connectionManager::init);
        }

        verify(gatewaySecurityLookup, never()).register(any());
    }

    @Test
    @DisplayName("Init, when a gateway payload is invalid, then it continues with the remaining entries")
    void initWhenGatewayPayloadIsInvalidThenContinues() {
        var errorMap = new ConcurrentHashMap<String, List<ErrorCodeMapping>>();
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        ConnectionManager connectionManager = newConnectionManager(errorMap, gatewayStacks);

        Http2Gateway enabled = Http2GatewayCreator.buildHttp2Gateway("gw-1", 1, "127.0.0.1", 0);
        when(redisManager.hgetAll(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME)).thenReturn(Collections.emptyMap());
        when(redisManager.hgetAll(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME)).thenReturn(Map.of(
                "1", Converter.valueAsString(enabled),
                "2", "{bad-json"));

        assertDoesNotThrow(connectionManager::init);

        ArgumentCaptor<Http2Gateway> gatewayCaptor = ArgumentCaptor.forClass(Http2Gateway.class);
        verify(gatewaySecurityLookup).register(gatewayCaptor.capture());
        assertEquals(1, gatewayCaptor.getValue().getNetworkId());
    }

    @Test
    @DisplayName("updateHttp2Gateway, when Redis payload is invalid, then evicts security and stops the stack")
    void updateHttp2GatewayWhenRedisPayloadIsInvalidThenEvictsAndStops() {
        var errorMap = new ConcurrentHashMap<String, List<ErrorCodeMapping>>();
        CoreStackManager existing = mock(CoreStackManager.class);
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        gatewayStacks.put(1, existing);
        ConnectionManager connectionManager = newConnectionManager(errorMap, gatewayStacks);

        when(redisManager.hget(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME, "1")).thenReturn("{bad-json");

        assertDoesNotThrow(() -> connectionManager.updateHttp2Gateway("1"));

        verify(gatewaySecurityLookup).evict(1);
        verify(existing).stop();
        assertEquals(0, gatewayStacks.size());
    }

    @Test
    @DisplayName("updateHttp2Gateway, when an existing stopped stack is present, then it is replaced")
    void updateHttp2GatewayWhenExistingStoppedStackIsPresentThenItIsReplaced() {
        var errorMap = new ConcurrentHashMap<String, List<ErrorCodeMapping>>();
        CoreStackManager existing = mock(CoreStackManager.class);
        when(existing.isStarted()).thenReturn(false);
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        gatewayStacks.put(1, existing);
        ConnectionManager connectionManager = newConnectionManager(errorMap, gatewayStacks);
        Http2Gateway gateway = Http2GatewayCreator.buildHttp2Gateway("http2-gw", 1, "127.0.0.1", ENABLED);

        when(redisManager.hget(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME, "1"))
                .thenReturn(Converter.valueAsString(gateway));

        try (MockedConstruction<CoreStackManager> ignored = mockConstruction(CoreStackManager.class)) {
            assertDoesNotThrow(() -> connectionManager.updateHttp2Gateway("1"));
            CoreStackManager created = ignored.constructed().getFirst();
            verify(existing).stop();
            verify(created).start();
            assertEquals(created, gatewayStacks.get(1));
        }
    }

    @Test
    @DisplayName("updateHttp2Gateway, when the new stack fails to start, then it is not stored")
    void updateHttp2GatewayWhenNewStackFailsToStartThenItIsNotStored() {
        var errorMap = new ConcurrentHashMap<String, List<ErrorCodeMapping>>();
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        ConnectionManager connectionManager = newConnectionManager(errorMap, gatewayStacks);
        Http2Gateway gateway = Http2GatewayCreator.buildHttp2Gateway("http2-gw", 1, "127.0.0.1", ENABLED);

        when(redisManager.hget(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME, "1"))
                .thenReturn(Converter.valueAsString(gateway));

        try (MockedConstruction<CoreStackManager> ignored = mockConstruction(
                CoreStackManager.class,
                (mock, context) -> doThrow(new IllegalStateException("boom")).when(mock).start())) {
            assertDoesNotThrow(() -> connectionManager.updateHttp2Gateway("1"));
        }

        assertEquals(0, gatewayStacks.size());
    }

    @Test
    @DisplayName("updateHttp2Gateway, when stopping a removed stack fails, then it still evicts and clears the map")
    void updateHttp2GatewayWhenStoppingRemovedStackFailsThenItStillEvictsAndClears() {
        var errorMap = new ConcurrentHashMap<String, List<ErrorCodeMapping>>();
        CoreStackManager existing = mock(CoreStackManager.class);
        doThrow(new IllegalStateException("stop-failed")).when(existing).stop();
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        gatewayStacks.put(1, existing);
        ConnectionManager connectionManager = newConnectionManager(errorMap, gatewayStacks);

        when(redisManager.hget(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME, "1")).thenReturn(null);

        assertDoesNotThrow(() -> connectionManager.updateHttp2Gateway("1"));

        verify(gatewaySecurityLookup).evict(1);
        verify(existing).stop();
        assertEquals(0, gatewayStacks.size());
    }

    private void updateAndStartHttp2Gateway(ConnectionManager connectionManager, Http2Gateway http2Gateway,
                                            ConcurrentHashMap<String, List<ErrorCodeMapping>> spyErrorCodeMappingConcurrentHashMap) {
        http2Gateway.setEnabled(ENABLED);
        when(redisManager.hget(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME, "1"))
                .thenReturn(Converter.valueAsString(http2Gateway));
        connectionManager.updateHttp2Gateway("1");
        assertEquals(1, spyErrorCodeMappingConcurrentHashMap.size());
    }

    private void stopHttp2Gateway(ConnectionManager connectionManager, Http2Gateway http2Gateway,
                                  ConcurrentHashMap<String, List<ErrorCodeMapping>> spyErrorCodeMappingConcurrentHashMap) {
        http2Gateway.setEnabled(0);
        when(redisManager.hget(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME, "1"))
                .thenReturn(Converter.valueAsString(http2Gateway));
        connectionManager.updateHttp2Gateway("1");
        assertEquals(1, spyErrorCodeMappingConcurrentHashMap.size());
    }

    @Test
    @DisplayName("Update error code mapping when error code mapping from redis is not empty")
    @SuppressWarnings("unchecked")
    void updateErrorCodeMappingTestFromRedisIsNotEmpty() {
        var spyErrorCodeMappingConcurrentHashMap = spy(new ConcurrentHashMap<String, List<ErrorCodeMapping>>());
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        ConnectionManager connectionManager = newConnectionManager(spyErrorCodeMappingConcurrentHashMap, gatewayStacks);

        ErrorCodeMapping errorCodeMapping = new ErrorCodeMapping(121, 88, "UNDELIV");
        when(redisManager.hget(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME, "1"))
                .thenReturn(Converter.valueAsString(List.of(errorCodeMapping)));

        connectionManager.updateErrorCodeMapping("1");

        assertEquals(1, spyErrorCodeMappingConcurrentHashMap.size());

        ArgumentCaptor<String> mnoIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<List<ErrorCodeMapping>> errorCodeMappingCaptor = ArgumentCaptor.forClass(List.class);
        verify(spyErrorCodeMappingConcurrentHashMap).put(mnoIdCaptor.capture(), errorCodeMappingCaptor.capture());

        assertEquals("1", mnoIdCaptor.getValue());
        assertEquals(Converter.valueAsString(List.of(errorCodeMapping)),
                Converter.valueAsString(errorCodeMappingCaptor.getValue()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("Update error code mapping when error code mapping from redis is empty and invalid JSON")
    void updateErrorCodeMappingTestFromRedisIsEmptyAndInvalidJSON(boolean empty) {
        var spyErrorCodeMappingConcurrentHashMap = spy(new ConcurrentHashMap<String, List<ErrorCodeMapping>>());
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        ConnectionManager connectionManager = newConnectionManager(spyErrorCodeMappingConcurrentHashMap, gatewayStacks);

        if (empty) {
            when(redisManager.hget(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME, "1")).thenReturn(null);
        } else {
            when(redisManager.hget(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME, "1")).thenReturn("{:'invalid json'}");
        }

        connectionManager.updateErrorCodeMapping("1");

        assertEquals(0, spyErrorCodeMappingConcurrentHashMap.size());
        verify(spyErrorCodeMappingConcurrentHashMap, never()).put(anyString(), anyList());
    }

    @Test
    @DisplayName("Destroy, when stopping a stack throws, then it still clears the stack registry")
    void destroyWhenStoppingStackThrowsThenItStillClearsRegistry() {
        var errorMap = new ConcurrentHashMap<String, List<ErrorCodeMapping>>();
        CoreStackManager stack = mock(CoreStackManager.class);
        doThrow(new IllegalStateException("stop-failed")).when(stack).stop();
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        gatewayStacks.put(1, stack);
        ConnectionManager connectionManager = newConnectionManager(errorMap, gatewayStacks);

        assertDoesNotThrow(connectionManager::destroy);

        verify(stack).stop();
        assertEquals(0, gatewayStacks.size());
    }
}
