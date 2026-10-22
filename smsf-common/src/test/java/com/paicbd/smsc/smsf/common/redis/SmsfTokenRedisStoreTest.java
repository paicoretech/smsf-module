package com.paicbd.smsc.smsf.common.redis;

import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.RedisManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SmsfTokenRedisStoreTest {

    private static final int NETWORK_ID = 7;
    private static final String AMF = "AMF";
    private static final String KEY = "smsf:token:7:AMF";
    private static final String ACCESS_TOKEN = "an-opaque-access-token";

    @Mock
    private RedisManager redisManager;

    private SmsfTokenRedisStore store;

    @BeforeEach
    void setUp() {
        store = new SmsfTokenRedisStore(redisManager);
    }

    private static SmsfSecurityToken token(long secondsToExpiry) {
        long now = Instant.now().getEpochSecond();
        return new SmsfSecurityToken(NETWORK_ID, AMF, ACCESS_TOKEN, "Bearer", "nsmsf-sms",
                now, now + secondsToExpiry);
    }

    @Test
    @DisplayName("the key pairs the gateway with the audience, so the AMF and UDM tokens never collide")
    void keyIsPerGatewayAndAudience() {
        assertThat(SmsfTokenRedisStore.keyFor(NETWORK_ID, AMF)).isEqualTo(KEY);
        assertThat(SmsfTokenRedisStore.keyFor(NETWORK_ID, "UDM")).isEqualTo("smsf:token:7:UDM");
        assertThat(SmsfTokenRedisStore.keyFor(8, AMF)).isEqualTo("smsf:token:8:AMF");
    }

    @Test
    @DisplayName("publish writes the serialized token with a TTL matching its remaining lifetime")
    void publishWritesWithTtl() {
        assertThat(store.publish(token(300))).isTrue();

        ArgumentCaptor<Long> ttl = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(redisManager).setex(eq(KEY), ttl.capture(), value.capture());
        assertThat(ttl.getValue()).isBetween(295L, 300L);
        assertThat(Converter.stringToObject(value.getValue(), SmsfSecurityToken.class).accessToken())
                .isEqualTo(ACCESS_TOKEN);
    }

    @Test
    @DisplayName("an already expired token still lands with a floor TTL rather than being rejected by Redis")
    void publishFloorsTheTtl() {
        store.publish(token(-30));

        verify(redisManager).setex(eq(KEY), eq(1L), anyString());
    }

    @Test
    @DisplayName("publish refuses a null or empty token instead of writing a useless entry")
    void publishRefusesEmptyTokens() {
        assertThat(store.publish(null)).isFalse();
        assertThat(store.publish(new SmsfSecurityToken(NETWORK_ID, AMF, null, "Bearer", "s", 0, 1))).isFalse();
        assertThat(store.publish(new SmsfSecurityToken(NETWORK_ID, AMF, "  ", "Bearer", "s", 0, 1))).isFalse();

        verifyNoInteractions(redisManager);
    }

    @Test
    @DisplayName("a Redis failure on publish is reported as false, not propagated")
    void publishSwallowsRedisFailures() {
        doThrow(new IllegalStateException("redis down"))
                .when(redisManager).setex(eq(KEY), anyLong(), anyString());

        assertThat(store.publish(token(300))).isFalse();
    }

    @Test
    @DisplayName("read returns the token stored for the gateway and audience")
    void readReturnsStoredToken() {
        when(redisManager.get(KEY)).thenReturn(Converter.valueAsString(token(300)));

        Optional<SmsfSecurityToken> read = store.read(NETWORK_ID, AMF);

        assertThat(read).isPresent();
        assertThat(read.get().accessToken()).isEqualTo(ACCESS_TOKEN);
        assertThat(read.get().targetNfType()).isEqualTo(AMF);
    }

    @ParameterizedTest(name = "an entry of [{0}] reads as empty")
    @NullSource
    @ValueSource(strings = {"   ", "{not-json"})
    @DisplayName("an unusable entry is empty, never a null token the caller could use")
    void readUnusableEntryIsEmpty(String stored) {
        when(redisManager.get(KEY)).thenReturn(stored);
        assertThat(store.read(NETWORK_ID, AMF)).isEmpty();
    }

    @Test
    @DisplayName("an entry without an access token is empty")
    void readEntryWithoutAccessTokenIsEmpty() {
        when(redisManager.get(KEY)).thenReturn(Converter.valueAsString(
                new SmsfSecurityToken(NETWORK_ID, AMF, null, "Bearer", "s",
                        Instant.now().getEpochSecond(), Instant.now().getEpochSecond() + 300)));

        assertThat(store.read(NETWORK_ID, AMF)).isEmpty();
    }

    @Test
    @DisplayName("an expired entry is empty even if Redis has not evicted it yet")
    void readExpiredEntryIsEmpty() {
        when(redisManager.get(KEY)).thenReturn(Converter.valueAsString(token(-5)));
        assertThat(store.read(NETWORK_ID, AMF)).isEmpty();
    }

    @Test
    @DisplayName("the margin rejects a token that would die in flight")
    void readAppliesTheMargin() {
        when(redisManager.get(KEY)).thenReturn(Converter.valueAsString(token(20)));

        assertThat(store.read(NETWORK_ID, AMF, 0)).isPresent();
        assertThat(store.read(NETWORK_ID, AMF, 60)).isEmpty();
    }

    @Test
    @DisplayName("a Redis failure on read is empty, not propagated")
    void readSwallowsRedisFailures() {
        when(redisManager.get(KEY)).thenThrow(new IllegalStateException("redis down"));
        assertThat(store.read(NETWORK_ID, AMF)).isEmpty();
    }

    @Test
    @DisplayName("authorizationHeader yields a ready-to-use header, or empty when unusable")
    void authorizationHeader() {
        when(redisManager.get(KEY)).thenReturn(Converter.valueAsString(token(300)));
        assertThat(store.authorizationHeader(NETWORK_ID, AMF, 30)).contains("Bearer " + ACCESS_TOKEN);
    }

    @Test
    @DisplayName("authorizationHeader is empty when no usable token is stored")
    void authorizationHeaderEmptyWhenNoToken() {
        when(redisManager.get(KEY)).thenReturn(null);
        assertThat(store.authorizationHeader(NETWORK_ID, AMF, 30)).isEmpty();
    }

    @Test
    @DisplayName("evict removes the entry, so a stopped gateway leaves no live credential behind")
    void evictRemovesTheEntry() {
        store.evict(NETWORK_ID, AMF);
        verify(redisManager).del(KEY);
    }

    @Test
    @DisplayName("a Redis failure on evict is swallowed: it must not break gateway shutdown")
    void evictSwallowsRedisFailures() {
        when(redisManager.del(KEY)).thenThrow(new IllegalStateException("redis down"));

        store.evict(NETWORK_ID, AMF);

        verify(redisManager).del(KEY);
        verify(redisManager, never()).get(anyString());
    }
}
