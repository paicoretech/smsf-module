package com.paicbd.smsc.smsf.common.redis;

import com.paicbd.smsc.exception.RTException;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.RedisManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.Optional;

/**
 * Redis-backed exchange for the NRF-derived access token.
 *
 * <p>{@code smsf-core} is the only writer: it owns the NRF integration and republishes on every
 * refresh. {@code smsf-n21-registration} and {@code smsf-service-mt} are readers, so that neither
 * of them opens a second token flow against the NRF.</p>
 *
 * <p>It goes through {@link RedisManager} rather than a {@code JedisCluster} directly, so the same
 * code works against a Redis cluster and against a standalone instance — the deployment picks one
 * with {@code redis.standalone.enabled}.</p>
 *
 * <p>Entries are stored as one Redis string per ({@code networkId}, {@code targetNfType}) pair with
 * an expiry matching the token's own lifetime, so an entry that stops being refreshed disappears on
 * its own instead of lingering as a stale credential. Readers still re-check the expiry carried in
 * the value, because Redis expiry and the token's {@code exp} can drift.</p>
 *
 * <p>Never log {@link SmsfSecurityToken#accessToken()}: failures are reported with the key and the
 * reason only.</p>
 */
@Slf4j
@RequiredArgsConstructor
public class SmsfTokenRedisStore {

    /** Prefix of every token entry. The full key is {@code smsf:token:<networkId>:<targetNfType>}. */
    public static final String KEY_PREFIX = "smsf:token:";

    /** Floor for the Redis TTL, so a very short-lived token still lands as a readable entry. */
    private static final long MIN_TTL_SECONDS = 1;

    private final RedisManager redisManager;

    public static String keyFor(int networkId, String targetNfType) {
        return KEY_PREFIX + networkId + ":" + targetNfType;
    }

    /**
     * Publishes the token, replacing any previous entry for the same gateway and audience. The
     * Redis TTL is the token's remaining lifetime, so consumers cannot read an entry that the
     * issuer already considers dead.
     *
     * @return {@code true} when the entry was written.
     */
    public boolean publish(SmsfSecurityToken token) {
        if (Objects.isNull(token) || Objects.isNull(token.accessToken()) || token.accessToken().isBlank()) {
            log.warn("Refusing to publish an empty token to Redis");
            return false;
        }
        String key = keyFor(token.networkId(), token.targetNfType());
        long ttl = Math.max(MIN_TTL_SECONDS, token.secondsToExpiry());
        try {
            redisManager.setex(key, ttl, Converter.valueAsString(token));
            log.debug("Published NRF token to Redis key={} ttl={}s", key, ttl);
            return true;
        } catch (Exception ex) {
            log.error("Could not publish the NRF token to Redis key={}: {}", key, ex.getMessage());
            return false;
        }
    }

    /**
     * Reads the token for a gateway and audience. Returns empty when the entry is missing,
     * unreadable, or already expired — never a token the caller should not use.
     */
    public Optional<SmsfSecurityToken> read(int networkId, String targetNfType) {
        return read(networkId, targetNfType, 0);
    }

    /**
     * @param marginSeconds treat the token as expired this many seconds before its actual expiry,
     *                      so a request is not sent with a credential that dies in flight.
     */
    public Optional<SmsfSecurityToken> read(int networkId, String targetNfType, long marginSeconds) {
        String key = keyFor(networkId, targetNfType);
        String raw;
        try {
            raw = redisManager.get(key);
        } catch (Exception ex) {
            log.error("Could not read the NRF token from Redis key={}: {}", key, ex.getMessage());
            return Optional.empty();
        }
        if (Objects.isNull(raw) || raw.isBlank()) {
            log.warn("No NRF token available in Redis for key={}", key);
            return Optional.empty();
        }
        SmsfSecurityToken token;
        try {
            token = Converter.stringToObject(raw, SmsfSecurityToken.class);
        } catch (RTException ex) {
            log.error("Malformed NRF token entry in Redis key={}: {}", key, ex.getMessage());
            return Optional.empty();
        }
        if (Objects.isNull(token) || Objects.isNull(token.accessToken()) || token.accessToken().isBlank()) {
            log.error("Empty NRF token entry in Redis key={}", key);
            return Optional.empty();
        }
        if (token.isExpired(marginSeconds)) {
            log.warn("Expired NRF token in Redis key={} (expired {}s ago)",
                    key, java.time.Instant.now().getEpochSecond() - token.expiresAtEpochSecond());
            return Optional.empty();
        }
        return Optional.of(token);
    }

    /** The {@code Authorization} header value, or empty when no usable token is available. */
    public Optional<String> authorizationHeader(int networkId, String targetNfType, long marginSeconds) {
        return read(networkId, targetNfType, marginSeconds).map(SmsfSecurityToken::authorizationHeader);
    }

    /** Removes the entry. Used when a gateway is disabled or evicted. */
    public void evict(int networkId, String targetNfType) {
        String key = keyFor(networkId, targetNfType);
        try {
            redisManager.del(key);
        } catch (Exception ex) {
            log.warn("Could not evict the NRF token from Redis key={}: {}", key, ex.getMessage());
        }
    }
}
