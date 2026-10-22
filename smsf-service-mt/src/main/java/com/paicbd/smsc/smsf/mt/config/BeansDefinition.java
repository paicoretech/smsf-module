package com.paicbd.smsc.smsf.mt.config;

import com.paicbd.smsc.dto.ErrorCodeMapping;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenCache;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenRedisStore;
import com.paicbd.smsc.smsf.mt.stack.MtStackManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.Generated;
import com.paicbd.smsc.utils.RedisManager;
import com.paicbd.smsc.ws.SocketSession;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Generated
@Configuration
@RequiredArgsConstructor
public class BeansDefinition {
    private final AppProperties appProperties;

    @Bean
    public ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingConcurrentHashMap() {
        return new ConcurrentHashMap<>();
    }

    /**
     * Cluster or standalone according to {@code redis.standalone.enabled}. In standalone mode only
     * the first entry of {@code redis.cluster.nodes} is used, as the host:port of the instance.
     */
    @Bean
    public RedisManager redisManager() {
        return Converter.paramsToRedisManager(
                new UtilsRecords.JedisConfigParams(appProperties.getRedisNodes(), appProperties.getRedisMaxTotal(),
                        appProperties.getRedisMaxIdle(), appProperties.getRedisMinIdle(),
                        appProperties.isRedisBlockWhenExhausted(), appProperties.getRedisConnectionTimeout(),
                        appProperties.getRedisSoTimeout(), appProperties.getRedisMaxAttempts(),
                        appProperties.getRedisUser(), appProperties.getRedisPassword(),
                        appProperties.isRedisStandaloneEnabled())
        );
    }

    /** Read-only here: smsf-core is the only writer of the NRF token. */
    @Bean
    public SmsfTokenRedisStore smsfTokenRedisStore(RedisManager redisManager) {
        return new SmsfTokenRedisStore(redisManager);
    }

    /**
     * Serves the AMF token from memory and refreshes it in the background from its own expiry,
     * so sending a request never touches Redis.
     */
    @Bean(destroyMethod = "close")
    public SmsfTokenCache smsfTokenCache(SmsfTokenRedisStore smsfTokenRedisStore) {
        return new SmsfTokenCache(smsfTokenRedisStore,
                appProperties.getOutboundTokenRefreshMarginSeconds(),
                appProperties.getTokenCheckIntervalSeconds(),
                appProperties.getTokenStalePollIntervalMs());
    }

    /**
     * Needed for the DLR/submit-result correlation and for the pending-message buffer that
     * {@code KafkaConsumerCustomImpl} keeps on shutdown. The UE SMS context tables stay in
     * smsf-core, so this module does not create them.
     */
    @Bean
    public ScyllaManager scyllaManager() {
        return new ScyllaManager(
                appProperties.getContactPoints(),
                appProperties.getLocalDatacenter(),
                appProperties.getUsername(),
                appProperties.getPassword(),
                false
        );
    }

    @Bean
    public SocketSession socketSession() {
        return new SocketSession("gw");
    }

    @Bean
    public ConcurrentMap<Integer, MtStackManager> gatewayStacks() {
        return new ConcurrentHashMap<>();
    }
}
