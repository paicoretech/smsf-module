package com.paicbd.smsc.smsf.n21.config;

import com.paicbd.smsc.utils.Generated;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Configuration consumed by {@code smsf-n21-registration}: the registration Kafka consumer, the
 * N21/UDM client, NRF discovery, Redis (gateway registry and the UDM token published by smsf-core)
 * and the management websocket.
 *
 * <p>No SBI server, no ScyllaDB and no OAuth2 client credentials: this module exposes no listener,
 * persists nothing, and reads its token instead of minting it.</p>
 */
@Getter
@Generated
@Component
public class AppProperties {

    @Value("#{'${redis.cluster.nodes}'.split(',')}")
    private List<String> redisNodes;

    @Value("${redis.threadPool.maxTotal:50}")
    private int redisMaxTotal;

    @Value("${redis.threadPool.maxIdle:30}")
    private int redisMaxIdle;

    @Value("${redis.threadPool.minIdle:5}")
    private int redisMinIdle;

    @Value("${redis.threadPool.blockWhenExhausted}")
    private boolean redisBlockWhenExhausted;

    @Value("${redis.connection.timeout:0}")
    private int redisConnectionTimeout;

    @Value("${redis.so.timeout:0}")
    private int redisSoTimeout;

    @Value("${redis.maxAttempts:0}")
    private int redisMaxAttempts;

    @Value("${redis.connection.user:}")
    private String redisUser;

    @Value("${redis.connection.password:}")
    private String redisPassword;

    /**
     * Connect to a single Redis instance instead of a cluster. When enabled only the first entry of
     * {@code redis.cluster.nodes} is used, as the host:port of that instance.
     */
    @Value("${redis.standalone.enabled:false}")
    private boolean redisStandaloneEnabled;

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;

    @Value("${spring.kafka.listener.concurrency:1}")
    private int kafkaListenerConcurrency;

    @Value("${kafka.consumer.register.max-poll-records:100}")
    private int kafkaConsumerMaxPollRecords;

    @Value("${websocket.server.enabled}")
    private boolean wsEnabled;

    @Value("${websocket.server.host}")
    private String wsHost;

    @Value("${websocket.server.port}")
    private int wsPort;

    @Value("${websocket.server.path}")
    private String wsPath;

    @Value("${websocket.retry.intervalSeconds}")
    private int wsRetryInterval;

    @Value("${websocket.header.name}")
    private String wsHeaderName;

    @Value("${websocket.header.value}")
    private String wsHeaderValue;

    /** Treat the Redis token as unusable this many seconds before it actually expires. */
    @Value("${smsf.security.outbound-token.refresh-margin-seconds:30}")
    private long outboundTokenRefreshMarginSeconds;

    /**
     * How often Redis is re-checked for the UDM token. The registration consumer is started when a
     * usable token appears and stopped when it goes away, so this is also the recovery latency.
     */
    @Value("${smsf.security.outbound-token.redis-check-interval-seconds:10}")
    private long tokenCheckIntervalSeconds;

    /**
     * How often Redis is re-read once the rotation is due but has not landed, between
     * {@code expiresAt - margin} and {@code expiresAt}. Short so the new token is picked up the
     * moment smsf-core publishes it; the window is bounded by the margin.
     */
    @Value("${smsf.security.outbound-token.stale-poll-interval-ms:100}")
    private long tokenStalePollIntervalMs;

    @Value("${smsf.n21.discovery.cache-ttl-seconds:300}")
    private int n21DiscoveryCacheTtlSeconds;

    @Value("${smsf.n21.discovery.subscriber-aware:true}")
    private boolean n21SubscriberAwareDiscovery;

    @Value("${smsf.n21.discovery.max-endpoints:4}")
    private int n21MaxEndpoints;

    /** Minimum wait before retrying to start an N21 client whose startup failed. */
    @Value("${smsf.n21.restart-min-interval-seconds:30}")
    private long n21RestartMinIntervalSeconds;

    /**
     * Home PLMN reported to the UDM when the AMF sends no GUAMI. Empty by default: without it
     * the registration is dropped rather than being sent with an invented PLMN.
     */
    @Value("${smsf.n21.home-plmn.mcc:}")
    private String n21HomePlmnMcc;

    @Value("${smsf.n21.home-plmn.mnc:}")
    private String n21HomePlmnMnc;
}
