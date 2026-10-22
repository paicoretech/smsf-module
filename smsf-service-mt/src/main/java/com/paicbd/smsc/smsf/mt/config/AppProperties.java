package com.paicbd.smsc.smsf.mt.config;

import com.paicbd.smsc.utils.Generated;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Configuration consumed by {@code smsf-service-mt}: Kafka consumers, Scylla (DLR correlation and
 * the pending-message buffer of the custom consumer), Redis (gateway registry and the NRF token
 * published by smsf-core) and the management websocket.
 *
 * <p>The SBI/TLS server, NRF and N21 settings are deliberately absent: this module neither exposes
 * a listener nor talks to the NRF or the UDM. The SBI api root is the exception: the N1N2 failure
 * callback is advertised to the AMF from here and served by smsf-core, so both modules take the
 * same value.</p>
 */
@Getter
@Generated
@Component
public class AppProperties {

    @Value("${smsf.sbi.server.apiroot:}")
    private String sbiServerApiRoot;

    @Value("${smsf.sbi.server.base-path:/nsmsf-sms/v2}")
    private String sbiServerBasePath;

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

    @Value("${kafka.consumer.max-poll-records:100}")
    private int kafkaConsumerMaxPollRecords;

    @Value("${scylla.contact.points}")
    private String contactPoints;

    @Value("${scylla.datacenter}")
    private String localDatacenter;

    @Value("${scylla.user}")
    private String username;

    @Value("${scylla.password}")
    private String password;

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

    /**
     * Treat the Redis token as unusable this many seconds before it actually expires, so a request
     * is not sent with a credential that dies in flight. Mirrors the margin smsf-core refreshes on.
     */
    @Value("${smsf.security.outbound-token.refresh-margin-seconds:30}")
    private long outboundTokenRefreshMarginSeconds;

    /** How often the module re-checks Redis for a usable token to pause or resume its consumers. */
    @Value("${smsf.security.outbound-token.redis-check-interval-seconds:10}")
    private long tokenCheckIntervalSeconds;

    @Value("${smsf.mt.amf-connection.check-interval-seconds:5}")
    private long amfConnectionCheckIntervalSeconds;

    /** How long to wait for the AMF failure callback when the AMF reported no maxWaitingTime. */
    @Value("${smsf.mt.correlation.default-wait-seconds:300}")
    private int correlationWaitSeconds;

    /**
     * How often Redis is re-read once the rotation is due but has not landed, between
     * {@code expiresAt - margin} and {@code expiresAt}. Short so the new token is picked up the
     * moment smsf-core publishes it; the window is bounded by the margin.
     */
    @Value("${smsf.security.outbound-token.stale-poll-interval-ms:100}")
    private long tokenStalePollIntervalMs;
}
