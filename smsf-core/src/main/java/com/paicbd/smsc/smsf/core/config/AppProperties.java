package com.paicbd.smsc.smsf.core.config;

import com.paicbd.smsc.utils.Generated;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Getter
@Generated
@Component
public class AppProperties {

    @Value("${smsf.sbi.server.base-path:/nsmsf-sms/v2}")
    private String smsfSbiServerBasePath;

    @Value("${smsf.sbi.server.apiroot}")
    private String smsfSbiServerApiRoot;

    @Value("${smsf.sbi.public-address:127.0.0.1}")
    private String smsfSbiPublicAddress;

    @Value("${smsf.sbi.http2.max-concurrent-streams:256}")
    private long sbiMaxConcurrentStreams;

    @Value("${smsf.security.jwt.verification-cache-ttl-seconds:30}")
    private long securityJwtVerificationCacheTtlSeconds;

    @Value("${smsf.security.jwt.verification-cache-max-size:1000}")
    private int securityJwtVerificationCacheMaxSize;

    @Value("${smsf.kafka.producer.linger-ms:0}")
    private int kafkaProducerLingerMs;

    @Value("${smsf.kafka.producer.batch-size:16384}")
    private int kafkaProducerBatchSize;

    @Value("${smsf.kafka.producer.compression-type:none}")
    private String kafkaProducerCompressionType;

    @Value("${smsf.kafka.producer.enable-idempotence:true}")
    private boolean kafkaProducerEnableIdempotence;

    @Value("${smsf.kafka.producer.acks:all}")
    private String kafkaProducerAcks;

    @Value("${server.port:9070}")
    private int serverPort;

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

    @Value("${smsf.security.enabled:true}")
    private boolean securityEnabled;

    @Value("#{'${smsf.security.bypass-paths:/actuator/**,/health,/info}'.split(',')}")
    private List<String> securityBypassPaths;

    @Value("${smsf.security.expected-scope:nsmsf-sms}")
    private String securityExpectedScope;

    @Value("#{'${smsf.security.allowed-jws-algorithms:RS256,ES256}'.split(',')}")
    private List<String> securityAllowedJwsAlgorithms;

    @Value("${smsf.security.clock-skew-seconds:30}")
    private long securityClockSkewSeconds;

    @Value("${smsf.security.log-success:false}")
    private boolean securityLogSuccess;

    @Value("${smsf.security.jwks.cache-ttl-seconds:600}")
    private long jwksCacheTtlSeconds;

    @Value("${smsf.security.jwks.cache-max-size:100}")
    private int jwksCacheMaxSize;

    @Value("${smsf.security.jwks.fetch-timeout-ms:5000}")
    private long jwksFetchTimeoutMs;

    @Value("${smsf.security.jwks.fetch-retry-max:2}")
    private int jwksFetchRetryMax;

    @Value("${smsf.security.bearer.header-name:Authorization}")
    private String bearerHeaderName;

    @Value("${smsf.security.bearer.scheme:Bearer}")
    private String bearerScheme;

    @Value("${smsf.tls.server.key-password:}")
    private String tlsServerKeyPassword;

    @Value("#{'${smsf.tls.protocols:TLSv1.3,TLSv1.2}'.split(',')}")
    private List<String> tlsProtocols;

    @Value("${smsf.tls.session-cache-size:1024}")
    private int tlsSessionCacheSize;

    @Value("${smsf.tls.session-timeout-seconds:300}")
    private long tlsSessionTimeoutSeconds;

    @Value("${smsf.tls.handshake-timeout-ms:10000}")
    private long tlsHandshakeTimeoutMs;

    @Value("${smsf.security.outbound-token.refresh-margin-seconds:30}")
    private long outboundTokenRefreshMarginSeconds;

    @Value("${smsf.security.outbound-token.initial-fetch-timeout-seconds:10}")
    private long outboundTokenInitialFetchTimeoutSeconds;

    @Value("${smsf.security.outbound-token.retry-backoff-seconds:5}")
    private long outboundTokenRetryBackoffSeconds;

    @Value("${smsf.nrf.api-full-version:2.0.0}")
    private String nrfApiFullVersion;

    @Value("${smsf.nrf.heartbeat-default-seconds:60}")
    private int nrfHeartbeatDefaultSeconds;

    @Value("${smsf.nrf.initial-register-timeout-seconds:10}")
    private long nrfInitialRegisterTimeoutSeconds;

    @Value("${smsf.nrf.retry-backoff-seconds:5}")
    private long nrfRetryBackoffSeconds;

    @Value("${smsf.n21.oauth2.scope:nudm-uecm}")
    private String n21Oauth2Scope;

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

    @Value("${message.parts.ttl:30}")
    private int messagePartsTtl;

    @Value("${message.parts.cache.ttl-grace-seconds:5}")
    private int messagePartsCacheTtlGraceSeconds;

    @Value("${message.parts.cache.max-size:50000}")
    private long messagePartsCacheMaxSize;

}
