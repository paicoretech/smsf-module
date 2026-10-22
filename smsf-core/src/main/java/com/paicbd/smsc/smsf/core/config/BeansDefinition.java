package com.paicbd.smsc.smsf.core.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paicbd.smsc.dto.ErrorCodeMapping;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.smsf.core.security.GatewaySecurityLookup;
import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.core.security.SecurityConstants;
import com.paicbd.smsc.smsf.core.security.SecurityMetrics;
import com.paicbd.smsc.smsf.core.security.SmsfGatewaySecurityLookupAdapter;
import com.paicbd.smsc.smsf.core.security.tls.SmsfPlaceholderSslContextFactory;
import com.paicbd.smsc.smsf.core.security.tls.SmsfSslContextProvider;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenRedisStore;
import com.paicbd.smsc.smsf.core.stack.CoreStackManager;
import com.paicbd.smsc.smsf.core.processors.multipart.MultiPartsHandler;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.Generated;
import com.paicbd.smsc.utils.RedisManager;
import com.paicbd.smsc.ws.SocketSession;
import com.paicore.sba.stack.security.audit.SecurityAuditLogger;
import com.paicore.sba.stack.security.audit.SecurityAuditMetrics;
import com.paicore.sba.stack.security.problem.SecurityProblemWriter;
import com.paicore.sba.stack.security.tls.PeerCertValidator;
import com.paicore.sba.stack.security.webfilter.Http2SecurityFilterConfig;
import com.paicore.sba.stack.security.webfilter.Http2SecurityWebFilter;
import com.paicore.sba.stack.transport.http2.tls.Http2NettyTlsCustomizer;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.embedded.netty.NettyReactiveWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import reactor.netty.http.HttpProtocol;

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

    @Bean
    public ScyllaManager scyllaManager() {
        ScyllaManager manager = new ScyllaManager(
                appProperties.getContactPoints(),
                appProperties.getLocalDatacenter(),
                appProperties.getUsername(),
                appProperties.getPassword(),
                false
        );
        manager.createSmsfUeSmsContextTables();
        return manager;
    }

    @Bean
    public MultiPartsHandler multiPartsHandler(ScyllaManager scyllaManager,
                                               KafkaTemplate<String, String> kafkaTemplate) {
        return new MultiPartsHandler(
                scyllaManager,
                kafkaTemplate,
                appProperties.getMessagePartsTtl(),
                appProperties.getMessagePartsTtl() + appProperties.getMessagePartsCacheTtlGraceSeconds(),
                appProperties.getMessagePartsCacheMaxSize());
    }

    @Bean
    public SocketSession socketSession() {
        return new SocketSession("gw");
    }

    @Bean
    public ConcurrentMap<Integer, CoreStackManager> gatewayStacks() {
        return new ConcurrentHashMap<>();
    }

    /** Writer side of the NRF token exchange; smsf-n21-registration and smsf-service-mt only read. */
    @Bean
    public SmsfTokenRedisStore smsfTokenRedisStore(RedisManager redisManager) {
        return new SmsfTokenRedisStore(redisManager);
    }

    @Bean
    public PeerCertValidator peerCertValidator() {
        return new PeerCertValidator();
    }

    @Bean
    public SecurityProblemWriter securityProblemWriter(ObjectMapper objectMapper) {
        return new SecurityProblemWriter(objectMapper,
                SecurityConstants.PROBLEM_TYPE_BASE,
                SecurityConstants.WWW_AUTHENTICATE_REALM,
                Constants.PROBLEM_JSON_CONTENT_TYPE);
    }

    @Bean
    public SecurityAuditMetrics securityAuditMetrics(SecurityMetrics securityMetrics) {
        return new SecurityAuditMetrics() {
            @Override
            public void incrementAuthSuccess() {
                securityMetrics.incrementAuthSuccess();
            }

            @Override
            public void incrementAuthFailure() {
                securityMetrics.incrementAuthFailure();
            }
        };
    }

    @Bean
    public SecurityAuditLogger securityAuditLogger(SecurityAuditMetrics securityAuditMetrics) {
        return new SecurityAuditLogger(appProperties.isSecurityLogSuccess(), securityAuditMetrics);
    }

    @Bean
    public Http2SecurityFilterConfig http2SecurityFilterConfig() {
        return Http2SecurityFilterConfig.builder()
                .securityEnabled(appProperties.isSecurityEnabled())
                .sbiServerBasePath(appProperties.getSmsfSbiServerBasePath())
                .bypassPaths(appProperties.getSecurityBypassPaths())
                .bearerHeaderName(appProperties.getBearerHeaderName())
                .bearerScheme(appProperties.getBearerScheme())
                .build();
    }

    @Bean
    public Http2SecurityWebFilter http2SecurityWebFilter(Http2SecurityFilterConfig http2SecurityFilterConfig,
                                                         GatewaySecurityLookup gatewaySecurityLookup,
                                                         SecurityProblemWriter securityProblemWriter,
                                                         SecurityAuditLogger securityAuditLogger,
                                                         PeerCertValidator peerCertValidator) {
        return new Http2SecurityWebFilter(
                http2SecurityFilterConfig,
                new SmsfGatewaySecurityLookupAdapter(gatewaySecurityLookup),
                securityProblemWriter,
                securityAuditLogger,
                peerCertValidator,
                SecurityConstants.EXCHANGE_ATTR_GATEWAY,
                SecurityConstants.EXCHANGE_ATTR_PRINCIPAL);
    }

    @Bean
    @ConditionalOnProperty(name = "smsf.tls.enabled", havingValue = "true", matchIfMissing = true)
    public Http2NettyTlsCustomizer http2NettyTlsCustomizer(SmsfSslContextProvider sslContextProvider) {
        return new Http2NettyTlsCustomizer(
                SmsfPlaceholderSslContextFactory.build(),
                sslContextProvider::get,
                appProperties.getTlsHandshakeTimeoutMs(),
                appProperties.getSbiMaxConcurrentStreams());
    }

    @Bean
    @ConditionalOnProperty(name = "smsf.tls.enabled", havingValue = "false")
    public WebServerFactoryCustomizer<NettyReactiveWebServerFactory> smsfH2cServerCustomizer() {
        return factory -> factory.addServerCustomizers(
                httpServer -> httpServer
                        .protocol(HttpProtocol.HTTP11, HttpProtocol.H2C)
                        .http2Settings(spec -> spec.maxConcurrentStreams(
                                appProperties.getSbiMaxConcurrentStreams())));
    }
}
