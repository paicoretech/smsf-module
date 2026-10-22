package com.paicbd.smsc.smsf.core.e2e;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.Http2GatewayNrfSecurity;
import com.paicbd.smsc.dto.Http2GatewayOutboundSecurity;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.common.e2e.SmsfE2EInfrastructure;
import com.paicbd.smsc.smsf.common.redis.SmsfSecurityToken;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenRedisStore;
import com.paicbd.smsc.smsf.common.utils.Http2GatewayCreator;
import com.paicbd.smsc.smsf.core.api.service.RegisterService;
import com.paicbd.smsc.smsf.core.config.AppProperties;
import com.paicbd.smsc.smsf.core.stack.CoreStackManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.RedisManager;
import com.paicbd.smsc.smsf.common.event.SmsfRegistrationEvent;
import com.paicbd.smsc.smsf.common.event.RegistrationAction;
import com.paicore.sba.stack.openapi.model.AccessType;
import com.paicore.sba.stack.openapi.model.UeSmsContextData;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.paicbd.smsc.smsf.common.constants.Constants.ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.when;

/**
 * End-to-end of what {@code smsf-core} produces, on real infrastructure.
 *
 * <p>The token is fetched from a NRF listening on a real port and published to a real Redis; the
 * registration event is published to a real Kafka broker and read back from it. Every assertion is
 * made against the infrastructure, not against a mock, so the traffic is observable on the wire.</p>
 *
 * <p>ScyllaDB is the one dependency stubbed here. A Scylla container needs more AIO capacity than a
 * developer host running other Scylla nodes can give it, which is why {@code smsc-management-be}
 * also mocks its {@code ScyllaManager} in the integration tests.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CoreRegistrationE2ETest {

    private static final int NETWORK_ID = 88;
    // Distinct per case: the topic is shared, so a test must not match another test's record.
    private static final String REGISTER_SUPI = "imsi-460000000000088";
    private static final String DEREGISTER_SUPI = "imsi-460000000000089";
    private static final String TOPIC = NETWORK_ID + ".smsf.udm.register";
    private static final String TOKEN_PATH = "/oauth2/token";

    @Mock
    private AppProperties appProperties;

    @Mock
    private ScyllaManager scyllaManager;

    private WireMockServer nrf;
    private int nrfPort;
    private Http2Gateway gateway;
    private RedisManager redisManager;
    private SmsfTokenRedisStore tokenStore;
    private KafkaTemplate<String, String> kafkaTemplate;

    @BeforeAll
    static void bootInfrastructure() {
        SmsfE2EInfrastructure.start();
        SmsfE2EInfrastructure.createTopics(TOPIC);
    }

    @BeforeEach
    void setUp() {
        nrf = new WireMockServer(wireMockConfig().dynamicPort());
        nrf.start();
        nrfPort = nrf.port();
        System.out.printf("  NRF simulator 127.0.0.1:%d%n", nrfPort);

        gateway = Http2GatewayCreator.buildHttp2GatewayWithMtls("smsf-gw", NETWORK_ID, "amf.local:8443", ENABLED);
        Http2GatewayNrfSecurity nrfSecurity = gateway.getNrfSecurity();
        nrfSecurity.setApiRoot("http://localhost:" + nrf.port());
        nrfSecurity.setTokenPath(TOKEN_PATH);
        nrfSecurity.setMtlsEnabled(false);
        nrfSecurity.setNrfRegisterPath(null);

        Http2GatewayOutboundSecurity outbound = gateway.getOutboundSecurity();
        outbound.setOauth2Enabled(true);
        outbound.setUsingNrf(true);
        outbound.setClientId("smsf-client");
        outbound.setClientSecret("s3cr3t");
        outbound.setScope("nsmsf-sms");

        when(appProperties.getOutboundTokenRefreshMarginSeconds()).thenReturn(30L);
        when(appProperties.getOutboundTokenInitialFetchTimeoutSeconds()).thenReturn(10L);
        when(appProperties.getOutboundTokenRetryBackoffSeconds()).thenReturn(5L);
        when(appProperties.getN21Oauth2Scope()).thenReturn("nudm-uecm");

        redisManager = SmsfE2EInfrastructure.redisManager();
        tokenStore = new SmsfTokenRedisStore(redisManager);
        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps()));
    }

    @AfterEach
    void tearDown() {
        tokenStore.evict(NETWORK_ID, Constants.NF_TYPE_AMF);
        tokenStore.evict(NETWORK_ID, Constants.NF_TYPE_UDM);
        nrf.stop();
    }

    private static Map<String, Object> producerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SmsfE2EInfrastructure.kafkaBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }

    private void nrfIssues(String accessToken) {
        nrf.stubFor(post(urlEqualTo(TOKEN_PATH)).willReturn(aResponse()
                .withStatus(200).withHeader("content-type", "application/json")
                .withBody("{\"access_token\":\"" + accessToken + "\",\"token_type\":\"Bearer\","
                        + "\"expires_in\":3600}")));
    }

    private static UeSmsContextData ueContext() {
        return new UeSmsContextData().supi(REGISTER_SUPI)
                .gpsi("msisdn-8613915900088").accessType(AccessType._3_GPP_ACCESS);
    }

    // --- NRF → Redis ----------------------------------------------------------

    @Test
    @DisplayName("NRF → Redis: the token is fetched over the wire and lands in Redis with a TTL")
    void tokenReachesRedis() {
        nrfIssues("e2e-amf-token");

        CoreStackManager manager = new CoreStackManager(gateway, appProperties, tokenStore,
                new ConcurrentHashMap<>());
        try {
            manager.start();

            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                Optional<SmsfSecurityToken> stored = tokenStore.read(NETWORK_ID, Constants.NF_TYPE_AMF);
                assertThat(stored).isPresent();
                assertThat(stored.get().accessToken()).isEqualTo("e2e-amf-token");
                assertThat(stored.get().issuedAtEpochSecond()).isPositive();
            });

            // The value really is in Redis, under the documented key and with a TTL.
            String key = SmsfTokenRedisStore.keyFor(NETWORK_ID, Constants.NF_TYPE_AMF);
            assertThat(redisManager.get(key)).contains("e2e-amf-token");
            assertThat(redisManager.exists(key)).isTrue();

            // And the request really left the module.
            nrf.verify(postRequestedFor(urlEqualTo(TOKEN_PATH))
                    .withRequestBody(containing("grant_type=client_credentials"))
                    .withRequestBody(containing("targetNfType=AMF")));
        } finally {
            manager.stop();
        }
    }

    @Test
    @DisplayName("stopping the gateway really removes the entry from Redis")
    void stoppingRemovesTheEntryFromRedis() {
        nrfIssues("e2e-amf-token");
        CoreStackManager manager = new CoreStackManager(gateway, appProperties, tokenStore,
                new ConcurrentHashMap<>());
        manager.start();
        String key = SmsfTokenRedisStore.keyFor(NETWORK_ID, Constants.NF_TYPE_AMF);
        await().atMost(Duration.ofSeconds(20)).until(() -> redisManager.exists(key));

        manager.stop();

        assertThat(redisManager.exists(key)).isFalse();
    }

    // --- controller → Kafka ---------------------------------------------------

    @Test
    @DisplayName("registration → Kafka: the event really lands on the topic, keyed by SUPI")
    void registrationEventReachesKafka() {
        RegisterService service = new RegisterService(scyllaManager, kafkaTemplate);

        service.publishRegistration(NETWORK_ID, ueContext());

        List<String> records = await().atMost(Duration.ofSeconds(30))
                .until(() -> SmsfE2EInfrastructure.consumeAll(
                                TOPIC, "assert-" + UUID.randomUUID(), Duration.ofSeconds(5)),
                        list -> list.stream().anyMatch(v -> v.contains(REGISTER_SUPI)));

        SmsfRegistrationEvent event = records.stream()
                .map(v -> Converter.stringToObject(v, SmsfRegistrationEvent.class))
                .filter(e -> REGISTER_SUPI.equals(e.getSupi()))
                .findFirst().orElseThrow();
        assertThat(event.getAction()).isEqualTo(RegistrationAction.REGISTER);
        assertThat(event.getNetworkId()).isEqualTo(NETWORK_ID);
        assertThat(event.getUeContext()).isNotNull();
        assertThat(event.getUeContext().getSupi()).isEqualTo(REGISTER_SUPI);
    }

    @Test
    @DisplayName("deregistration → Kafka: the event carries only the SUPI")
    void deregistrationEventReachesKafka() {
        RegisterService service = new RegisterService(scyllaManager, kafkaTemplate);

        service.publishDeregistration(NETWORK_ID, DEREGISTER_SUPI);

        List<String> records = await().atMost(Duration.ofSeconds(30))
                .until(() -> SmsfE2EInfrastructure.consumeAll(
                                TOPIC, "assert-" + UUID.randomUUID(), Duration.ofSeconds(5)),
                        list -> list.stream().anyMatch(v -> v.contains(DEREGISTER_SUPI)));

        SmsfRegistrationEvent event = records.stream()
                .map(v -> Converter.stringToObject(v, SmsfRegistrationEvent.class))
                .filter(e -> DEREGISTER_SUPI.equals(e.getSupi()))
                .findFirst().orElseThrow();
        assertThat(event.getAction()).isEqualTo(RegistrationAction.DEREGISTER);
        assertThat(event.getUeContext()).isNull();
    }
}
