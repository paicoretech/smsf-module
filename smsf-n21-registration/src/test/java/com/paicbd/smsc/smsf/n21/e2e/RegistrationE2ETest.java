package com.paicbd.smsc.smsf.n21.e2e;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.Http2GatewayUDMSecurity;
import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.common.e2e.SmsfE2EInfrastructure;
import com.paicbd.smsc.smsf.common.redis.SmsfSecurityToken;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenCache;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenRedisStore;
import com.paicbd.smsc.smsf.common.utils.Http2GatewayCreator;
import com.paicbd.smsc.smsf.n21.config.AppProperties;
import com.paicbd.smsc.smsf.n21.stack.N21StackManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.RedisManager;
import com.paicbd.smsc.smsf.common.event.SmsfRegistrationEvent;
import com.paicbd.smsc.smsf.common.event.RegistrationAction;
import com.paicore.sba.stack.openapi.model.AccessType;
import com.paicore.sba.stack.openapi.model.UeSmsContextData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.paicbd.smsc.smsf.common.constants.Constants.ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.when;

/**
 * End-to-end registration flow, on real infrastructure.
 *
 * <p>The event is produced to a real Kafka broker, the module's own listener container consumes it,
 * the UDM token is read from a real Redis, and the forwarding leaves as real HTTP/2 to a UDM
 * listening on a real port. Every hop is observable on the wire: Kafka on its broker port, Redis on
 * 6379 of its container, and the N21 request on the UDM's port.</p>
 *
 * <p>One message per case — enough to prove the path, cheap enough to run on every build.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RegistrationE2ETest {

    private static final int NETWORK_ID = 77;
    private static final String SUPI = "imsi-460000000000077";
    private static final String TOPIC = NETWORK_ID + ".smsf.udm.register";
    private static final String REGISTRATION_PATH = "/nudm-uecm/v1/{supi}/registrations/smsf-3gpp-access";
    private static final String EXPECTED_URL = "/nudm-uecm/v1/" + SUPI + "/registrations/smsf-3gpp-access";
    private static final String ACCESS_TOKEN = "e2e-udm-token";

    @Mock
    private AppProperties appProperties;

    private WireMockServer udm;
    private int udmPort;
    private Http2Gateway gateway;
    private N21StackManager manager;
    private SmsfTokenCache tokenCache;
    private SmsfTokenRedisStore tokenStore;

    @BeforeAll
    static void bootInfrastructure() {
        SmsfE2EInfrastructure.start();
        SmsfE2EInfrastructure.createTopics(TOPIC);
    }

    @BeforeEach
    void setUp() {
        udm = new WireMockServer(wireMockConfig().dynamicPort());
        udm.start();
        udmPort = udm.port();
        System.out.printf("  UDM simulator 127.0.0.1:%d%n", udmPort);

        gateway = Http2GatewayCreator.buildHttp2GatewayWithMtls("smsf-gw", NETWORK_ID, "amf.local:8443", ENABLED);
        gateway.setNfInstanceId(UUID.fromString("11111111-2222-3333-4444-555555555555"));

        Http2GatewayUDMSecurity udmSecurity = new Http2GatewayUDMSecurity();
        udmSecurity.setForwardRegistrationEnabled(true);
        udmSecurity.setOauth2Enabled(true);
        udmSecurity.setAuthorizationHeader("authorization");
        udmSecurity.setNrfDiscoveryEnabled(false);
        udmSecurity.setApiRoot("http://localhost:" + udm.port());
        udmSecurity.setRegistrationPath(REGISTRATION_PATH);
        udmSecurity.setDeregistrationPath(REGISTRATION_PATH);
        udmSecurity.setMtlsEnabled(false);
        gateway.setUdmSecurity(udmSecurity);

        when(appProperties.getOutboundTokenRefreshMarginSeconds()).thenReturn(30L);
        when(appProperties.getTokenCheckIntervalSeconds()).thenReturn(1L);
        when(appProperties.getKafkaListenerConcurrency()).thenReturn(1);
        when(appProperties.getN21RestartMinIntervalSeconds()).thenReturn(30L);
        when(appProperties.getN21DiscoveryCacheTtlSeconds()).thenReturn(300);
        when(appProperties.getN21MaxEndpoints()).thenReturn(4);
        when(appProperties.getN21HomePlmnMcc()).thenReturn("460");
        when(appProperties.getN21HomePlmnMnc()).thenReturn("01");

        RedisManager redisManager = SmsfE2EInfrastructure.redisManager();
        tokenStore = new SmsfTokenRedisStore(redisManager);
        tokenCache = new SmsfTokenCache(tokenStore, 30L, 1L, 50L);

        manager = new N21StackManager(gateway, appProperties, tokenCache, realConsumerFactory());
    }

    @AfterEach
    void tearDown() {
        if (manager.isStarted()) {
            manager.stop();
        }
        tokenCache.close();
        tokenStore.evict(NETWORK_ID, Constants.NF_TYPE_UDM);
        udm.stop();
    }

    /** A consumer factory pointing at the real broker, as the module's own configuration builds it. */
    private ConsumerFactory<String, String> realConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SmsfE2EInfrastructure.kafkaBootstrapServers());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /** Publishes the token to the real Redis, as smsf-core does. */
    private void publishTokenToRedis() {
        long now = Instant.now().getEpochSecond();
        tokenStore.publish(new SmsfSecurityToken(NETWORK_ID, Constants.NF_TYPE_UDM, ACCESS_TOKEN,
                "Bearer", "nudm-uecm", now, now + 3600));
    }

    private static String registerEvent() {
        return Converter.valueAsString(SmsfRegistrationEvent.builder()
                .networkId(NETWORK_ID).supi(SUPI).action(RegistrationAction.REGISTER)
                .ueContext(new UeSmsContextData().supi(SUPI)
                        .gpsi("msisdn-8613915900077").accessType(AccessType._3_GPP_ACCESS))
                .timestamp(Instant.now().toEpochMilli()).build());
    }

    private static String deregisterEvent() {
        return Converter.valueAsString(SmsfRegistrationEvent.builder()
                .networkId(NETWORK_ID).supi(SUPI).action(RegistrationAction.DEREGISTER)
                .timestamp(Instant.now().toEpochMilli()).build());
    }

    // --- the whole path -------------------------------------------------------

    @Test
    @DisplayName("Kafka → consumer → UDM: one registration event crosses the whole chain")
    void registrationCrossesTheWholeChain() {
        udm.stubFor(put(urlPathMatching(".*/registrations/smsf-3gpp-access")).willReturn(aResponse()
                .withStatus(201).withHeader("content-type", "application/json").withBody("{}")));
        publishTokenToRedis();
        manager.start();

        // The consumer only starts once the token is really in Redis.
        await().atMost(Duration.ofSeconds(20))
                .until(() -> tokenCache.isAvailable(NETWORK_ID, Constants.NF_TYPE_UDM));

        SmsfE2EInfrastructure.produce(TOPIC, SUPI, registerEvent());

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                udm.verify(putRequestedFor(urlPathMatching(EXPECTED_URL))
                        .withHeader("authorization", equalTo("Bearer " + ACCESS_TOKEN))
                        .withRequestBody(containing("smsfInstanceId"))
                        .withRequestBody(containing("smsfSbiSupInd"))));
    }

    @Test
    @DisplayName("Kafka → consumer → UDM: a deregistration crosses the chain as a DELETE")
    void deregistrationCrossesTheWholeChain() {
        udm.stubFor(delete(urlPathMatching(".*/registrations/smsf-3gpp-access"))
                .willReturn(aResponse().withStatus(204)));
        publishTokenToRedis();
        manager.start();
        await().atMost(Duration.ofSeconds(20))
                .until(() -> tokenCache.isAvailable(NETWORK_ID, Constants.NF_TYPE_UDM));

        SmsfE2EInfrastructure.produce(TOPIC, SUPI, deregisterEvent());

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                udm.verify(deleteRequestedFor(urlPathMatching(EXPECTED_URL))
                        .withHeader("authorization", equalTo("Bearer " + ACCESS_TOKEN))));
    }

    @Test
    @DisplayName("with no token in Redis the consumer never starts, and the event waits in Kafka")
    void withoutTokenTheEventStaysInKafka() {
        udm.stubFor(put(urlPathMatching(".*")).willReturn(aResponse().withStatus(201).withBody("{}")));
        // Deliberately no token published.
        manager.start();

        SmsfE2EInfrastructure.produce(TOPIC, SUPI, registerEvent());

        // Nothing reaches the UDM...
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(6))
                .untilAsserted(() -> udm.verify(0, anyRequestedFor(anyUrl())));
        // ...and the record is still on the topic, readable by a fresh consumer group.
        assertThat(SmsfE2EInfrastructure.consumeAll(TOPIC, "assert-" + UUID.randomUUID(), Duration.ofSeconds(10)))
                .anyMatch(value -> value.contains(SUPI));
    }

    @Test
    @DisplayName("the consumer starts by itself when the token finally lands, and drains the backlog")
    void backlogIsDrainedWhenTheTokenArrives() {
        udm.stubFor(put(urlPathMatching(".*/registrations/smsf-3gpp-access")).willReturn(aResponse()
                .withStatus(201).withHeader("content-type", "application/json").withBody("{}")));
        manager.start();

        SmsfE2EInfrastructure.produce(TOPIC, SUPI, registerEvent());
        publishTokenToRedis();

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() ->
                udm.verify(putRequestedFor(urlPathMatching(EXPECTED_URL))));
    }
}
