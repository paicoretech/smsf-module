package com.paicbd.smsc.smsf.mt.e2e;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.kafka.KafkaConsumerFactory;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.common.e2e.SmsfE2EInfrastructure;
import com.paicbd.smsc.smsf.common.redis.SmsfSecurityToken;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenCache;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenRedisStore;
import com.paicbd.smsc.smsf.common.utils.Http2GatewayCreator;
import com.paicbd.smsc.smsf.mt.config.AppProperties;
import com.paicbd.smsc.smsf.mt.stack.MtStackManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.RedisManager;
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
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.paicbd.smsc.smsf.common.constants.Constants.ENABLED;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * End-to-end MT delivery, on real infrastructure.
 *
 * <p>The message is produced to a real Kafka broker, the module's own priority consumer picks it up,
 * the AMF token is read from a real Redis, and the delivery leaves as real HTTP/2 to an AMF
 * listening on a real port. Every hop crosses the wire.</p>
 *
 * <p>ScyllaDB is stubbed: a Scylla container needs more AIO capacity than a developer host running
 * other Scylla nodes can give it, the same reason {@code smsc-management-be} mocks its
 * {@code ScyllaManager} in the integration tests.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MtDeliveryE2ETest {

    private static final int NETWORK_ID = 99;
    private static final String HIGH_TOPIC = NETWORK_ID + ".smsf.sms.high";
    private static final String MEDIUM_TOPIC = NETWORK_ID + ".smsf.sms.medium";
    private static final String LOW_TOPIC = NETWORK_ID + ".smsf.sms.low";
    private static final String ACCESS_TOKEN = "e2e-amf-token";

    @Mock
    private AppProperties appProperties;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private ScyllaManager scyllaManager;

    private WireMockServer amf;
    private int amfPort;
    private Http2Gateway gateway;
    private MtStackManager manager;
    private SmsfTokenCache tokenCache;
    private SmsfTokenRedisStore tokenStore;

    @BeforeAll
    static void bootInfrastructure() {
        SmsfE2EInfrastructure.start();
        SmsfE2EInfrastructure.createTopics(HIGH_TOPIC, MEDIUM_TOPIC, LOW_TOPIC);
    }

    @BeforeEach
    void setUp() {
        amf = new WireMockServer(wireMockConfig().dynamicPort());
        amf.start();
        amfPort = amf.port();
        System.out.printf("  AMF simulator 127.0.0.1:%d%n", amfPort);

        gateway = Http2GatewayCreator.buildHttp2Gateway("smsf-gw", NETWORK_ID,
                "http://localhost:" + amf.port() + "/nsmsf-sms/v2/ue-contexts/{supi}/sendsms", ENABLED);
        gateway.getOutboundSecurity().setOauth2Enabled(true);
        gateway.getOutboundSecurity().setMtlsEnabled(false);
        // One consumer per priority, so the poll loop draws from all three.
        gateway.setMessagesPerSecond(10);
        gateway.setMessagesPerSecondHigh(10);
        gateway.setMessagesPerSecondMedium(10);
        gateway.setMessagesPerSecondLow(10);

        when(appProperties.getKafkaBootstrapServers()).thenReturn(SmsfE2EInfrastructure.kafkaBootstrapServers());
        when(appProperties.getOutboundTokenRefreshMarginSeconds()).thenReturn(30L);
        when(appProperties.getTokenCheckIntervalSeconds()).thenReturn(1L);
        // The custom consumer replays whatever it buffered on its last shutdown; nothing here.
        when(scyllaManager.getPendingMessagesByTopic(anyString())).thenReturn(Collections.emptyList());

        RedisManager redisManager = SmsfE2EInfrastructure.redisManager();
        tokenStore = new SmsfTokenRedisStore(redisManager);
        tokenCache = new SmsfTokenCache(tokenStore, 30L, 1L, 50L);

        manager = new MtStackManager(gateway, appProperties, new KafkaConsumerFactory(), kafkaTemplate,
                scyllaManager, tokenCache, new ConcurrentHashMap<>());
    }

    @AfterEach
    void tearDown() {
        if (manager.isStarted()) {
            manager.stop();
        }
        tokenCache.close();
        tokenStore.evict(NETWORK_ID, Constants.NF_TYPE_AMF);
        amf.stop();
    }

    private void publishTokenToRedis() {
        long now = Instant.now().getEpochSecond();
        tokenStore.publish(new SmsfSecurityToken(NETWORK_ID, Constants.NF_TYPE_AMF, ACCESS_TOKEN,
                "Bearer", "nsmsf-sms", now, now + 3600));
    }

    private static String mtMessage(String messageId, String destination) {
        MessageEvent event = MessageEvent.builder()
                .id(messageId)
                .messageId(messageId)
                .parentId(messageId)
                .sourceAddr("12025550100")
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .destinationAddr(destination)
                .destAddrTon(1)
                .destAddrNpi(1)
                .destination("imsi-" + destination)
                .originProtocol("SMPP")
                .originNetworkId(1)
                .originNetworkName("origin-gw")
                .originNetworkType("GW")
                .destNetworkId(NETWORK_ID)
                .destNetworkName("smsf-gw")
                .destNetworkType("GW")
                .shortMessage("e2e MT message")
                .build();
        event.setDataCoding(0);
        return Converter.valueAsString(event);
    }

    // --- the whole path -------------------------------------------------------

    @Test
    @DisplayName("Kafka → priority consumer → AMF: one MT message crosses the whole chain")
    void mtMessageCrossesTheWholeChain() {
        amf.stubFor(post(urlPathMatching(".*")).willReturn(aResponse()
                .withStatus(200).withHeader("content-type", "application/json").withBody("{}")));
        publishTokenToRedis();
        manager.start();
        await().atMost(Duration.ofSeconds(20))
                .until(() -> tokenCache.isAvailable(NETWORK_ID, Constants.NF_TYPE_AMF));

        SmsfE2EInfrastructure.produce(MEDIUM_TOPIC, "1", mtMessage("1783981200001-86234715900001", "5215512345678"));

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() ->
                amf.verify(postRequestedFor(urlPathMatching(".*/sendsms"))
                        .withHeader("authorization", equalTo("Bearer " + ACCESS_TOKEN))
                        .withHeader("content-type", containing("multipart/related"))));
    }

    @Test
    @DisplayName("the high-priority topic reaches the AMF too, over its own consumer")
    void highPriorityMessageCrossesTheWholeChain() {
        amf.stubFor(post(urlPathMatching(".*")).willReturn(aResponse()
                .withStatus(200).withHeader("content-type", "application/json").withBody("{}")));
        publishTokenToRedis();
        manager.start();
        await().atMost(Duration.ofSeconds(20))
                .until(() -> tokenCache.isAvailable(NETWORK_ID, Constants.NF_TYPE_AMF));

        SmsfE2EInfrastructure.produce(HIGH_TOPIC, "2", mtMessage("1783981200002-86234715900002", "5215512345679"));

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() ->
                amf.verify(postRequestedFor(urlPathMatching(".*/sendsms"))
                        .withHeader("authorization", equalTo("Bearer " + ACCESS_TOKEN))));
    }

    @Test
    @DisplayName("with no token in Redis the consumers stay paused and nothing reaches the AMF")
    void withoutTokenNothingIsDelivered() {
        amf.stubFor(post(urlPathMatching(".*")).willReturn(aResponse().withStatus(200).withBody("{}")));
        // Deliberately no token published.
        manager.start();

        SmsfE2EInfrastructure.produce(LOW_TOPIC, "3", mtMessage("1783981200003-86234715900003", "5215512345680"));

        await().during(Duration.ofSeconds(5)).atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> amf.verify(0, anyRequestedFor(anyUrl())));
    }
}
