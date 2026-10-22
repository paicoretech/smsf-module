package com.paicbd.smsc.smsf.mt.functional;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenCache;
import com.paicbd.smsc.smsf.common.utils.Http2GatewayCreator;
import com.paicbd.smsc.smsf.mt.config.AppProperties;
import com.paicbd.smsc.smsf.mt.http2.Http2ClientConnection;
import com.paicbd.smsc.smsf.mt.stack.MtStackManager;
import com.paicbd.smsc.kafka.KafkaConsumerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.kafka.core.KafkaTemplate;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

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
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.when;

/**
 * Functional test of the MT delivery: a real {@code MessageEvent} driven through the module's
 * processor, encoded and sent over real HTTP/2 to a simulated AMF with the bearer the token cache
 * holds.
 *
 * <p>Nothing between {@code Http2ClientConnection} and the wire is mocked — the Netty client
 * connects, negotiates h2c and sends the multipart/related payload. Kafka, ScyllaDB and Redis are
 * stubbed at their boundaries.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AmfDeliveryFunctionalTest {

    private static final int NETWORK_ID = 10;
    private static final String BEARER = "Bearer amf-access-token";

    @Mock
    private AppProperties appProperties;

    @Mock
    private KafkaConsumerFactory kafkaConsumerFactory;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private ScyllaManager scyllaManager;

    @Mock
    private SmsfTokenCache tokenCache;

    private WireMockServer amf;
    private int amfPort;
    private Http2Gateway gateway;
    private MtStackManager manager;

    @BeforeEach
    void setUp() {
        amf = new WireMockServer(wireMockConfig().dynamicPort());
        amf.start();
        amfPort = amf.port();

        // The gateway host is the full sendsms URL template: the client takes host:port from it and
        // the request path is resolved from it, substituting {supi} with the destination.
        gateway = Http2GatewayCreator.buildHttp2Gateway("smsf-gw", NETWORK_ID,
                "http://localhost:" + amfPort + "/nsmsf-sms/v2/ue-contexts/{supi}/sendsms", ENABLED);
        gateway.getOutboundSecurity().setOauth2Enabled(true);
        // Plain h2c against the simulated AMF; the mTLS path has its own coverage.
        gateway.getOutboundSecurity().setMtlsEnabled(false);

        when(appProperties.getOutboundTokenRefreshMarginSeconds()).thenReturn(30L);
        when(appProperties.getTokenCheckIntervalSeconds()).thenReturn(10L);
        when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        when(tokenCache.authorizationHeader(NETWORK_ID, Constants.NF_TYPE_AMF))
                .thenReturn(Optional.of(BEARER));

        manager = new MtStackManager(gateway, appProperties, kafkaConsumerFactory, kafkaTemplate,
                scyllaManager, tokenCache, new ConcurrentHashMap<>());
    }

    @AfterEach
    void tearDown() {
        amf.stop();
    }

    /** Connects the real AMF client and marks the stack started, without the Kafka consumers. */
    private void connectClient() throws Exception {
        Http2ClientConnection connection = manager.getClientConnection();
        connection.connect();
        assertThat(connection.isConnected())
                .as("the HTTP/2 client should have connected to the simulated AMF")
                .isTrue();
        setStarted();
    }

    /** Same shape the routing module publishes; the CDR path needs the identity fields populated. */
    private MessageEvent mtEvent() {
        String messageId = "1720569600000-98765432101";
        MessageEvent event = MessageEvent.builder()
                .id(messageId)
                .messageId(messageId)
                .parentId(messageId)
                .sourceAddr("12025550100")
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .destinationAddr("5215512345678")
                .destAddrTon(1)
                .destAddrNpi(1)
                .destination("imsi-5215512345678")
                .originProtocol("SMPP")
                .originNetworkId(1)
                .originNetworkName("origin-gw")
                .originNetworkType("GW")
                .destNetworkId(NETWORK_ID)
                .destNetworkName("smsf-gw")
                .destNetworkType("GW")
                .shortMessage("functional test message")
                .build();
        event.setDataCoding(0);
        return event;
    }

    private void deliver(MessageEvent event) throws Exception {
        Method method = manager.getMessageProcessing().getClass()
                .getMethod("processOutgoingRequest", MessageEvent.class);
        method.invoke(manager.getMessageProcessing(), event);
    }

    // --- real traffic ---------------------------------------------------------

    @Test
    @DisplayName("an MT message becomes a real POST to the AMF carrying the cached bearer")
    void mtMessageReachesTheAmf() throws Exception {
        amf.stubFor(post(urlPathMatching(".*")).willReturn(aResponse()
                .withStatus(200).withHeader("content-type", "application/json").withBody("{}")));
        connectClient();

        deliver(mtEvent());

        await().atMost(java.time.Duration.ofSeconds(10)).untilAsserted(() ->
                amf.verify(postRequestedFor(urlPathMatching(".*"))
                        .withHeader("authorization", equalTo(BEARER))));
    }

    @Test
    @DisplayName("the payload really is multipart/related, as the SBI MT delivery requires")
    void payloadIsMultipartRelated() throws Exception {
        amf.stubFor(post(urlPathMatching(".*")).willReturn(aResponse()
                .withStatus(200).withHeader("content-type", "application/json").withBody("{}")));
        connectClient();

        deliver(mtEvent());

        await().atMost(java.time.Duration.ofSeconds(10)).untilAsserted(() ->
                amf.verify(postRequestedFor(urlPathMatching(".*"))
                        .withHeader("content-type", containing("multipart/related"))));
    }

    @Test
    @DisplayName("an AMF rejection is processed without breaking the stack")
    void amfRejectionIsHandled() throws Exception {
        amf.stubFor(post(urlPathMatching(".*")).willReturn(aResponse()
                .withStatus(404).withHeader("content-type", "application/problem+json")
                .withBody("{\"cause\":\"USER_NOT_FOUND\"}")));
        connectClient();

        deliver(mtEvent());

        await().atMost(java.time.Duration.ofSeconds(10)).untilAsserted(() ->
                amf.verify(postRequestedFor(urlPathMatching(".*"))));
        assertThat(manager.getClientConnection().isConnected()).isTrue();
    }

    @Test
    @DisplayName("an AMF that is down does not propagate the failure out of the processor")
    void amfDownIsSwallowed() throws Exception {
        connectClient();
        amf.stop();

        deliver(mtEvent());

        assertThat(manager.isStarted()).isTrue();
    }

    @Test
    @DisplayName("with no cached token the request still goes out unauthenticated only if OAuth2 is off")
    void withoutTokenNoAuthorizationHeaderIsSent() throws Exception {
        when(tokenCache.authorizationHeader(NETWORK_ID, Constants.NF_TYPE_AMF)).thenReturn(Optional.empty());
        amf.stubFor(post(urlPathMatching(".*")).willReturn(aResponse()
                .withStatus(200).withHeader("content-type", "application/json").withBody("{}")));
        connectClient();

        deliver(mtEvent());

        // The consumers are what gate on the token; the processor itself simply sends no bearer.
        await().atMost(java.time.Duration.ofSeconds(10)).untilAsserted(() ->
                amf.verify(postRequestedFor(urlPathMatching(".*"))));
        assertThat(amf.findAll(postRequestedFor(urlPathMatching(".*"))).getFirst()
                .getHeaders().getHeader("authorization").isPresent()).isFalse();
    }

    @Test
    @DisplayName("the client is not connected: nothing is sent and the processor survives")
    void notConnectedGeneratesNoTraffic() throws Exception {
        amf.stubFor(post(urlPathMatching(".*")).willReturn(aResponse().withStatus(200).withBody("{}")));
        setStarted();

        deliver(mtEvent());

        amf.verify(0, anyRequestedFor(anyUrl()));
    }

    // --- helpers --------------------------------------------------------------

    private void setStarted() throws Exception {
        Field field = MtStackManager.class.getDeclaredField("started");
        field.setAccessible(true);
        ((AtomicBoolean) field.get(manager)).set(true);
    }
}
