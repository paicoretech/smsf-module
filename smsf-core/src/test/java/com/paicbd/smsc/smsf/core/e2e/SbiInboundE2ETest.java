package com.paicbd.smsc.smsf.core.e2e;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.common.e2e.SmsfE2EInfrastructure;
import com.paicbd.smsc.smsf.common.utils.Http2GatewayCreator;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.RedisManager;
import com.paicore.sba.stack.codec.EncodeTpduRequest;
import com.paicore.sba.stack.codec.MessageType;
import com.paicore.sba.stack.codec.Ss7SmsTpduCodec;
import com.paicbd.smsc.smsf.common.event.SmsfRegistrationEvent;
import com.paicbd.smsc.smsf.common.event.RegistrationAction;
import com.paicore.sba.stack.mo.MultipartRelatedEncoder;
import com.paicore.sba.stack.openapi.model.AccessType;
import com.paicore.sba.stack.openapi.model.RefToBinaryData;
import com.paicore.sba.stack.openapi.model.SmsRecordData;
import com.paicore.sba.stack.openapi.model.UeSmsContextData;
import com.paicore.sba.stack.transport.http2.NettyHttp2Client;
import com.paicore.sba.stack.transport.http2.config.ConnectionPoolSettings;
import com.paicore.sba.stack.transport.http2.config.Http2ClientSettings;
import com.paicore.sba.stack.transport.http2.config.TlsSettings;
import com.paicore.sba.stack.transport.http2.dto.Http2Request;
import com.paicore.sba.stack.transport.http2.dto.Http2Response;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end of the SBI the AMF talks to: the real HTTP/2 listener of {@code smsf-core}, reached by
 * a real HTTP/2 client over loopback.
 *
 * <p>This is the leg the other end-to-end tests do not cover — they start from Kafka or from the
 * NRF. Here the whole application context comes up on a real port, an AMF-shaped client sends a UE
 * registration and an MO message, and the effects are asserted on the real Kafka broker.</p>
 *
 * <p>The listener uses an operating-system-assigned port so concurrent CI builds cannot collide.</p>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SbiInboundE2ETest {

    private static final int NETWORK_ID = 66;
    private static final String SUPI = "imsi-460000000000066";
    private static final String DESTINATION = "5215512345678";
    private static final String BASE_PATH = "/nsmsf-sms/v2";
    private static final String REGISTER_TOPIC = NETWORK_ID + KafkaTopicsConstants.SMSF_UDM_REGISTER_TOPIC_SUFFIX;

    @LocalServerPort
    private int sbiPort;
    private static final Ss7SmsTpduCodec CODEC = new Ss7SmsTpduCodec();

    /** ScyllaDB is the one dependency stubbed; a Scylla container needs more AIO than the host has. */
    @MockitoBean
    private ScyllaManager scyllaManager;

    /**
     * The stub has to keep state, not just answer calls: the SBI now decides whether a DELETE is a
     * real deregistration by asking whether the context is stored. A memoryless mock would report
     * every SUPI as absent and the deregistration would never be published — which is precisely the
     * production behaviour for a SUPI that was never registered, so the test would pass for the
     * wrong reason. Static because {@code @MockitoBean} resets the mock between test methods, while
     * the registration in {@code @Order(1)} must still be visible to the deletion in
     * {@code @Order(2)}.
     */
    private static final Set<String> STORED_CONTEXTS = ConcurrentHashMap.newKeySet();

    @BeforeEach
    void stubScyllaAsAStore() {
        doAnswer(invocation -> {
            STORED_CONTEXTS.add(invocation.getArgument(0, UeSmsContextData.class).getSupi());
            return null;
        }).when(scyllaManager).upsertSmsfUeSmsContext(any());
        doAnswer(invocation -> {
            STORED_CONTEXTS.remove(invocation.getArgument(0, String.class));
            return null;
        }).when(scyllaManager).deleteSmsfUeSmsContextBySupi(anyString());
        when(scyllaManager.existsSmsfUeSmsContextBySupi(anyString()))
                .thenAnswer(invocation -> STORED_CONTEXTS.contains(invocation.getArgument(0, String.class)));
    }

    @BeforeAll
    static void bootInfrastructure() {
        SmsfE2EInfrastructure.start();
        SmsfE2EInfrastructure.createTopics(REGISTER_TOPIC, KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC);
        seedGateway();
    }

    /**
     * Puts the gateway in the real Redis before the context starts: the module reads its registry
     * from there, and the MO flow resolves its message factory per gateway.
     */
    private static void seedGateway() {
        Http2Gateway gateway = Http2GatewayCreator.buildHttp2Gateway(
                "smsf-e2e-gw", NETWORK_ID, "http://127.0.0.1:1/unused", Constants.ENABLED);
        gateway.setNfInstanceId(UUID.fromString("22222222-3333-4444-5555-666666666666"));
        // No NRF and no OAuth2: this test is about the inbound leg, and the outbound ones have
        // their own coverage. Without a register path and with OAuth2 off the stack starts clean.
        gateway.getNrfSecurity().setNrfRegisterPath(null);
        gateway.getNrfSecurity().setApiRoot(null);
        gateway.getOutboundSecurity().setOauth2Enabled(false);

        RedisManager redisManager = SmsfE2EInfrastructure.redisManager();
        redisManager.hset(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME,
                String.valueOf(NETWORK_ID), Converter.valueAsString(gateway));
    }

    @DynamicPropertySource
    static void applicationProperties(DynamicPropertyRegistry registry) {
        SmsfE2EInfrastructure.start();
        registry.add("spring.kafka.bootstrap-servers", SmsfE2EInfrastructure::kafkaBootstrapServers);
        registry.add("redis.cluster.nodes", SmsfE2EInfrastructure::redisHostAndPort);
        registry.add("redis.standalone.enabled", () -> "true");
        // Cleartext HTTP/2 so the capture is readable and no per-gateway server certificate is needed.
        registry.add("smsf.tls.enabled", () -> "false");
        // The inbound token validation has its own tests; here the traffic is what matters.
        registry.add("smsf.security.enabled", () -> "false");
        registry.add("websocket.server.enabled", () -> "false");
    }

    /** An HTTP/2 client standing in for the AMF, using the same stack the platform uses. */
    private NettyHttp2Client amfClient() {
        NettyHttp2Client client = new NettyHttp2Client(Http2ClientSettings.builder()
                .host("127.0.0.1")
                .port(sbiPort)
                .tls(TlsSettings.builder().enabled(false).verifyHostname(false).build())
                .pool(ConnectionPoolSettings.builder()
                        .connectTimeoutMillis(5000).readTimeoutMillis(10000).idleTimeoutSeconds(30)
                        .build())
                .build());
        client.connect();
        return client;
    }

    private Http2Response send(String method, String path, String contentType, byte[] body)
            throws Exception {
        NettyHttp2Client client = amfClient();
        try {
            Map<String, String> headers = new HashMap<>();
            if (contentType != null) {
                headers.put("content-type", contentType);
            }
            headers.put("accept", "application/json");
            return client.send(Http2Request.builder()
                    .method(method).path(BASE_PATH + path).headers(headers).body(body)
                    .build()).get(15, TimeUnit.SECONDS);
        } finally {
            client.close();
        }
    }

    // --- UE registration ------------------------------------------------------

    @Test
    @Order(1)
    @DisplayName("AMF → SBI: a UE registration over real HTTP/2 answers 201 and publishes to Kafka")
    void registrationOverRealHttp2() throws Exception {
        UeSmsContextData body = new UeSmsContextData().supi(SUPI)
                .gpsi("msisdn-8613915900066").accessType(AccessType._3_GPP_ACCESS);

        Http2Response response = send("PUT", "/" + NETWORK_ID + "/ue-contexts/" + SUPI,
                "application/json", Converter.valueAsString(body).getBytes(StandardCharsets.UTF_8));

        assertThat(response.getStatus()).isEqualTo(201);

        List<String> records = await().atMost(Duration.ofSeconds(30))
                .until(() -> SmsfE2EInfrastructure.consumeAll(
                                REGISTER_TOPIC, "assert-" + UUID.randomUUID(), Duration.ofSeconds(5)),
                        list -> list.stream().anyMatch(v -> v.contains(SUPI)));

        SmsfRegistrationEvent event = records.stream()
                .map(v -> Converter.stringToObject(v, SmsfRegistrationEvent.class))
                .filter(e -> SUPI.equals(e.getSupi()))
                .findFirst().orElseThrow();
        assertThat(event.getAction()).isEqualTo(RegistrationAction.REGISTER);
        assertThat(event.getNetworkId()).isEqualTo(NETWORK_ID);
    }

    @Test
    @Order(2)
    @DisplayName("AMF → SBI: a deregistration over real HTTP/2 answers 204 and publishes to Kafka")
    void deregistrationOverRealHttp2() throws Exception {
        Http2Response response = send("DELETE", "/" + NETWORK_ID + "/ue-contexts/" + SUPI, null, null);

        assertThat(response.getStatus()).isEqualTo(204);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> SmsfE2EInfrastructure.consumeAll(
                                REGISTER_TOPIC, "assert-" + UUID.randomUUID(), Duration.ofSeconds(5)),
                        list -> list.stream()
                                .map(v -> Converter.stringToObject(v, SmsfRegistrationEvent.class))
                                .anyMatch(e -> RegistrationAction.DEREGISTER.equals(e.getAction())));
    }

    @Test
    @Order(3)
    @DisplayName("AMF → SBI: deleting a SUPI that was never registered answers 204 but publishes nothing")
    void unknownSupiDeletionIsSilent() throws Exception {
        String unknownSupi = "imsi-460000000000199";

        Http2Response response = send("DELETE", "/" + NETWORK_ID + "/ue-contexts/" + unknownSupi, null, null);

        // Idempotent for the AMF: retrying a DELETE must not turn into an error.
        assertThat(response.getStatus()).isEqualTo(204);

        // But nothing is registered at the UDM, so no DEREGISTER is worth forwarding.
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(SmsfE2EInfrastructure.consumeAll(
                        REGISTER_TOPIC, "assert-" + UUID.randomUUID(), Duration.ofSeconds(2)))
                        .noneMatch(value -> value.contains(unknownSupi)));
    }

    @Test
    @Order(4)
    @DisplayName("AMF → SBI: an invalid SUPI is rejected at the API layer, without reaching Kafka")
    void invalidSupiIsRejected() throws Exception {
        Http2Response response = send("PUT", "/" + NETWORK_ID + "/ue-contexts/not-a-supi",
                "application/json", "{\"supi\":\"not-a-supi\"}".getBytes(StandardCharsets.UTF_8));

        assertThat(response.getStatus()).isEqualTo(400);
    }

    // --- MO -------------------------------------------------------------------

    @Test
    @Order(5)
    @DisplayName("AMF → SBI: an MO message over real HTTP/2 is decoded and published to the routing topic")
    void moMessageOverRealHttp2() throws Exception {
        MultipartRelatedEncoder.Encoded encoded = singlePartMultipart("Hello from the AMF");

        Http2Response response = send("POST",
                "/" + NETWORK_ID + "/ue-contexts/" + SUPI + "/sendsms",
                encoded.contentType(), encoded.body());

        assertThat(response.getStatus()).isEqualTo(200);

        List<String> records = await().atMost(Duration.ofSeconds(30))
                .until(() -> SmsfE2EInfrastructure.consumeAll(
                                KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC,
                                "assert-" + UUID.randomUUID(), Duration.ofSeconds(5)),
                        list -> list.stream().anyMatch(v -> v.contains("Hello from the AMF")));

        MessageEvent event = records.stream()
                .map(v -> Converter.stringToObject(v, MessageEvent.class))
                .filter(e -> "Hello from the AMF".equals(e.getShortMessage()))
                .findFirst().orElseThrow();
        assertThat(event.getSource()).isEqualTo(SUPI);
        assertThat(event.getOriginProtocol()).isEqualTo(Constants.ORIGIN_PROTOCOL_HTTP2);
    }

    private static MultipartRelatedEncoder.Encoded singlePartMultipart(String text) {
        byte[] tpdu = CODEC.encode(EncodeTpduRequest.builder()
                .messageType(MessageType.SMS_SUBMIT)
                .destinationAddress(DESTINATION)
                .destinationAddressTon(1)
                .destinationAddressNpi(1)
                .message(text)
                .dataCoding(0)
                .protocolId(0)
                .build());
        SmsRecordData smsRecord = new SmsRecordData();
        smsRecord.setSmsRecordId(UUID.randomUUID().toString());
        smsRecord.setSmsPayload(new RefToBinaryData().contentId("tpdu"));
        return MultipartRelatedEncoder.encode(smsRecord, tpdu);
    }
}
