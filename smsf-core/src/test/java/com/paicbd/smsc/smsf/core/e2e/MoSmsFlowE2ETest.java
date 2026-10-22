package com.paicbd.smsc.smsf.core.e2e;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.core.stack.CoreStackManager;
import com.paicbd.smsc.smsf.core.api.controller.MoSmsController;
import com.paicbd.smsc.smsf.core.api.service.MoSmsService;
import com.paicbd.smsc.smsf.common.processors.MessageTpduProcessor;
import com.paicbd.smsc.smsf.common.processors.MessageFactory;
import com.paicbd.smsc.smsf.core.processors.multipart.MultiPartsHandler;
import com.paicbd.smsc.utils.Converter;
import com.paicore.sba.stack.codec.EncodeTpduRequest;
import com.paicore.sba.stack.codec.MessageType;
import com.paicore.sba.stack.codec.Ss7SmsTpduCodec;
import com.paicore.sba.stack.mo.MultipartRelatedEncoder;
import com.paicore.sba.stack.openapi.model.RefToBinaryData;
import com.paicore.sba.stack.openapi.model.SmsRecordData;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MoSmsFlowE2ETest {

    private static final int NETWORK_ID = 11;
    private static final String SUPI = "imsi-460000203924736";
    private static final String INVALID_SUPI = "not-a-supi";
    private static final String DESTINATION = "50212345678";
    private static final String SOURCE_ADDR = "8613915900000";
    private static final String GPSI = "msisdn-" + SOURCE_ADDR;
    private static final int TTL = 30;

    private final Ss7SmsTpduCodec codec = new Ss7SmsTpduCodec();
    private final ConcurrentMap<String, String> scyllaBackedStore = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, BlockingQueue<String>> kafkaSentByTopic = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafkaTemplate = (KafkaTemplate<String, String>) mock(KafkaTemplate.class);
    private final ScyllaManager scyllaManager = mock(ScyllaManager.class);

    private WebTestClient client;

    @BeforeEach
    void setUp() {
        wireKafka();
        wireScyllaMultipartCache();

        MultiPartsHandler multiPartsHandler = new MultiPartsHandler(scyllaManager, kafkaTemplate, TTL, TTL + 5, 1_000);

        Http2Gateway gateway = new Http2Gateway();
        gateway.setNetworkId(NETWORK_ID);
        MessageFactory messageFactory = new MessageFactory(
                gateway, new ConcurrentHashMap<>(), new MessageTpduProcessor(new Ss7SmsTpduCodec()));

        CoreStackManager stackManager = mock(CoreStackManager.class);
        when(stackManager.getMessageFactory()).thenReturn(messageFactory);

        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        gatewayStacks.put(NETWORK_ID, stackManager);

        MoSmsService moSmsService = new MoSmsService(kafkaTemplate, multiPartsHandler, gatewayStacks, scyllaManager);
        MoSmsController controller = new MoSmsController(moSmsService);

        client = WebTestClient.bindToController(controller)
                .configureClient()
                .baseUrl("http://localhost")
                .build();
    }

    @Test
    @DisplayName("single-part MO → one MessageEvent published to pre.message.medium and 200 OK")
    void singlePartMoPublishesOne() {
        MultipartRelatedEncoder.Encoded encoded = singlePartMultipart("Hello world");

        putSendSms(NETWORK_ID, SUPI, encoded).expectStatus().isOk();

        List<String> sent = drainTopic(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC);
        assertEquals(1, sent.size());
        MessageEvent event = Converter.stringToObject(sent.get(0), MessageEvent.class);
        assertEquals(SUPI, event.getSource());
        assertEquals(Constants.ORIGIN_PROTOCOL_HTTP2, event.getOriginProtocol());
        assertEquals("Hello world", event.getShortMessage());
        assertTrue(scyllaBackedStore.isEmpty(), "Scylla parts store should be empty for single-part MO");
    }

    @Test
    @DisplayName("concatenated MO (2 parts, in order) → ONE combined MessageEvent published after part 2")
    void concatMoTwoPartsInOrderPublishesOnce() {
        int ref = 0xF2;
        MultipartRelatedEncoder.Encoded p1 = concatMultipart("hello ", ref, 2, 1);
        MultipartRelatedEncoder.Encoded p2 = concatMultipart("world", ref, 2, 2);

        putSendSms(NETWORK_ID, SUPI, p1).expectStatus().isOk();
        assertTrue(drainTopic(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC).isEmpty(),
                "no publish after part 1");
        putSendSms(NETWORK_ID, SUPI, p2).expectStatus().isOk();

        List<String> sent = drainTopic(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC);
        assertEquals(1, sent.size(), "exactly one combined MessageEvent should be published");
        MessageEvent combined = Converter.stringToObject(sent.get(0), MessageEvent.class);
        assertNotNull(combined.getMessageParts());
        assertEquals(2, combined.getMessageParts().size());
        assertEquals("hello ", combined.getMessageParts().get(0).getShortMessage());
        assertEquals("world", combined.getMessageParts().get(1).getShortMessage());
        assertTrue(scyllaBackedStore.isEmpty(), "Scylla parts store should be cleaned after completion");
    }

    @Test
    @DisplayName("concatenated MO with parts arriving out of order still publishes exactly once")
    void concatMoOutOfOrderPublishesOnce() {
        int ref = 0xAB;
        MultipartRelatedEncoder.Encoded p2 = concatMultipart("world", ref, 2, 2);
        MultipartRelatedEncoder.Encoded p1 = concatMultipart("hello ", ref, 2, 1);

        putSendSms(NETWORK_ID, SUPI, p2).expectStatus().isOk();
        putSendSms(NETWORK_ID, SUPI, p1).expectStatus().isOk();

        List<String> sent = drainTopic(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC);
        assertEquals(1, sent.size());
        MessageEvent combined = Converter.stringToObject(sent.get(0), MessageEvent.class);
        assertEquals(2, combined.getMessageParts().size());
    }

    @Test
    @DisplayName("duplicate segment sequence does not cause an extra publish")
    void concatMoDuplicateSegmentDoesNotDoublePublish() {
        int ref = 0x11;
        MultipartRelatedEncoder.Encoded p1 = concatMultipart("hi ", ref, 2, 1);
        MultipartRelatedEncoder.Encoded p2 = concatMultipart("there", ref, 2, 2);

        putSendSms(NETWORK_ID, SUPI, p1).expectStatus().isOk();
        putSendSms(NETWORK_ID, SUPI, p1).expectStatus().isOk();
        putSendSms(NETWORK_ID, SUPI, p2).expectStatus().isOk();

        List<String> sent = drainTopic(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC);
        assertEquals(1, sent.size(), "duplicate part 1 must not trigger extra Kafka publish");
    }

    @Test
    @DisplayName("orphan Scylla parts row is cleaned and the fresh part is treated as new")
    void orphanScyllaEntryIsCleaned() {
        int ref = 0x77;
        scyllaBackedStore.put(SUPI + "|" + ref, "stale-part");
        MultipartRelatedEncoder.Encoded p1 = concatMultipart("hello ", ref, 2, 1);

        putSendSms(NETWORK_ID, SUPI, p1).expectStatus().isOk();

        assertEquals("hello ", scyllaBackedStore.get(SUPI + "|" + ref),
                "stale entry must be replaced by the new part");
        assertTrue(drainTopic(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC).isEmpty(),
                "part 1 alone must not publish");
    }

    @Test
    @DisplayName("invalid SUPI → 400 problem+json with INVALID_SUPI cause and no Kafka publish")
    void invalidSupiRejects() {
        MultipartRelatedEncoder.Encoded encoded = singlePartMultipart("hi");

        putSendSms(NETWORK_ID, INVALID_SUPI, encoded)
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.cause").isEqualTo(Constants.CAUSE_INVALID_SUPI);

        assertTrue(drainTopic(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC).isEmpty());
    }

    @Test
    @DisplayName("networkId with no registered stack → 500 problem+json")
    void gatewayNotRegisteredReturnsInternalError() {
        MultipartRelatedEncoder.Encoded encoded = singlePartMultipart("hi");
        int unknownNetworkId = 999;

        putSendSms(unknownNetworkId, SUPI, encoded)
                .expectStatus().is5xxServerError();

        assertTrue(drainTopic(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC).isEmpty());
    }

    private WebTestClient.ResponseSpec putSendSms(int networkId, String supi, MultipartRelatedEncoder.Encoded body) {
        return client.post()
                .uri("/{nid}/ue-contexts/{supi}/sendsms", networkId, supi)
                .contentType(MediaType.valueOf(body.contentType()))
                .bodyValue(body.body())
                .exchange();
    }

    private MultipartRelatedEncoder.Encoded singlePartMultipart(String text) {
        byte[] tpdu = codec.encode(EncodeTpduRequest.builder()
                .messageType(MessageType.SMS_SUBMIT)
                .destinationAddress(DESTINATION)
                .destinationAddressTon(1)
                .destinationAddressNpi(1)
                .message(text)
                .dataCoding(0)
                .protocolId(0)
                .build());
        return MultipartRelatedEncoder.encode(smsRecordData(), tpdu);
    }

    private MultipartRelatedEncoder.Encoded concatMultipart(String text, int ref, int total, int seq) {
        byte[] udh = new byte[]{0x05, 0x00, 0x03, (byte) ref, (byte) total, (byte) seq};
        byte[] textBytes = text.getBytes(StandardCharsets.UTF_8);
        byte[] userData = new byte[udh.length + textBytes.length];
        System.arraycopy(udh, 0, userData, 0, udh.length);
        System.arraycopy(textBytes, 0, userData, udh.length, textBytes.length);

        byte[] tpdu = codec.encode(EncodeTpduRequest.builder()
                .messageType(MessageType.SMS_SUBMIT)
                .destinationAddress(DESTINATION)
                .destinationAddressTon(1)
                .destinationAddressNpi(1)
                .userData(userData)
                .userDataLength(userData.length)
                .userDataHeaderPresent(true)
                .dataCoding(4)
                .protocolId(0)
                .build());
        return MultipartRelatedEncoder.encode(smsRecordData(), tpdu);
    }

    private SmsRecordData smsRecordData() {
        return new SmsRecordData()
                .smsRecordId("rec-" + System.nanoTime())
                .gpsi(GPSI)
                .smsPayload(new RefToBinaryData().contentId("sms"));
    }

    private void wireKafka() {
        ProducerRecord<String, String> dummy = new ProducerRecord<>("t", "k", "v");
        RecordMetadata meta = new RecordMetadata(new TopicPartition("t", 0), 0L, 0, 0L, 0, 0);
        CompletableFuture<SendResult<String, String>> ok = CompletableFuture.completedFuture(new SendResult<>(dummy, meta));

        when(kafkaTemplate.send(anyString(), anyString())).thenAnswer(inv -> {
            String topic = inv.getArgument(0, String.class);
            String value = inv.getArgument(1, String.class);
            kafkaSentByTopic.computeIfAbsent(topic, t -> new LinkedBlockingQueue<>()).add(value);
            return ok;
        });
    }

    private void wireScyllaMultipartCache() {
        when(scyllaManager.getSmsfMessagePartsByKey(anyString()))
                .thenAnswer(inv -> scyllaBackedStore.get(inv.getArgument(0, String.class)));
        doAnswer(inv -> {
            scyllaBackedStore.put(inv.getArgument(0, String.class), inv.getArgument(1, String.class));
            return null;
        }).when(scyllaManager).insertIntoSmsfMessageParts(anyString(), anyString(), anyInt());
        doAnswer(inv -> {
            scyllaBackedStore.remove(inv.getArgument(0, String.class));
            return null;
        }).when(scyllaManager).deleteSmsfMessagePartsByKey(anyString());
    }

    private List<String> drainTopic(String topic) {
        BlockingQueue<String> queue = kafkaSentByTopic.getOrDefault(topic, new LinkedBlockingQueue<>());
        return queue.stream().toList();
    }
}
