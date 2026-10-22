package com.paicbd.smsc.smsf.core.api.service;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.smsf.common.processors.MessageFactory;
import com.paicbd.smsc.smsf.common.processors.MessageTpduProcessor;
import com.paicbd.smsc.smsf.common.utils.Http2GatewayCreator;
import com.paicbd.smsc.smsf.core.stack.CoreStackManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.RegEventState;
import com.paicbd.smsc.utils.RequestDelivery;
import com.paicore.sba.stack.codec.Ss7SmsTpduCodec;
import com.paicore.sba.stack.openapi.model.N1N2MessageTransferCause;
import com.paicore.sba.stack.openapi.model.N1N2MsgTxfrFailureNotification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.paicbd.smsc.scylla.ScyllaTablesConstants.SMSF_SUBMIT_SM_RESULT_TABLE;
import static com.paicbd.smsc.smsf.common.constants.Constants.ENABLED;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class N1N2FailureServiceTest {

    private static final int NETWORK_ID = 7;
    private static final String MESSAGE_ID = "1783981200123-86234715900123";
    private static final String SUPI = "imsi-5215512345678";

    private ScyllaManager scyllaManager;
    private KafkaTemplate<String, String> kafkaTemplate;
    private N1N2FailureService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        scyllaManager = mock(ScyllaManager.class);
        kafkaTemplate = mock(KafkaTemplate.class);

        Http2Gateway gateway = Http2GatewayCreator.buildHttp2Gateway(
                "smsf-gw", NETWORK_ID, "http://127.0.0.1/namf-comm/v1/ue-contexts/{supi}/n1-n2-messages", ENABLED);
        CoreStackManager stack = mock(CoreStackManager.class);
        when(stack.getGateway()).thenReturn(gateway);
        when(stack.getMessageFactory()).thenReturn(new MessageFactory(gateway, new ConcurrentHashMap<>(),
                new MessageTpduProcessor(new Ss7SmsTpduCodec())));

        ConcurrentMap<Integer, CoreStackManager> stacks = new ConcurrentHashMap<>();
        stacks.put(NETWORK_ID, stack);


        service = new N1N2FailureService(scyllaManager, kafkaTemplate, stacks);
    }

    @Test
    @DisplayName("an absence cause deactivates the subscriber and closes the delivery with a receipt")
    void absenceCauseClosesTheDelivery() {
        storedDelivery(MESSAGE_ID, true);
        claimWon(MESSAGE_ID);

        service.processFailureNotification(NETWORK_ID, MESSAGE_ID, notification(N1N2MessageTransferCause.UE_NOT_RESPONDING));

        verify(scyllaManager).updateSmsfRegEventState(SUPI, RegEventState.INACTIVE.name());
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());
    }

    @Test
    @DisplayName("a cause that is not absence closes the delivery without touching the subscriber")
    void otherCausesDoNotDeactivate() {
        storedDelivery(MESSAGE_ID, true);
        claimWon(MESSAGE_ID);

        service.processFailureNotification(NETWORK_ID, MESSAGE_ID, notification(N1N2MessageTransferCause.AN_NOT_RESPONDING));

        verify(scyllaManager, never()).updateSmsfRegEventState(anyString(), anyString());
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());
    }

    @Test
    @DisplayName("a notification is never retried: the AMF already gave up on the delivery")
    void theDeliveryIsNeverRetried() {
        MessageEvent retryable = event(MESSAGE_ID, true);
        retryable.setValidityPeriod(3600);
        retryable.setSmscMessagePriority("MEDIUM");
        when(scyllaManager.selectFromTable(SMSF_SUBMIT_SM_RESULT_TABLE, MESSAGE_ID))
                .thenReturn(Converter.valueAsString(retryable));
        claimWon(MESSAGE_ID);

        service.processFailureNotification(NETWORK_ID, MESSAGE_ID, notification(N1N2MessageTransferCause.AN_NOT_RESPONDING));

        verify(kafkaTemplate, never()).send(eq("retries.sms.medium"), anyString());
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());
    }

    @Test
    @DisplayName("a delivery the timer already closed is left alone, so there is no second CDR or receipt")
    void alreadyClosedDeliveryIsLeftAlone() {
        storedDelivery(MESSAGE_ID, true);
        when(scyllaManager.deleteFromTableIfExists(SMSF_SUBMIT_SM_RESULT_TABLE, MESSAGE_ID)).thenReturn(false);

        service.processFailureNotification(NETWORK_ID, MESSAGE_ID, notification(N1N2MessageTransferCause.UE_NOT_RESPONDING));

        verify(scyllaManager, never()).updateSmsfRegEventState(anyString(), anyString());
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    @DisplayName("a notification for a delivery with no correlation changes nothing")
    void withoutCorrelationNothingHappens() {
        when(scyllaManager.selectFromTable(SMSF_SUBMIT_SM_RESULT_TABLE, MESSAGE_ID)).thenReturn(null);

        service.processFailureNotification(NETWORK_ID, MESSAGE_ID, notification(N1N2MessageTransferCause.UE_NOT_RESPONDING));

        verify(scyllaManager, never()).deleteFromTableIfExists(anyString(), anyString());
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    @DisplayName("a message that asked for no receipt is still closed, only without the DLR")
    void withoutRequestedDlrOnlyTheCdrIsRaised() {
        storedDelivery(MESSAGE_ID, false);
        claimWon(MESSAGE_ID);

        service.processFailureNotification(NETWORK_ID, MESSAGE_ID, notification(N1N2MessageTransferCause.UE_NOT_RESPONDING));

        verify(scyllaManager).updateSmsfRegEventState(SUPI, RegEventState.INACTIVE.name());
        verify(kafkaTemplate, never()).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());
    }

    @Test
    @DisplayName("a notification for a gateway that is not registered surfaces, so the AMF retries it")
    void unknownGatewaySurfaces() {
        var unknownGateway = notification(N1N2MessageTransferCause.UE_NOT_RESPONDING);

        assertThrows(IllegalStateException.class,
                () -> service.processFailureNotification(999, MESSAGE_ID, unknownGateway));
    }

    private void storedDelivery(String messageId, boolean requestDlr) {
        when(scyllaManager.selectFromTable(SMSF_SUBMIT_SM_RESULT_TABLE, messageId))
                .thenReturn(Converter.valueAsString(event(messageId, requestDlr)));
    }

    private void claimWon(String messageId) {
        when(scyllaManager.deleteFromTableIfExists(SMSF_SUBMIT_SM_RESULT_TABLE, messageId)).thenReturn(true);
    }

    private MessageEvent event(String messageId, boolean requestDlr) {
        MessageEvent event = MessageEvent.builder()
                .id(messageId)
                .messageId(messageId)
                .parentId(messageId)
                .sourceAddr("12025550100")
                .destinationAddr("5215512345678")
                .destination(SUPI)
                .originProtocol("SMPP")
                .originNetworkId(1)
                .destNetworkId(NETWORK_ID)
                .shortMessage("X")
                .build();
        event.setDataCoding(0);
        event.setRegisteredDelivery(requestDlr
                ? RequestDelivery.REQUEST_DLR.getValue()
                : RequestDelivery.NON_REQUEST_DLR.getValue());
        return event;
    }

    private N1N2MsgTxfrFailureNotification notification(N1N2MessageTransferCause cause) {
        return new N1N2MsgTxfrFailureNotification().cause(cause).n1n2MsgDataUri("/namf-comm/v1/x").maxWaitingTime(60);
    }
}
