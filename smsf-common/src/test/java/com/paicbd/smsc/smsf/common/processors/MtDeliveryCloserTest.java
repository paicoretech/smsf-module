package com.paicbd.smsc.smsf.common.processors;

import com.paicbd.smsc.cdr.CdrProcessor;
import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.smsf.common.utils.Http2GatewayCreator;
import com.paicbd.smsc.utils.ErrorCodes;
import com.paicbd.smsc.utils.RegEventState;
import com.paicbd.smsc.utils.RequestDelivery;
import com.paicbd.smsc.utils.UtilsEnum;
import com.paicore.sba.stack.codec.Ss7SmsTpduCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.ConcurrentHashMap;

import static com.paicbd.smsc.scylla.ScyllaTablesConstants.SMSF_SUBMIT_SM_RESULT_TABLE;
import static com.paicbd.smsc.smsf.common.constants.Constants.ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MtDeliveryCloserTest {

    private static final int NETWORK_ID = 7;
    private static final String MESSAGE_ID = "1783981200123-86234715900123";
    private static final String SUPI = "imsi-5215512345678";

    private ScyllaManager scyllaManager;
    private KafkaTemplate<String, String> kafkaTemplate;
    private CdrProcessor cdrProcessor;
    private MtDeliveryCloser closer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        scyllaManager = mock(ScyllaManager.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        cdrProcessor = mock(CdrProcessor.class);
        Http2Gateway gateway = Http2GatewayCreator.buildHttp2Gateway(
                "smsf-gw", NETWORK_ID, "http://127.0.0.1/namf-comm/v1/ue-contexts/{supi}/n1-n2-messages", ENABLED);
        MessageFactory messageFactory = new MessageFactory(gateway, new ConcurrentHashMap<>(),
                new MessageTpduProcessor(new Ss7SmsTpduCodec()));
        closer = new MtDeliveryCloser(gateway, scyllaManager, kafkaTemplate, cdrProcessor, messageFactory);
    }

    @Test
    @DisplayName("only the caller that removes the correlation may close the delivery")
    void onlyOneCallerWinsTheRow() {
        when(scyllaManager.deleteFromTableIfExists(SMSF_SUBMIT_SM_RESULT_TABLE, MESSAGE_ID))
                .thenReturn(true, false);

        assertTrue(closer.claim(MESSAGE_ID));
        assertFalse(closer.claim(MESSAGE_ID));
    }

    @Test
    @DisplayName("if the claim itself fails nobody closes, rather than both closing")
    void aFailedClaimDoesNotClose() {
        when(scyllaManager.deleteFromTableIfExists(anyString(), anyString()))
                .thenThrow(new IllegalStateException("scylla down"));

        assertFalse(closer.claim(MESSAGE_ID));
    }

    @Test
    @DisplayName("an absent subscriber is marked, charged back and reported with the code of its cause")
    void absentSubscriberIsMarkedAndClosed() {
        MessageEvent event = event(true);

        closer.closeAsAbsent(event, "UE_NOT_RESPONDING");

        assertEquals(ErrorCodes.SMSF_UE_NOT_RESPONDING, event.getErrorCode());
        verify(scyllaManager).updateSmsfRegEventState(SUPI, RegEventState.INACTIVE.name());
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());
        verify(cdrProcessor).publishCdr(eq(event), eq(""), eq(UtilsEnum.Module.HTTP2_CLIENT),
                eq(UtilsEnum.MessageType.MESSAGE), eq(UtilsEnum.CdrStatus.FAILED), anyString(), eq(kafkaTemplate));
    }

    @Test
    @DisplayName("an absence cause we do not recognise still closes the delivery, with the callback code")
    void anUnknownAbsenceCauseFallsBackToTheCallbackCode() {
        MessageEvent event = event(true);

        closer.closeAsAbsent(event, "SOMETHING_THE_AMF_INVENTED");

        assertEquals(ErrorCodes.SMSF_N1N2_FAILURE_CALLBACK, event.getErrorCode());
    }

    @Test
    @DisplayName("a subscriber with no resolved SUPI is not marked, and the delivery still closes")
    void withoutSupiNothingIsMarked() {
        MessageEvent event = event(true);
        event.setDestination(null);

        closer.closeAsAbsent(event, "UE_NOT_RESPONDING");

        verify(scyllaManager, never()).updateSmsfRegEventState(anyString(), anyString());
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());
    }

    @Test
    @DisplayName("no receipt is sent when the originator did not ask for one")
    void noReceiptWhenNoneWasRequested() {
        closer.closeAsFailed(event(false), ErrorCodes.SMSF_UE_NOT_RESPONDING, "comment");

        verify(kafkaTemplate, never()).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());
        verify(cdrProcessor).publishCdr(any(MessageEvent.class), eq(""), eq(UtilsEnum.Module.HTTP2_CLIENT),
                eq(UtilsEnum.MessageType.MESSAGE), eq(UtilsEnum.CdrStatus.FAILED), anyString(), eq(kafkaTemplate));
    }

    @Test
    @DisplayName("a comment is built only when the AMF named a cause")
    void theCommentCarriesTheCause() {
        assertTrue(closer.buildFailureComment(ErrorCodes.SMSF_UE_NOT_RESPONDING, "UE_NOT_RESPONDING")
                .contains("UE_NOT_RESPONDING"));
        assertNull(closer.buildFailureComment(ErrorCodes.SMSF_UE_NOT_RESPONDING, null));
        assertNull(closer.buildFailureComment(ErrorCodes.SMSF_UE_NOT_RESPONDING, "  "));
    }

    private MessageEvent event(boolean requestDlr) {
        MessageEvent event = MessageEvent.builder()
                .id(MESSAGE_ID)
                .messageId(MESSAGE_ID)
                .parentId(MESSAGE_ID)
                .sourceAddr("12025550100")
                .destinationAddr("5215512345678")
                .destination(SUPI)
                .originProtocol("SMPP")
                .destNetworkId(NETWORK_ID)
                .shortMessage("X")
                .build();
        event.setDataCoding(0);
        event.setRegisteredDelivery(requestDlr
                ? RequestDelivery.REQUEST_DLR.getValue()
                : RequestDelivery.NON_REQUEST_DLR.getValue());
        return event;
    }
}
