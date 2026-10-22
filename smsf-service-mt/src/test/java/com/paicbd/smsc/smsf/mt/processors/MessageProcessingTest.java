package com.paicbd.smsc.smsf.mt.processors;

import com.paicbd.smsc.cdr.CdrProcessor;
import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.MessagePart;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.smsf.mt.http2.Http2ClientConnection;
import com.paicbd.smsc.smsf.common.processors.MessageTpduProcessor;
import com.paicbd.smsc.smsf.common.processors.MessageFactory;
import com.paicbd.smsc.smsf.common.processors.MtTimings;
import com.paicbd.smsc.smsf.common.utils.Http2GatewayCreator;
import com.paicbd.smsc.utils.RegEventState;
import com.paicbd.smsc.utils.RequestDelivery;
import com.paicbd.smsc.utils.ErrorCodes;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.UtilsEnum;
import com.paicore.sba.stack.codec.Ss7SmsTpduCodec;
import com.paicore.sba.stack.transport.http2.dto.Http2Request;
import com.paicore.sba.stack.transport.http2.dto.Http2Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import static com.paicbd.smsc.smsf.common.constants.Constants.ENABLED;
import static com.paicbd.smsc.scylla.ScyllaTablesConstants.SMSF_SUBMIT_SM_RESULT_TABLE;
import static com.paicore.sba.stack.transport.http2.util.Http2Constants.HEADER_CONTENT_TYPE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MessageProcessingTest {

    private static final String MESSAGE_ID = "1783981200123-86234715900123";
    private static final String FIRST_PART_ID = "1783981200124-86234715900124";
    private static final String FINAL_PART_ID = "1783981200125-86234715900125";
    private static final String SECOND_MESSAGE_ID = "1783981200126-86234715900126";
    private static final String THIRD_MESSAGE_ID = "1783981200127-86234715900127";
    private static final String FOURTH_MESSAGE_ID = "1783981200128-86234715900128";
    private static final String FIFTH_MESSAGE_ID = "1783981200129-86234715900129";
    private static final String SIXTH_MESSAGE_ID = "1783981200130-86234715900130";
    private static final int STATUS_REPORT_INDICATION_MASK = 0x20;

    @Mock
    private Http2ClientConnection clientConnection;

    @Mock
    private ScyllaManager scyllaManager;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private CdrProcessor cdrProcessor;

    private MessageProcessor messageProcessing;

    private Http2Gateway gateway;

    @BeforeEach
    void setUp() {
        gateway = Http2GatewayCreator.buildHttp2Gateway(
                "smsf-gw", 7, "http://127.0.0.1/nsmsf-sms/v2/ue-contexts/{supi}/sendsms", ENABLED);
        rebuildProcessor();
    }

    /** Rebuilt after a test changes the gateway's retry lists, which are read at dispatch time. */
    private void rebuildProcessor() {
        MessageFactory messageFactory = new MessageFactory(gateway, new ConcurrentHashMap<>(),
                new MessageTpduProcessor(new Ss7SmsTpduCodec()));
        messageProcessing = new MessageProcessor(gateway, clientConnection, messageFactory, scyllaManager, kafkaTemplate, cdrProcessor, Optional::empty, "https://smsf.local/namf-cb/v1/4/n1n2-failure", MtTimings.defaults(), Runnable::run);
    }

    @AfterEach
    void tearDown() {
        messageProcessing.stopWatchers();
    }

    @Test
    @DisplayName("processIncomingRequest, when called, then does not throw and does not interact with client")
    void processIncomingRequestWhenCalledThenDoesNotThrow() {
        Http2Request request = Http2Request.builder().method("POST").path("/incoming").build();

        assertDoesNotThrow(() -> messageProcessing.processIncomingRequest(request));
        verify(clientConnection, never()).send(any(Http2Request.class));
    }

    @Test
    @DisplayName("processOutgoingRequest, when valid event, then sends a multipart/related sendsms request")
    void processOutgoingRequestWhenValidEventThenSends() {
        MessageEvent event = buildMessageEvent(MESSAGE_ID, "5215512345678", "Hello SMSF");
        Http2Response response = okResponse();
        when(clientConnection.send(any(Http2Request.class))).thenReturn(CompletableFuture.completedFuture(response));

        messageProcessing.processOutgoingRequest(event);

        ArgumentCaptor<Http2Request> requestCaptor = ArgumentCaptor.forClass(Http2Request.class);
        verify(clientConnection).send(requestCaptor.capture());

        Http2Request captured = requestCaptor.getValue();
        assertEquals("POST", captured.getMethod());
        assertEquals("/nsmsf-sms/v2/ue-contexts/imsi-5215512345678/sendsms", captured.getPath());
        assertEquals(MESSAGE_ID, captured.getCorrelationId());
        assertTrue(captured.getHeaders().get(HEADER_CONTENT_TYPE).startsWith("multipart/related"));

        String body = new String(captured.getBody(), java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(body.contains("\"n1MessageClass\":\"SMS\""));
        assertTrue(body.contains("\"serviceInstanceId\":\"" + MESSAGE_ID + "\""));
        assertTrue(body.contains("\"gpsi\":\"msisdn-5215512345678\""));
        assertTrue(body.contains("\"contentId\":\"n1-sms-payload\""));
        assertTrue(body.contains("\"n1n2FailureTxfNotifURI\":\"https://smsf.local/namf-cb/v1/4/n1n2-failure/" + MESSAGE_ID + "\""));
        assertTrue(body.contains("Content-Id: n1-sms-payload"));
        assertEquals(1, messageProcessing.getProcessMtPerSecond());
        verifyCdr(event, UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.SUCCESS, null);
    }

    @Test
    @DisplayName("processOutgoingRequest sends every part but requests and persists DLR only for the final segment")
    void processOutgoingRequestWhenMultipartRequestsDlrOnlyForFinalSegment() {
        MessageEvent event = buildMessageEvent(MESSAGE_ID, "5215512345678", null);
        event.setRegisteredDelivery(RequestDelivery.REQUEST_DLR.getValue());
        event.setMessageParts(List.of(
                buildPart(FIRST_PART_ID, "Hello ", 1),
                buildPart(FINAL_PART_ID, "world", 2)));
        Http2Response response = okResponse();
        when(clientConnection.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        messageProcessing.processOutgoingRequest(event);

        ArgumentCaptor<Http2Request> captor = ArgumentCaptor.forClass(Http2Request.class);
        verify(clientConnection, times(2)).send(captor.capture());

        List<Http2Request> requests = captor.getAllValues();
        assertEquals(FIRST_PART_ID, requests.get(0).getCorrelationId());
        assertEquals(FINAL_PART_ID, requests.get(1).getCorrelationId());
        for (Http2Request request : requests) {
            assertEquals("/nsmsf-sms/v2/ue-contexts/imsi-5215512345678/sendsms", request.getPath());
            String body = new String(request.getBody(), java.nio.charset.StandardCharsets.UTF_8);
            assertTrue(body.contains("\"gpsi\":\"msisdn-5215512345678\""));
            assertTrue(binaryPartOf(request).length > 0);
        }
        assertEquals(0, binaryPartOf(requests.get(0))[0] & STATUS_REPORT_INDICATION_MASK);
        assertEquals(STATUS_REPORT_INDICATION_MASK, binaryPartOf(requests.get(1))[0] & STATUS_REPORT_INDICATION_MASK);
        verify(scyllaManager).insertIntoTable(eq(SMSF_SUBMIT_SM_RESULT_TABLE), eq(FINAL_PART_ID), anyString(), anyInt());
        verify(scyllaManager, never()).insertIntoTable(eq(SMSF_SUBMIT_SM_RESULT_TABLE), eq(FIRST_PART_ID), anyString(), anyInt());
        verify(cdrProcessor, times(2)).publishCdr(any(MessageEvent.class), eq(UtilsEnum.Module.HTTP2_CLIENT), eq(UtilsEnum.MessageType.MESSAGE), eq(UtilsEnum.CdrStatus.SUCCESS), eq(kafkaTemplate));
        assertEquals(2, messageProcessing.getProcessMtPerSecond());
    }

    @Test
    @DisplayName("processOutgoingRequest, when the message cannot be encoded, then it is not sent")
    void processOutgoingRequestWhenEncodeFailsThenSkip() {
        MessageEvent event = buildMessageEvent(SECOND_MESSAGE_ID, "5215512345678", "Body");
        event.setDataCoding(null);

        assertDoesNotThrow(() -> messageProcessing.processOutgoingRequest(event));
        verify(clientConnection, never()).send(any(Http2Request.class));
        assertEquals(0, messageProcessing.getProcessMtPerSecond());
        verifyCdr(event, UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, 500);
    }

    @Test
    @DisplayName("processOutgoingRequest, when client send completes successfully, then processResponse handles it")
    void processOutgoingRequestWhenSuccessThenResponseHandled() {
        MessageEvent event = buildMessageEvent(THIRD_MESSAGE_ID, "5215511112222", "Ok");
        Http2Response response = okResponse();
        when(clientConnection.send(any(Http2Request.class))).thenReturn(CompletableFuture.completedFuture(response));

        assertDoesNotThrow(() -> messageProcessing.processOutgoingRequest(event));
        verify(clientConnection).send(any(Http2Request.class));
        verifyCdr(event, UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.SUCCESS, null);
    }

    @Test
    @DisplayName("processOutgoingRequest, when client send fails, then processError handles it without throwing")
    void processOutgoingRequestWhenErrorThenErrorHandled() {
        MessageEvent event = buildMessageEvent(FOURTH_MESSAGE_ID, "5215512345678", "X");
        CompletableFuture<Http2Response> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("not connected"));
        when(clientConnection.send(any(Http2Request.class))).thenReturn(failed);

        assertDoesNotThrow(() -> messageProcessing.processOutgoingRequest(event));
        verify(clientConnection).send(any(Http2Request.class));
        verifyCdr(event, UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, 503);
    }

    @Test
    @DisplayName("processOutgoingRequest persists DLR correlation and keeps it after a successful response")
    void processOutgoingRequestPersistsRequestedDlrCorrelation() {
        MessageEvent event = buildMessageEvent(MESSAGE_ID, "5215512345678", "Awaiting receipt");
        event.setRegisteredDelivery(RequestDelivery.REQUEST_DLR.getValue());
        when(clientConnection.send(any(Http2Request.class))).thenReturn(CompletableFuture.completedFuture(okResponse()));

        messageProcessing.processOutgoingRequest(event);

        verify(scyllaManager).insertIntoTable(eq(SMSF_SUBMIT_SM_RESULT_TABLE), eq(MESSAGE_ID), anyString(), anyInt());
        verify(scyllaManager, never()).deleteFromTable(anyString(), anyString());
    }

    @Test
    @DisplayName("processOutgoingRequest removes DLR correlation after a non-success response")
    void processOutgoingRequestRemovesDlrCorrelationAfterRejectedResponse() {
        MessageEvent event = buildMessageEvent(MESSAGE_ID, "5215512345678", "Rejected message");
        event.setRegisteredDelivery(RequestDelivery.REQUEST_DLR.getValue());
        when(clientConnection.send(any(Http2Request.class))).thenReturn(CompletableFuture.completedFuture(Http2Response.builder().status(503).build()));

        messageProcessing.processOutgoingRequest(event);

        verify(scyllaManager).insertIntoTable(eq(SMSF_SUBMIT_SM_RESULT_TABLE), eq(MESSAGE_ID), anyString(), anyInt());
        verify(scyllaManager).deleteFromTable(SMSF_SUBMIT_SM_RESULT_TABLE, MESSAGE_ID);
        verifyCdr(event, UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, 503);
    }

    @Test
    @DisplayName("processOutgoingRequest does not persist correlation when DLR was not requested")
    void processOutgoingRequestSkipsCorrelationWhenDlrWasNotRequested() {
        MessageEvent event = buildMessageEvent(MESSAGE_ID, "5215512345678", "No receipt");
        event.setRegisteredDelivery(RequestDelivery.NON_REQUEST_DLR.getValue());
        when(clientConnection.send(any(Http2Request.class))).thenReturn(CompletableFuture.completedFuture(okResponse()));

        messageProcessing.processOutgoingRequest(event);

        verify(scyllaManager, never()).insertIntoTable(anyString(), anyString(), anyString(), anyInt());
        verify(scyllaManager, never()).deleteFromTable(anyString(), anyString());
    }

    @Test
    @DisplayName("processOutgoingRequest cleans correlation when the client fails synchronously")
    void processOutgoingRequestCleansCorrelationAfterSynchronousClientFailure() {
        MessageEvent event = buildMessageEvent(MESSAGE_ID, "5215512345678", "Transport failure");
        event.setRegisteredDelivery(RequestDelivery.REQUEST_DLR.getValue());
        when(clientConnection.send(any(Http2Request.class))).thenThrow(new IllegalStateException("connection unavailable"));

        assertDoesNotThrow(() -> messageProcessing.processOutgoingRequest(event));

        verify(scyllaManager).insertIntoTable(eq(SMSF_SUBMIT_SM_RESULT_TABLE), eq(MESSAGE_ID), anyString(), anyInt());
        verify(scyllaManager).deleteFromTable(SMSF_SUBMIT_SM_RESULT_TABLE, MESSAGE_ID);
        assertEquals(0, messageProcessing.getProcessMtPerSecond());
        verifyCdr(event, UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, 503);
    }

    @Test
    @DisplayName("processOutgoingRequest continues when DLR correlation cannot be persisted")
    void processOutgoingRequestContinuesAfterCorrelationPersistenceFailure() {
        MessageEvent event = buildMessageEvent(MESSAGE_ID, "5215512345678", "Persistence failure");
        event.setRegisteredDelivery(RequestDelivery.REQUEST_DLR.getValue());
        doThrow(new IllegalStateException("scylla unavailable")).when(scyllaManager).insertIntoTable(eq(SMSF_SUBMIT_SM_RESULT_TABLE), eq(MESSAGE_ID), anyString(), anyInt());
        when(clientConnection.send(any(Http2Request.class))).thenReturn(CompletableFuture.completedFuture(okResponse()));

        assertDoesNotThrow(() -> messageProcessing.processOutgoingRequest(event));

        verify(clientConnection).send(any(Http2Request.class));
        assertEquals(1, messageProcessing.getProcessMtPerSecond());
    }

    @Test
    @DisplayName("processOutgoingRequest tolerates correlation cleanup failure")
    void processOutgoingRequestToleratesCorrelationCleanupFailure() {
        MessageEvent event = buildMessageEvent(MESSAGE_ID, "5215512345678", "Cleanup failure");
        event.setRegisteredDelivery(RequestDelivery.REQUEST_DLR.getValue());
        doThrow(new IllegalStateException("scylla unavailable")).when(scyllaManager).deleteFromTable(SMSF_SUBMIT_SM_RESULT_TABLE, MESSAGE_ID);
        when(clientConnection.send(any(Http2Request.class))).thenReturn(CompletableFuture.completedFuture(Http2Response.builder().status(500).build()));

        assertDoesNotThrow(() -> messageProcessing.processOutgoingRequest(event));

        verify(scyllaManager).deleteFromTable(SMSF_SUBMIT_SM_RESULT_TABLE, MESSAGE_ID);
    }

    @Test
    @DisplayName("processResponse, when response and event provided, then does not throw")
    void processResponseWhenProvidedThenDoesNotThrow() {
        MessageEvent event = buildMessageEvent(FIFTH_MESSAGE_ID, "5215512345678", "Response");
        Http2Response response = okResponse();
        assertDoesNotThrow(() -> messageProcessing.processResponse(response, event));
        verifyCdr(event, UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.SUCCESS, null);
    }

    @Test
    @DisplayName("processError, when error and event provided, then does not throw")
    void processErrorWhenProvidedThenDoesNotThrow() {
        MessageEvent event = buildMessageEvent(SIXTH_MESSAGE_ID, "5215512345678", "Failure");
        Throwable error = new RuntimeException("kaboom");
        assertDoesNotThrow(() -> messageProcessing.processError(error, event));
        verifyCdr(event, UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, 500);
    }

    @Test
    @DisplayName("processResponse publishes a failed CDR when the response is null")
    void processResponseWhenNullPublishesFailedCdr() {
        MessageEvent event = buildMessageEvent(SECOND_MESSAGE_ID, "5215512345678", "No response");

        messageProcessing.processResponse(null, event);

        verifyCdr(event, UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, 500);
    }

    @Test
    @DisplayName("processError maps timeout failures to HTTP gateway timeout")
    void processErrorWhenTimeoutMapsGatewayTimeout() {
        MessageEvent event = buildMessageEvent(THIRD_MESSAGE_ID, "5215512345678", "Timeout");

        messageProcessing.processError(new RuntimeException(new TimeoutException("read timeout")), event);

        verifyCdr(event, UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, 504);
    }

    @Test
    @DisplayName("processOutgoingRequest publishes DELIVER CDR for a delivery report")
    void processOutgoingRequestWhenDlrPublishesDeliverCdr() {
        MessageEvent event = buildMessageEvent(FOURTH_MESSAGE_ID, "5215512345678", null);
        event.setDlr(true);
        event.setStatus("UNDELIV");
        event.setErrorCode(32);
        when(clientConnection.send(any(Http2Request.class))).thenReturn(CompletableFuture.completedFuture(okResponse()));

        messageProcessing.processOutgoingRequest(event);

        verifyCdr(event, UtilsEnum.MessageType.DELIVER, UtilsEnum.CdrStatus.SUCCESS, 32);
        verify(scyllaManager, never()).insertIntoTable(anyString(), anyString(), anyString(), anyInt());
    }

    @Test
    @DisplayName("processOutgoingRequest publishes one CDR for each multipart result")
    void processOutgoingRequestWhenMultipartHasMixedResultsPublishesEachCdr() {
        MessageEvent event = buildMessageEvent(MESSAGE_ID, "5215512345678", null);
        event.setMessageParts(List.of(buildPart(FIRST_PART_ID, "Hello ", 1), buildPart(FINAL_PART_ID, "world", 2)));
        when(clientConnection.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(okResponse()))
                .thenReturn(CompletableFuture.completedFuture(Http2Response.builder().status(429).build()));

        messageProcessing.processOutgoingRequest(event);

        ArgumentCaptor<MessageEvent> eventCaptor = ArgumentCaptor.forClass(MessageEvent.class);
        ArgumentCaptor<UtilsEnum.CdrStatus> statusCaptor = ArgumentCaptor.forClass(UtilsEnum.CdrStatus.class);
        verify(cdrProcessor, times(2)).publishCdr(eventCaptor.capture(), eq(UtilsEnum.Module.HTTP2_CLIENT), eq(UtilsEnum.MessageType.MESSAGE), statusCaptor.capture(), eq(kafkaTemplate));
        assertEquals(List.of(FIRST_PART_ID, FINAL_PART_ID), eventCaptor.getAllValues().stream().map(MessageEvent::getMessageId).toList());
        assertEquals(List.of(UtilsEnum.CdrStatus.SUCCESS, UtilsEnum.CdrStatus.FAILED), statusCaptor.getAllValues());
        assertEquals(429, eventCaptor.getAllValues().get(1).getErrorCode());
    }

    @Test
    @DisplayName("processResponse appends the 3GPP ProblemDetails cause to the CDR comment on failure")
    void processResponseAppendsProblemCauseToComment() {
        MessageEvent event = buildMessageEvent(FIFTH_MESSAGE_ID, "5215512345678", "Rejected");
        String problemJson = "{\"status\":404,\"title\":\"Not Found\",\"cause\":\"CONTEXT_NOT_FOUND\",\"detail\":\"UE context not found\"}";
        Http2Response response = Http2Response.builder().status(404)
                .body(problemJson.getBytes(StandardCharsets.UTF_8)).build();

        messageProcessing.processResponse(response, event);

        assertEquals(404, event.getErrorCode());
        assertEquals("HTTP2", event.getDestProtocol());
        verify(cdrProcessor).publishCdr(event, "", UtilsEnum.Module.HTTP2_CLIENT,
                UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED,
                "Not Found - CONTEXT_NOT_FOUND", kafkaTemplate);
    }

    @Test
    @DisplayName("processResponse falls back to the plain CDR when the failure body carries no cause")
    void processResponseWithoutCauseUsesPlainCdr() {
        MessageEvent event = buildMessageEvent(SIXTH_MESSAGE_ID, "5215512345678", "Rejected");
        Http2Response response = Http2Response.builder().status(503)
                .body("{\"status\":503,\"title\":\"Service Unavailable\"}".getBytes(StandardCharsets.UTF_8)).build();

        messageProcessing.processResponse(response, event);

        verifyCdr(event, UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, 503);
    }

    @Test
    @DisplayName("processOutgoingRequest maps a missing destinationSupi to a 404 USER_NOT_FOUND CDR without sending")
    void processOutgoingRequestWhenDestinationSupiMissingPublishesUserNotFound() {
        MessageEvent event = buildMessageEvent(MESSAGE_ID, "5215512345678", "No supi");
        event.setDestination(null);

        messageProcessing.processOutgoingRequest(event);

        verify(clientConnection, never()).send(any(Http2Request.class));
        assertEquals(404, event.getErrorCode());
        verify(cdrProcessor).publishCdr(event, "", UtilsEnum.Module.HTTP2_CLIENT,
                UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED,
                "Not Found - USER_NOT_FOUND", kafkaTemplate);
    }

    // --- throughput counters --------------------------------------------------

    @Test
    @DisplayName("a delivery receipt counts on the DLR rate, not on the MT one")
    void deliveryReceiptCountsSeparately() {
        MessageEvent dlr = buildMessageEvent("dlr-rate-1", "5215512345678", "X");
        dlr.setDlr(true);
        when(clientConnection.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(okResponse()));

        messageProcessing.processOutgoingRequest(dlr);

        assertEquals(1, messageProcessing.getProcessDlrPerSecond());
        assertEquals(0, messageProcessing.getProcessMtPerSecond());
    }

    @Test
    @DisplayName("a message counts on the MT rate, not on the DLR one")
    void messageCountsSeparately() {
        MessageEvent event = buildMessageEvent("mt-rate-1", "5215512345678", "X");
        when(clientConnection.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(okResponse()));

        messageProcessing.processOutgoingRequest(event);

        assertEquals(1, messageProcessing.getProcessMtPerSecond());
        assertEquals(0, messageProcessing.getProcessDlrPerSecond());
    }

    // --- failure flow: retry or CDR, never lost -------------------------------

    @Test
    @DisplayName("a failure the gateway lists as auto-retry goes back to the retries topic")
    void failureListedAsAutoRetryIsRepublished() {
        gateway.setAutoRetryErrorCode("503");
        rebuildProcessor();
        MessageEvent event = retryableEvent("auto-retry-1");

        failSendWith(new IllegalStateException("not connected"));
        messageProcessing.processOutgoingRequest(event);

        // The attempt is recorded as failed...
        verifyCdr(event, UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, 503);
        // ...and the message itself lives on, with its error cleared and the counter advanced.
        MessageEvent republished = capturePublished("retries.sms.medium");
        assertTrue(republished.isRetry());
        assertEquals(1, republished.getRetryNumber());
        assertNull(republished.getErrorCode());
        assertEquals("auto-retry-1", republished.getMessageId());
    }

    @Test
    @DisplayName("a failure the gateway lists as alternate-destination goes back to routing")
    void failureListedAsAlternateDestinationIsRerouted() {
        gateway.setRetryAlternateDestinationErrorCode("503");
        rebuildProcessor();
        MessageEvent event = retryableEvent("alt-dest-1");

        failSendWith(new IllegalStateException("not connected"));
        messageProcessing.processOutgoingRequest(event);

        verifyCdr(event, UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, 503);
        assertTrue(capturePublished("pre.message.medium").isRetry());
    }

    @Test
    @DisplayName("a failure the gateway lists as no-retry is written off as a failed CDR")
    void failureListedAsNoRetryIsClosed() {
        gateway.setNoRetryErrorCode("503");
        gateway.setAutoRetryErrorCode("503");
        rebuildProcessor();
        MessageEvent event = retryableEvent("no-retry-1");

        failSendWith(new IllegalStateException("not connected"));
        messageProcessing.processOutgoingRequest(event);

        verifyCdr(event, UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, 503);
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    @DisplayName("an error the gateway lists nowhere is not retried blindly")
    void unlistedErrorIsNotRetried() {
        gateway.setAutoRetryErrorCode("408,429");
        rebuildProcessor();
        MessageEvent event = retryableEvent("unlisted-1");

        failSendWith(new IllegalStateException("not connected"));
        messageProcessing.processOutgoingRequest(event);

        verifyCdr(event, UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, 503);
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    @DisplayName("a message that has reached its validity period is closed, retryable error or not")
    void lastRetryIsClosedEvenWhenTheErrorIsRetryable() {
        gateway.setAutoRetryErrorCode("503");
        rebuildProcessor();
        MessageEvent event = retryableEvent("last-retry-1");
        event.setLastRetry(true);

        failSendWith(new IllegalStateException("not connected"));
        messageProcessing.processOutgoingRequest(event);

        verifyCdr(event, UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, 503);
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    @DisplayName("a message with no validity period left is closed without retrying")
    void noValidityPeriodIsClosed() {
        gateway.setAutoRetryErrorCode("503");
        rebuildProcessor();
        MessageEvent event = buildMessageEvent("no-validity-1", "5215512345678", "X");
        event.setValidityPeriod(0);

        failSendWith(new IllegalStateException("not connected"));
        messageProcessing.processOutgoingRequest(event);

        verifyCdr(event, UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, 503);
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    @DisplayName("an AMF rejection follows the same flow as a transport failure")
    void amfRejectionIsRetriedToo() {
        gateway.setAutoRetryErrorCode("500");
        rebuildProcessor();
        MessageEvent event = retryableEvent("amf-500-1");

        when(clientConnection.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        Http2Response.builder().status(500).build()));

        messageProcessing.processOutgoingRequest(event);

        assertTrue(capturePublished("retries.sms.medium").isRetry());
    }

    @Test
    @DisplayName("a closed failure sends the originator a delivery receipt when one was requested")
    void closedFailureSendsTheFailureDlr() {
        MessageEvent event = buildMessageEvent("dlr-fail-1", "5215512345678", "X");
        event.setRegisteredDelivery(RequestDelivery.REQUEST_DLR.getValue());

        failSendWith(new IllegalStateException("not connected"));
        messageProcessing.processOutgoingRequest(event);

        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());
    }

    @Test
    @DisplayName("a closed failure sends no receipt when the originator did not ask for one")
    void closedFailureWithoutRequestedDlrSendsNothing() {
        MessageEvent event = buildMessageEvent("dlr-fail-2", "5215512345678", "X");
        event.setRegisteredDelivery(RequestDelivery.NON_REQUEST_DLR.getValue());

        failSendWith(new IllegalStateException("not connected"));
        messageProcessing.processOutgoingRequest(event);

        verify(kafkaTemplate, never()).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());
    }

    @Test
    @DisplayName("an MT with no registeredDelivery at all is closed without a receipt, not with an error")
    void closedFailureWithoutRegisteredDeliveryDoesNotThrow() {
        // An MT built from an MO TPDU carries no registeredDelivery; unboxing it would blow up here.
        MessageEvent event = buildMessageEvent("dlr-fail-3", "5215512345678", "X");
        event.setRegisteredDelivery(null);

        failSendWith(new IllegalStateException("not connected"));

        assertDoesNotThrow(() -> messageProcessing.processOutgoingRequest(event));
        verify(kafkaTemplate, never()).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());
    }

    @Test
    @DisplayName("a billed message that fails for good is refunded")
    void closedFailureRefundsABilledMessage() {
        MessageEvent event = buildMessageEvent("refund-1", "5215512345678", "X");
        event.setApplyForRefund(true);
        event.setSmscMessagePriority("MEDIUM");

        failSendWith(new IllegalStateException("not connected"));
        messageProcessing.processOutgoingRequest(event);

        verify(kafkaTemplate).send(eq("charging.sms.medium"), anyString());
    }

    @Test
    @DisplayName("a message that is retried is not refunded, because it has not failed for good yet")
    void retriedMessageIsNotRefunded() {
        gateway.setAutoRetryErrorCode("503");
        rebuildProcessor();
        MessageEvent event = retryableEvent("refund-2");
        event.setApplyForRefund(true);

        failSendWith(new IllegalStateException("not connected"));
        messageProcessing.processOutgoingRequest(event);

        verify(kafkaTemplate, never()).send(eq("charging.sms.medium"), anyString());
        verify(kafkaTemplate).send(eq("retries.sms.medium"), anyString());
    }

    /** Alive and retryable: a validity period left and no last-retry flag. */
    private MessageEvent retryableEvent(String messageId) {
        MessageEvent event = buildMessageEvent(messageId, "5215512345678", "X");
        event.setValidityPeriod(60);
        event.setSmscMessagePriority("MEDIUM");
        return event;
    }

    private void failSendWith(Throwable cause) {
        CompletableFuture<Http2Response> failed = new CompletableFuture<>();
        failed.completeExceptionally(cause);
        when(clientConnection.send(any(Http2Request.class))).thenReturn(failed);
    }

    @Test
    @DisplayName("an absent-subscriber cause marks the subscriber INACTIVE and closes the delivery")
    void absentCauseDeactivatesTheSubscriber() {
        MessageEvent event = buildMessageEvent("absent-1", "5215512345678", "X");
        respondWith(200, "{\"cause\":\"UE_NOT_RESPONDING\"}");

        messageProcessing.processOutgoingRequest(event);

        verify(scyllaManager).updateSmsfRegEventState("imsi-5215512345678", RegEventState.INACTIVE.name());
        assertEquals(ErrorCodes.SMSF_UE_NOT_RESPONDING, event.getErrorCode());
        verifyFailureCdr(event, "UE_NOT_RESPONDING");
    }

    @Test
    @DisplayName("each absent cause raises its own error code")
    void everyAbsentCauseRaisesItsOwnCode() {
        assertEquals(ErrorCodes.SMSF_UE_NOT_REACHABLE_FOR_SESSION,
                errorCodeAfterCause("absent-2", "UE_NOT_REACHABLE_FOR_SESSION"));
        assertEquals(ErrorCodes.SMSF_N1_MSG_NOT_TRANSFERRED,
                errorCodeAfterCause("absent-3", "N1_MSG_NOT_TRANSFERRED"));
    }

    @Test
    @DisplayName("an absent subscriber is not deactivated when the SUPI was never resolved")
    void absentWithoutSupiDoesNotDeactivate() {
        MessageEvent event = buildMessageEvent("absent-4", "5215512345678", "X");

        MessageEvent absent = buildMessageEvent("absent-4", "5215512345678", "X");

        assertDoesNotThrow(() -> messageProcessing.handleAbsentSubscriber(absent, "UE_NOT_RESPONDING"));

        event.setDestination(null);
        assertDoesNotThrow(() -> messageProcessing.handleAbsentSubscriber(event, "UE_NOT_RESPONDING"));
        verify(scyllaManager, never()).updateSmsfRegEventState(eq(null), anyString());
    }

    @Test
    @DisplayName("a retryable cause leaves the subscriber alone and retries the delivery when the gateway retries that code")
    void retryableCauseRetriesWithoutDeactivating() {
        gateway.setAutoRetryErrorCode(String.valueOf(ErrorCodes.SMSF_UE_TEMPORARY_REJECT));
        rebuildProcessor();
        MessageEvent event = retryableEvent("retry-cause-1");
        respondWith(409, "{\"error\":{\"cause\":\"TEMPORARY_REJECT_HANDOVER_ONGOING\"}}");

        messageProcessing.processOutgoingRequest(event);

        verify(scyllaManager, never()).updateSmsfRegEventState(anyString(), anyString());
        assertEquals(ErrorCodes.SMSF_UE_TEMPORARY_REJECT, event.getErrorCode());
        assertTrue(capturePublished("retries.sms.medium").isRetry());
    }

    @Test
    @DisplayName("a retryable cause on a 2xx is still not a delivery: the AMF did not transfer the message")
    void retryableCauseOnSuccessStatusIsNotDelivered() {
        MessageEvent event = buildMessageEvent("retry-cause-2", "5215512345678", "X");
        respondWith(200, "{\"cause\":\"AN_NOT_RESPONDING\"}");

        messageProcessing.processOutgoingRequest(event);

        assertEquals(ErrorCodes.SMSF_UE_TEMPORARY_REJECT, event.getErrorCode());
        verifyFailureCdr(event, "AN_NOT_RESPONDING");
        verify(scyllaManager, never()).updateSmsfRegEventState(anyString(), anyString());
    }

    @Test
    @DisplayName("while the AMF is still trying to reach the UE the delivery stays open: no CDR and no receipt")
    void pendingCauseLeavesTheTransactionOpen() {
        MessageEvent event = buildMessageEvent("pending-1", "5215512345678", "X");
        event.setRegisteredDelivery(RequestDelivery.REQUEST_DLR.getValue());
        respondWith(202, "{\"cause\":\"ATTEMPTING_TO_REACH_UE\"}");

        messageProcessing.processOutgoingRequest(event);

        verifyNoCdr();
        verify(kafkaTemplate, never()).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());
        verify(scyllaManager, never()).updateSmsfRegEventState(anyString(), anyString());
    }

    @Test
    @DisplayName("the correlation outlives the maxWaitingTime the AMF reported")
    void pendingCorrelationUsesTheReportedMaxWaitingTime() {
        MessageEvent event = buildMessageEvent("pending-2", "5215512345678", "X");
        respondWith(504, "{\"error\":{\"cause\":\"ATTEMPTING_TO_REACH_UE\"},\"errInfo\":{\"maxWaitingTime\":1800}}");

        messageProcessing.processOutgoingRequest(event);

        verify(scyllaManager).insertIntoTable(eq(SMSF_SUBMIT_SM_RESULT_TABLE), eq("pending-2"), anyString(), eq(1860));
    }

    @Test
    @DisplayName("without a maxWaitingTime the correlation keeps the default lifetime")
    void pendingCorrelationFallsBackToTheDefaultTtl() {
        MessageEvent event = buildMessageEvent("pending-3", "5215512345678", "X");
        respondWith(202, "{\"cause\":\"WAITING_FOR_ASYNCHRONOUS_TRANSFER\"}");

        messageProcessing.processOutgoingRequest(event);

        verify(scyllaManager).insertIntoTable(eq(SMSF_SUBMIT_SM_RESULT_TABLE), eq("pending-3"), anyString(), eq(360));
    }






    @Test
    @DisplayName("the configured timings are used, not the defaults")
    void configuredTimingsAreHonoured() {
        MessageFactory factory = new MessageFactory(gateway, new ConcurrentHashMap<>(),
                new MessageTpduProcessor(new Ss7SmsTpduCodec()));
        MessageProcessor configured = new MessageProcessor(gateway, clientConnection, factory, scyllaManager,
                kafkaTemplate, cdrProcessor, Optional::empty, "https://smsf.local/namf-cb", new MtTimings(30),
                Runnable::run);
        try {
            MessageEvent event = buildMessageEvent("timings-1", "5215512345678", "X");
            respondWith(200, "{\"cause\":\"UE_NOT_RESPONDING\"}");

            configured.processOutgoingRequest(event);

            verify(scyllaManager).updateSmsfRegEventState("imsi-5215512345678", RegEventState.INACTIVE.name());
            assertEquals(30, configured.correlationWaitSeconds(null, event));
        } finally {
            configured.stopWatchers();
        }
    }

    @Test
    @DisplayName("a 2xx without a recognisable cause is not a delivery")
    void successStatusWithoutCauseIsNotADelivery() {
        MessageEvent event = buildMessageEvent("no-cause-1", "5215512345678", "X");
        respondWith(200, "{}");

        messageProcessing.processOutgoingRequest(event);

        assertEquals(ErrorCodes.SMSF_UE_TEMPORARY_REJECT, event.getErrorCode());
        verify(scyllaManager, never()).updateSmsfRegEventState(anyString(), anyString());
    }

    @Test
    @DisplayName("the correlation never waits longer than the validity the message has left")
    void correlationWaitIsCappedByTheRemainingValidity() {
        MessageEvent shortLived = buildMessageEvent("cap-1", "5215512345678", "X");
        shortLived.setValidityPeriod(120);

        assertEquals(120, messageProcessing.correlationWaitSeconds(1800, shortLived));
        assertEquals(120, messageProcessing.correlationWaitSeconds(null, shortLived));

        MessageEvent noValidity = buildMessageEvent("cap-2", "5215512345678", "X");
        assertEquals(1800, messageProcessing.correlationWaitSeconds(1800, noValidity));
        assertEquals(300, messageProcessing.correlationWaitSeconds(null, noValidity));
        assertEquals(300, messageProcessing.correlationWaitSeconds(0, noValidity));
    }

    @Test
    @DisplayName("a correlation still stored when the wait runs out is closed with 525, without deactivating")
    void expiredCorrelationClosesWith525() {
        MessageEvent event = buildMessageEvent("expired-1", "5215512345678", "X");
        event.setRegisteredDelivery(RequestDelivery.REQUEST_DLR.getValue());
        when(scyllaManager.selectFromTable(SMSF_SUBMIT_SM_RESULT_TABLE, "expired-1"))
                .thenReturn(Converter.valueAsString(event));
        when(scyllaManager.deleteFromTableIfExists(SMSF_SUBMIT_SM_RESULT_TABLE, "expired-1")).thenReturn(true);

        messageProcessing.closeExpiredCorrelation("expired-1");

        verify(scyllaManager).deleteFromTableIfExists(SMSF_SUBMIT_SM_RESULT_TABLE, "expired-1");
        verify(scyllaManager, never()).updateSmsfRegEventState(anyString(), anyString());
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());

        ArgumentCaptor<MessageEvent> closed = ArgumentCaptor.forClass(MessageEvent.class);
        verify(cdrProcessor).publishCdr(closed.capture(), eq(""), eq(UtilsEnum.Module.HTTP2_CLIENT),
                eq(UtilsEnum.MessageType.MESSAGE), eq(UtilsEnum.CdrStatus.FAILED),
                org.mockito.ArgumentMatchers.contains("NO_FAILURE_NOTIFICATION_RECEIVED"), eq(kafkaTemplate));
        assertEquals(ErrorCodes.SMSF_N1N2_CORRELATION_TIMEOUT, closed.getValue().getErrorCode());
    }

    @Test
    @DisplayName("a correlation the callback already removed leaves the timer with nothing to do")
    void expiredCorrelationAlreadyClosedDoesNothing() {
        when(scyllaManager.selectFromTable(SMSF_SUBMIT_SM_RESULT_TABLE, "expired-2")).thenReturn(null);

        messageProcessing.closeExpiredCorrelation("expired-2");

        verify(scyllaManager, never()).deleteFromTableIfExists(anyString(), anyString());
        verifyNoCdr();
    }

    @Test
    @DisplayName("when the callback wins the race the timer closes nothing, so there is one CDR and one receipt")
    void expiredCorrelationLosesTheRaceAgainstTheCallback() {
        MessageEvent event = buildMessageEvent("expired-3", "5215512345678", "X");
        event.setRegisteredDelivery(RequestDelivery.REQUEST_DLR.getValue());
        when(scyllaManager.selectFromTable(SMSF_SUBMIT_SM_RESULT_TABLE, "expired-3"))
                .thenReturn(Converter.valueAsString(event));
        when(scyllaManager.deleteFromTableIfExists(SMSF_SUBMIT_SM_RESULT_TABLE, "expired-3")).thenReturn(false);

        messageProcessing.closeExpiredCorrelation("expired-3");

        verifyNoCdr();
        verify(kafkaTemplate, never()).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());
        verify(scyllaManager, never()).updateSmsfRegEventState(anyString(), anyString());
    }

    @Test
    @DisplayName("maxWaitingTime is read only when the AMF reported it")
    void extractMaxWaitingTimeReturnsNullWhenAbsent() {
        assertNull(messageProcessing.extractMaxWaitingTime(null));
        assertNull(messageProcessing.extractMaxWaitingTime(Http2Response.builder().status(504).build()));
        assertNull(messageProcessing.extractMaxWaitingTime(responseOf(504, "{\"errInfo\":{}}")));
        assertEquals(90, messageProcessing.extractMaxWaitingTime(responseOf(504, "{\"errInfo\":{\"maxWaitingTime\":90}}")));
    }

    private void verifyFailureCdr(MessageEvent event, String cause) {
        assertEquals("HTTP2", event.getDestProtocol());
        verify(cdrProcessor).publishCdr(eq(event), eq(""), eq(UtilsEnum.Module.HTTP2_CLIENT),
                eq(UtilsEnum.MessageType.MESSAGE), eq(UtilsEnum.CdrStatus.FAILED),
                org.mockito.ArgumentMatchers.contains(cause), eq(kafkaTemplate));
    }

    private void verifyNoCdr() {
        verify(cdrProcessor, never()).publishCdr(any(MessageEvent.class), any(), any(), any(), any());
        verify(cdrProcessor, never()).publishCdr(any(MessageEvent.class), anyString(), any(), any(), any(), anyString(), any());
    }

    private int errorCodeAfterCause(String messageId, String cause) {
        MessageEvent event = buildMessageEvent(messageId, "5215512345678", "X");
        respondWith(200, "{\"cause\":\"" + cause + "\"}");
        messageProcessing.processOutgoingRequest(event);
        return event.getErrorCode();
    }

    /** What a conforming AMF answers on a transfer it accepted: 200 with the success cause. */
    private Http2Response okResponse() {
        return responseOf(200, "{\"cause\":\"N1_N2_TRANSFER_INITIATED\"}");
    }

    private void respondWith(int status, String jsonBody) {
        when(clientConnection.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(responseOf(status, jsonBody)));
    }

    private Http2Response responseOf(int status, String jsonBody) {
        return Http2Response.builder()
                .status(status)
                .body(jsonBody.getBytes(StandardCharsets.UTF_8))
                .build();
    }

    private MessageEvent capturePublished(String expectedTopic) {
        ArgumentCaptor<String> topic = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(topic.capture(), payload.capture());
        assertEquals(expectedTopic, topic.getValue());
        return Converter.stringToObject(payload.getValue(), MessageEvent.class);
    }

    private MessageEvent buildMessageEvent(String messageId, String destination, String shortMessage) {
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
                .destNetworkId(7)
                .destNetworkName("smsf-gw")
                .destNetworkType("GW")
                .shortMessage(shortMessage)
                .build();
        event.setDataCoding(0);
        return event;
    }

    private MessagePart buildPart(String messageId, String text, int sequence) {
        return MessagePart.builder()
                .messageId(messageId)
                .shortMessage(text)
                .msgReferenceNumber("77")
                .totalSegment(2)
                .segmentSequence(sequence)
                .partBytes(text.getBytes(StandardCharsets.ISO_8859_1))
                .udhBytes(new byte[]{0x05, 0x00, 0x03, 0x77, 0x02, (byte) sequence})
                .build();
    }

    private void verifyCdr(MessageEvent event, UtilsEnum.MessageType messageType, UtilsEnum.CdrStatus status, Integer errorCode) {
        assertEquals(errorCode, event.getErrorCode());
        assertEquals("HTTP2", event.getDestProtocol());
        verify(cdrProcessor).publishCdr(event, UtilsEnum.Module.HTTP2_CLIENT, messageType, status, kafkaTemplate);
    }

    private byte[] binaryPartOf(Http2Request request) {
        byte[] body = request.getBody();
        byte[] marker = "Content-Id: n1-sms-payload\r\n\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        int start = indexOf(body, marker, 0) + marker.length;
        String contentType = request.getHeaders().get(HEADER_CONTENT_TYPE);
        byte[] boundary = ("\r\n--" + contentType.split("boundary=\"")[1].split("\"")[0])
                .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        return java.util.Arrays.copyOfRange(body, start, indexOf(body, boundary, start));
    }

    private int indexOf(byte[] haystack, byte[] needle, int from) {
        outer:
        for (int i = from; i <= haystack.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) continue outer;
            }
            return i;
        }
        return -1;
    }
}
