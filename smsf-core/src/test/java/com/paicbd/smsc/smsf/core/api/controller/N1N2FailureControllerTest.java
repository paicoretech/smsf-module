package com.paicbd.smsc.smsf.core.api.controller;

import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.core.api.service.N1N2FailureService;
import com.paicore.sba.stack.openapi.model.N1N2MessageTransferCause;
import com.paicore.sba.stack.openapi.model.N1N2MsgTxfrFailureNotification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class N1N2FailureControllerTest {

    private static final int NETWORK_ID = 11;
    private static final String MESSAGE_ID = "1783981200123-86234715900123";
    private static final MediaType PROBLEM_JSON = MediaType.valueOf(Constants.PROBLEM_JSON_CONTENT_TYPE);

    private N1N2FailureService n1n2FailureService;
    private WebTestClient client;

    @BeforeEach
    void setUp() {
        n1n2FailureService = mock(N1N2FailureService.class);
        client = WebTestClient.bindToController(new N1N2FailureController(n1n2FailureService))
                .configureClient()
                .baseUrl("http://localhost")
                .build();
    }

    @Test
    @DisplayName("POST n1n2-failure, when the AMF reports a cause, then 204 and the notification is relayed")
    void failureNotificationIsRelayed() {
        client.post().uri("/{nid}/namf-cb/n1n2-failure/{messageId}", NETWORK_ID, MESSAGE_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(notification(N1N2MessageTransferCause.UE_NOT_RESPONDING))
                .exchange()
                .expectStatus().isNoContent();

        verify(n1n2FailureService).processFailureNotification(eq(NETWORK_ID), eq(MESSAGE_ID),
                any(N1N2MsgTxfrFailureNotification.class));
    }

    @Test
    @DisplayName("POST n1n2-failure, when the mandatory cause is missing, then 400 with a ProblemDetails")
    void failureNotificationWithoutCauseIsRejected() {
        client.post().uri("/{nid}/namf-cb/n1n2-failure/{messageId}", NETWORK_ID, MESSAGE_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new N1N2MsgTxfrFailureNotification())
                .exchange()
                .expectStatus().isBadRequest()
                .expectHeader().contentType(PROBLEM_JSON)
                .expectBody()
                .jsonPath("$.cause").isEqualTo("MANDATORY_IE_MISSING");

        verify(n1n2FailureService, never()).processFailureNotification(anyInt(), anyString(), any());
    }

    @Test
    @DisplayName("POST n1n2-failure, when the relay fails, then 500 and the AMF can retry")
    void failureNotificationRelayErrorIsReported() {
        doThrow(new IllegalStateException("kafka down"))
                .when(n1n2FailureService).processFailureNotification(anyInt(), anyString(), any());

        client.post().uri("/{nid}/namf-cb/n1n2-failure/{messageId}", NETWORK_ID, MESSAGE_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(notification(N1N2MessageTransferCause.N1_MSG_NOT_TRANSFERRED))
                .exchange()
                .expectStatus().is5xxServerError();
    }

    private N1N2MsgTxfrFailureNotification notification(N1N2MessageTransferCause cause) {
        return new N1N2MsgTxfrFailureNotification().cause(cause).retryAfter(30).maxWaitingTime(1800);
    }
}
