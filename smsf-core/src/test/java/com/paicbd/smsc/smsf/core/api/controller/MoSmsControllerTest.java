package com.paicbd.smsc.smsf.core.api.controller;

import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.core.api.exception.DlrCorrelationNotFoundException;
import com.paicbd.smsc.smsf.core.api.service.MoSmsService;
import com.paicore.sba.stack.openapi.model.SmsDeliveryStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MoSmsControllerTest {

    private static final int NETWORK_ID = 11;
    private static final String VALID_SUPI = "imsi-460000000000001";
    private static final String INVALID_SUPI = "not-a-supi";
    private static final String MULTIPART = "multipart/related;boundary=abc";
    private static final byte[] SAMPLE_BODY = "sample-body".getBytes();
    private static final String MESSAGE_ID = "1783981200123-86234715900123";
    private static final MediaType PROBLEM_JSON = MediaType.valueOf(Constants.PROBLEM_JSON_CONTENT_TYPE);

    private MoSmsService moSmsService;
    private WebTestClient client;

    @BeforeEach
    void setUp() {
        moSmsService = mock(MoSmsService.class);
        MoSmsController controller = new MoSmsController(moSmsService);
        client = WebTestClient.bindToController(controller)
                .configureClient()
                .baseUrl("http://localhost")
                .build();
    }

    @Test
    @DisplayName("PUT sendsms with valid SUPI and registered UE Context → 200 OK echoing SmsRecordDeliveryData")
    void sendSmsAcceptedReturnsOk() {
        when(moSmsService.processMoSms(anyInt(), anyString(), anyString(), any(byte[].class)))
                .thenReturn(MESSAGE_ID);

        client.post().uri("/{nid}/ue-contexts/{supi}/sendsms", NETWORK_ID, VALID_SUPI)
                .contentType(MediaType.valueOf(MULTIPART))
                .bodyValue(SAMPLE_BODY)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentType(MediaType.APPLICATION_JSON)
                .expectBody()
                .jsonPath("$.smsRecordId").isEqualTo(MESSAGE_ID)
                .jsonPath("$.deliveryStatus").isEqualTo(SmsDeliveryStatus.SMSF_ACCEPTED.getValue());

        verify(moSmsService).processMoSms(eq(NETWORK_ID), eq(VALID_SUPI), anyString(), any(byte[].class));
    }

    @Test
    @DisplayName("PUT sendsms with invalid SUPI → 400 problem+json with INVALID_SUPI cause")
    void sendSmsRejectsInvalidSupi() {
        client.post().uri("/{nid}/ue-contexts/{supi}/sendsms", NETWORK_ID, INVALID_SUPI)
                .contentType(MediaType.valueOf(MULTIPART))
                .bodyValue(SAMPLE_BODY)
                .exchange()
                .expectStatus().isBadRequest()
                .expectHeader().contentType(PROBLEM_JSON)
                .expectBody()
                .jsonPath("$.status").isEqualTo(400)
                .jsonPath("$.cause").isEqualTo(Constants.CAUSE_INVALID_SUPI)
                .jsonPath("$.instance")
                    .isEqualTo(Constants.UE_CONTEXTS_PATH + "/" + INVALID_SUPI + Constants.SENDSMS_SUFFIX);

        verify(moSmsService, never()).processMoSms(anyInt(), anyString(), anyString(), any(byte[].class));
    }

    @Test
    @DisplayName("PUT sendsms → 500 problem+json when the service throws")
    void sendSmsMapsServiceExceptionToInternalError() {
        when(moSmsService.processMoSms(anyInt(), anyString(), anyString(), any(byte[].class)))
                .thenThrow(new IllegalStateException("decode boom"));

        client.post().uri("/{nid}/ue-contexts/{supi}/sendsms", NETWORK_ID, VALID_SUPI)
                .contentType(MediaType.valueOf(MULTIPART))
                .bodyValue(SAMPLE_BODY)
                .exchange()
                .expectStatus().is5xxServerError()
                .expectHeader().contentType(PROBLEM_JSON)
                .expectBody()
                .jsonPath("$.status").isEqualTo(500)
                .jsonPath("$.detail").isEqualTo("decode boom");
    }

    @Test
    @DisplayName("PUT sendsms → 404 problem+json with CONTEXT_NOT_FOUND when DLR correlation is missing")
    void sendSmsMapsMissingDlrCorrelationToNotFound() {
        when(moSmsService.processMoSms(anyInt(), anyString(), anyString(), any(byte[].class)))
                .thenThrow(new DlrCorrelationNotFoundException("DLR correlation not found for smsRecordId " + MESSAGE_ID));

        client.post().uri("/{nid}/ue-contexts/{supi}/sendsms", NETWORK_ID, VALID_SUPI)
                .contentType(MediaType.valueOf(MULTIPART))
                .bodyValue(SAMPLE_BODY)
                .exchange()
                .expectStatus().isNotFound()
                .expectHeader().contentType(PROBLEM_JSON)
                .expectBody()
                .jsonPath("$.status").isEqualTo(404)
                .jsonPath("$.cause").isEqualTo(Constants.CAUSE_CONTEXT_NOT_FOUND)
                .jsonPath("$.detail").isEqualTo("DLR correlation not found for smsRecordId " + MESSAGE_ID);
    }
}
