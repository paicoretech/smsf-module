package com.paicbd.smsc.smsf.core.api.controller;

import com.paicbd.smsc.smsf.core.config.AppProperties;
import com.paicbd.smsc.smsf.core.api.service.RegisterService;
import com.paicore.sba.stack.openapi.model.AccessType;
import com.paicore.sba.stack.openapi.model.UeSmsContextData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UeContextControllerTest {

    private static final String BASE_PATH = "/nsmsf-sms/v2";
    private static final String VALID_SUPI = "imsi-460000000000001";
    private static final int NETWORK_ID = 11;
    private static final String ETAG = "\"abc123def4567890\"";

    private RegisterService registerService;
    private WebTestClient client;

    @BeforeEach
    void setUp() {
        AppProperties props = mock(AppProperties.class);
        when(props.getSmsfSbiServerBasePath()).thenReturn(BASE_PATH);

        registerService = mock(RegisterService.class);
        UeContextController controller = new UeContextController(registerService, props);
        client = WebTestClient.bindToController(controller)
                .configureClient()
                .baseUrl("http://localhost")
                .build();
    }

    @Test
    @DisplayName("PUT first time → 201 with Location and ETag")
    void putCreates() {
        when(registerService.upsertUeContext(any(UeSmsContextData.class)))
                .thenReturn(new RegisterService.UpsertResult(RegisterService.Outcome.CREATED, ETAG));

        client.put().uri("/{nid}/ue-contexts/{supi}", NETWORK_ID, VALID_SUPI)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(sampleBody(VALID_SUPI))
                .exchange()
                .expectStatus().isCreated()
                .expectHeader().exists(HttpHeaders.LOCATION)
                .expectHeader().valueEquals(HttpHeaders.LOCATION,
                        BASE_PATH + "/" + NETWORK_ID + "/ue-contexts/" + VALID_SUPI)
                .expectHeader().valueEquals(HttpHeaders.ETAG, ETAG)
                .expectHeader().contentType(MediaType.APPLICATION_JSON)
                .expectBody()
                .jsonPath("$.supi").isEqualTo(VALID_SUPI)
                .jsonPath("$.gpsi").isEqualTo("msisdn-8613915900000");
    }

    @Test
    @DisplayName("PUT existing → 204 with ETag")
    void putUpdates() {
        when(registerService.upsertUeContext(any(UeSmsContextData.class)))
                .thenReturn(new RegisterService.UpsertResult(RegisterService.Outcome.UPDATED, ETAG));

        client.put().uri("/{nid}/ue-contexts/{supi}", NETWORK_ID, VALID_SUPI)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(sampleBody(VALID_SUPI))
                .exchange()
                .expectStatus().isNoContent()
                .expectHeader().valueEquals(HttpHeaders.ETAG, ETAG);
    }

    @Test
    @DisplayName("PUT with invalid SUPI in path → 400 INVALID_SUPI")
    void putRejectsBadSupiInPath() {
        client.put().uri("/{nid}/ue-contexts/{supi}", NETWORK_ID, "not-a-supi")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(sampleBody(VALID_SUPI))
                .exchange()
                .expectStatus().isBadRequest()
                .expectHeader().contentType("application/problem+json")
                .expectBody()
                .jsonPath("$.cause").isEqualTo("INVALID_SUPI");

        verify(registerService, never()).upsertUeContext(any());
    }

    @Test
    @DisplayName("PUT with body SUPI mismatching path → 400 INVALID_SUPI")
    void putRejectsBodyPathMismatch() {
        client.put().uri("/{nid}/ue-contexts/{supi}", NETWORK_ID, VALID_SUPI)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(sampleBody("imsi-460000000000999"))
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.cause").isEqualTo("INVALID_SUPI")
                .jsonPath("$.detail").isEqualTo("Body SUPI does not match the path");

        verify(registerService, never()).upsertUeContext(any());
    }

    @Test
    @DisplayName("DELETE existing → 204, and the deregistration is published")
    void deleteRemoves() {
        when(registerService.deleteUeContext(anyString(), any()))
                .thenReturn(new RegisterService.DeleteResult(RegisterService.Outcome.DELETED));

        client.delete().uri("/{nid}/ue-contexts/{supi}", NETWORK_ID, VALID_SUPI)
                .exchange().expectStatus().isNoContent();

        verify(registerService).publishDeregistration(NETWORK_ID, VALID_SUPI);
    }

    @Test
    @DisplayName("DELETE non-existing → 204, but nothing is published to the UDM")
    void deleteIdempotent() {
        when(registerService.deleteUeContext(anyString(), any()))
                .thenReturn(new RegisterService.DeleteResult(RegisterService.Outcome.NOT_FOUND));

        // Still 204: an AMF retrying a DELETE it never saw the answer to must not get an error.
        client.delete().uri("/{nid}/ue-contexts/{supi}", NETWORK_ID, "imsi-460000000000777")
                .exchange().expectStatus().isNoContent();

        // ...but there is nothing registered at the UDM, so no DEREGISTER travels.
        verify(registerService, never()).publishDeregistration(anyInt(), anyString());
    }

    @Test
    @DisplayName("DELETE with If-Match that does not match → 412 ETAG_MISMATCH")
    void deleteMismatchingEtag() {
        when(registerService.deleteUeContext(anyString(), any()))
                .thenReturn(new RegisterService.DeleteResult(RegisterService.Outcome.ETAG_MISMATCH));

        client.delete().uri("/{nid}/ue-contexts/{supi}", NETWORK_ID, VALID_SUPI)
                .header(HttpHeaders.IF_MATCH, "\"deadbeefdeadbeef\"")
                .exchange()
                .expectStatus().isEqualTo(412)
                .expectHeader().contentType("application/problem+json")
                .expectBody()
                .jsonPath("$.cause").isEqualTo("ETAG_MISMATCH");
    }

    @Test
    @DisplayName("DELETE with invalid SUPI → 400 INVALID_SUPI")
    void deleteRejectsBadSupi() {
        client.delete().uri("/{nid}/ue-contexts/{supi}", NETWORK_ID, "garbage")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.cause").isEqualTo("INVALID_SUPI");

        verify(registerService, never()).deleteUeContext(anyString(), any());
    }

    private static UeSmsContextData sampleBody(String supi) {
        return new UeSmsContextData()
                .supi(supi)
                .gpsi("msisdn-8613915900000")
                .pei("imei-123456789012345")
                .accessType(AccessType._3_GPP_ACCESS);
    }
}
