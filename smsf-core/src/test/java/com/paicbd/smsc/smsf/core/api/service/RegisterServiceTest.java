package com.paicbd.smsc.smsf.core.api.service;

import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.smsf.common.event.SmsfRegistrationEvent;
import com.paicbd.smsc.smsf.common.event.RegistrationAction;
import com.paicore.sba.stack.openapi.model.AccessType;
import com.paicore.sba.stack.openapi.model.UeSmsContextData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RegisterServiceTest {

    private static final String SUPI = "imsi-460000000000001";
    private static final int NETWORK_ID = 11;

    private static final String REGISTER_TOPIC = NETWORK_ID + KafkaTopicsConstants.SMSF_UDM_REGISTER_TOPIC_SUFFIX;

    @Mock
    private ScyllaManager scyllaManager;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private RegisterService registerService;

    @BeforeEach
    void setUp() {
        registerService = new RegisterService(scyllaManager, kafkaTemplate);
    }

    /** Captures the JSON the service published and parses it back into the shared event contract. */
    private SmsfRegistrationEvent capturePublishedEvent(String expectedKey) {
        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(REGISTER_TOPIC), eq(expectedKey), payload.capture());
        return Converter.stringToObject(payload.getValue(), SmsfRegistrationEvent.class);
    }

    @Test
    @DisplayName("upsertUeContext returns CREATED with exact etag when SUPI does not exist yet")
    void upsertUeContextCreatesWhenAbsent() {
        UeSmsContextData body = sampleBody();
        when(scyllaManager.existsSmsfUeSmsContextBySupi(SUPI)).thenReturn(false);

        RegisterService.UpsertResult result = registerService.upsertUeContext(body);

        assertThat(result.outcome()).isEqualTo(RegisterService.Outcome.CREATED);
        assertThat(result.etag())
                .startsWith("\"")
                .endsWith("\"")
                .hasSize(18)
                .isEqualTo(expectedEtag(body));

        InOrder inOrder = inOrder(scyllaManager);
        inOrder.verify(scyllaManager).existsSmsfUeSmsContextBySupi(SUPI);
        inOrder.verify(scyllaManager).upsertSmsfUeSmsContext(body);
    }

    @Test
    @DisplayName("upsertUeContext returns UPDATED with exact etag when SUPI already exists")
    void upsertUeContextUpdatesWhenPresent() {
        UeSmsContextData body = sampleBody();
        when(scyllaManager.existsSmsfUeSmsContextBySupi(SUPI)).thenReturn(true);

        RegisterService.UpsertResult result = registerService.upsertUeContext(body);

        assertThat(result.outcome()).isEqualTo(RegisterService.Outcome.UPDATED);
        assertThat(result.etag()).isEqualTo(expectedEtag(body));
        verify(scyllaManager).upsertSmsfUeSmsContext(body);
    }

    @Test
    @DisplayName("upsertUeContext etag changes when payload changes")
    void upsertUeContextEtagDependsOnPayload() {
        UeSmsContextData first = sampleBody();
        UeSmsContextData second = sampleBody().gpsi("msisdn-8613999999999");
        when(scyllaManager.existsSmsfUeSmsContextBySupi(SUPI)).thenReturn(false);

        String firstEtag = registerService.upsertUeContext(first).etag();
        String secondEtag = registerService.upsertUeContext(second).etag();

        assertThat(secondEtag).isNotEqualTo(expectedEtag(first));
        assertThat(firstEtag).isEqualTo(expectedEtag(first));
        assertThat(secondEtag).isEqualTo(expectedEtag(second));
    }

    @Test
    @DisplayName("upsertUeContext etag is stable for the same payload")
    void upsertUeContextEtagIsStable() {
        UeSmsContextData body = sampleBody();
        when(scyllaManager.existsSmsfUeSmsContextBySupi(SUPI)).thenReturn(true);

        String etagA = registerService.upsertUeContext(body).etag();
        String etagB = registerService.upsertUeContext(sampleBody()).etag();

        assertThat(etagA).isEqualTo(etagB);
    }

    @Test
    @DisplayName("upsertUeContext propagates Scylla errors after checking existence")
    void upsertUeContextPropagatesScyllaErrors() {
        UeSmsContextData body = sampleBody();
        when(scyllaManager.existsSmsfUeSmsContextBySupi(SUPI)).thenReturn(false);
        doThrow(new IllegalStateException("scylla down"))
                .when(scyllaManager).upsertSmsfUeSmsContext(body);

        assertThatThrownBy(() -> registerService.upsertUeContext(body))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("scylla down");
        verify(scyllaManager).existsSmsfUeSmsContextBySupi(SUPI);
    }

    @Test
    @DisplayName("upsertUeContext forwards the exact body and payload fields to Scylla")
    void upsertUeContextForwardsBody() {
        UeSmsContextData body = sampleBody();
        when(scyllaManager.existsSmsfUeSmsContextBySupi(SUPI)).thenReturn(false);

        registerService.upsertUeContext(body);

        ArgumentCaptor<UeSmsContextData> captor = ArgumentCaptor.forClass(UeSmsContextData.class);
        verify(scyllaManager).upsertSmsfUeSmsContext(captor.capture());
        assertThat(captor.getValue()).isSameAs(body);
        assertThat(captor.getValue())
                .extracting(UeSmsContextData::getSupi, UeSmsContextData::getGpsi,
                        UeSmsContextData::getPei, UeSmsContextData::getAccessType)
                .containsExactly(SUPI, "msisdn-8613915900000", "imei-123456789012345",
                        AccessType._3_GPP_ACCESS);
        verify(scyllaManager).existsSmsfUeSmsContextBySupi(SUPI);
    }

    @Test
    @DisplayName("deleteUeContext without If-Match checks existence with a key-only read, never the document")
    void deleteUeContextWithoutIfMatch() {
        when(scyllaManager.existsSmsfUeSmsContextBySupi(SUPI)).thenReturn(true);

        RegisterService.DeleteResult result = registerService.deleteUeContext(SUPI, null);

        assertThat(result.outcome()).isEqualTo(RegisterService.Outcome.DELETED);
        verify(scyllaManager, never()).getSmsfUeSmsContextBySupi(SUPI);

        InOrder inOrder = inOrder(scyllaManager);
        inOrder.verify(scyllaManager).existsSmsfUeSmsContextBySupi(SUPI);
        inOrder.verify(scyllaManager).deleteSmsfUeSmsContextBySupi(SUPI);
    }

    @Test
    @DisplayName("deleteUeContext on a SUPI that is not stored reports NOT_FOUND and writes no tombstone")
    void deleteUeContextWithoutIfMatchOnMissingRow() {
        when(scyllaManager.existsSmsfUeSmsContextBySupi(SUPI)).thenReturn(false);

        RegisterService.DeleteResult result = registerService.deleteUeContext(SUPI, null);

        assertThat(result.outcome()).isEqualTo(RegisterService.Outcome.NOT_FOUND);
        verify(scyllaManager, never()).deleteSmsfUeSmsContextBySupi(SUPI);
        verify(scyllaManager, never()).getSmsfUeSmsContextBySupi(SUPI);
    }

    @Test
    @DisplayName("deleteUeContext with wildcard If-Match deletes without reading current state")
    void deleteUeContextWildcardIfMatch() {
        when(scyllaManager.existsSmsfUeSmsContextBySupi(SUPI)).thenReturn(true);

        RegisterService.DeleteResult result = registerService.deleteUeContext(SUPI, " * ");

        assertThat(result.outcome()).isEqualTo(RegisterService.Outcome.DELETED);
        verify(scyllaManager, never()).getSmsfUeSmsContextBySupi(SUPI);
        verify(scyllaManager).deleteSmsfUeSmsContextBySupi(SUPI);
    }

    @Test
    @DisplayName("deleteUeContext with matching If-Match reads current row before deleting")
    void deleteUeContextWithMatchingIfMatch() {
        UeSmsContextData current = sampleBody();
        String etag = expectedEtag(current);
        when(scyllaManager.getSmsfUeSmsContextBySupi(SUPI)).thenReturn(current);

        RegisterService.DeleteResult result = registerService.deleteUeContext(SUPI, " " + etag + " ");

        assertThat(result.outcome()).isEqualTo(RegisterService.Outcome.DELETED);

        InOrder inOrder = inOrder(scyllaManager);
        inOrder.verify(scyllaManager).getSmsfUeSmsContextBySupi(SUPI);
        inOrder.verify(scyllaManager).deleteSmsfUeSmsContextBySupi(SUPI);
        // The conditional path pays no extra read: the row it already fetched proves existence.
        verify(scyllaManager, never()).existsSmsfUeSmsContextBySupi(SUPI);
    }

    @Test
    @DisplayName("deleteUeContext with mismatching If-Match returns ETAG_MISMATCH and does not delete")
    void deleteUeContextWithMismatchingIfMatch() {
        UeSmsContextData current = sampleBody();
        when(scyllaManager.getSmsfUeSmsContextBySupi(SUPI)).thenReturn(current);

        RegisterService.DeleteResult result = registerService.deleteUeContext(SUPI, "\"deadbeefdeadbeef\"");

        assertThat(result.outcome()).isEqualTo(RegisterService.Outcome.ETAG_MISMATCH);
        verify(scyllaManager).getSmsfUeSmsContextBySupi(SUPI);
        verify(scyllaManager, never()).deleteSmsfUeSmsContextBySupi(SUPI);
    }

    @Test
    @DisplayName("deleteUeContext with If-Match when the row is gone reports NOT_FOUND for free")
    void deleteUeContextWithIfMatchOnMissingRow() {
        when(scyllaManager.getSmsfUeSmsContextBySupi(SUPI)).thenReturn(null);

        RegisterService.DeleteResult result = registerService.deleteUeContext(SUPI, "\"abc1234567890def\"");

        assertThat(result.outcome()).isEqualTo(RegisterService.Outcome.NOT_FOUND);
        verify(scyllaManager).getSmsfUeSmsContextBySupi(SUPI);
        verify(scyllaManager, never()).deleteSmsfUeSmsContextBySupi(SUPI);
        verify(scyllaManager, never()).existsSmsfUeSmsContextBySupi(SUPI);
    }

    @Test
    @DisplayName("deleteUeContext propagates Scylla errors when reading current state")
    void deleteUeContextPropagatesReadErrors() {
        when(scyllaManager.getSmsfUeSmsContextBySupi(SUPI))
                .thenThrow(new IllegalStateException("scylla read down"));

        assertThatThrownBy(() -> registerService.deleteUeContext(SUPI, "\"abc1234567890de\""))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("scylla read down");
        verify(scyllaManager, never()).deleteSmsfUeSmsContextBySupi(SUPI);
    }

    @Test
    @DisplayName("deleteUeContext propagates Scylla errors when issuing the deletion")
    void deleteUeContextPropagatesDeleteErrors() {
        when(scyllaManager.existsSmsfUeSmsContextBySupi(SUPI)).thenReturn(true);
        doThrow(new IllegalStateException("scylla delete down"))
                .when(scyllaManager).deleteSmsfUeSmsContextBySupi(SUPI);

        assertThatThrownBy(() -> registerService.deleteUeContext(SUPI, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("scylla delete down");
        verify(scyllaManager, never()).getSmsfUeSmsContextBySupi(SUPI);
    }

    @Test
    @DisplayName("publishRegistration emits a REGISTER event carrying the UE context, keyed by SUPI")
    void publishRegistrationEmitsRegisterEvent() {
        UeSmsContextData body = sampleBody();

        registerService.publishRegistration(NETWORK_ID, body);

        SmsfRegistrationEvent event = capturePublishedEvent(SUPI);
        assertThat(event.getAction()).isEqualTo(RegistrationAction.REGISTER);
        assertThat(event.getNetworkId()).isEqualTo(NETWORK_ID);
        assertThat(event.getSupi()).isEqualTo(SUPI);
        assertThat(event.getTimestamp()).isPositive();
        assertThat(event.getUeContext()).isNotNull();
        assertThat(event.getUeContext())
                .extracting(UeSmsContextData::getSupi, UeSmsContextData::getGpsi,
                        UeSmsContextData::getPei, UeSmsContextData::getAccessType)
                .containsExactly(SUPI, "msisdn-8613915900000", "imei-123456789012345",
                        AccessType._3_GPP_ACCESS);
    }

    @Test
    @DisplayName("publishDeregistration emits a DEREGISTER event with no UE context")
    void publishDeregistrationEmitsDeregisterEvent() {
        registerService.publishDeregistration(NETWORK_ID, SUPI);

        SmsfRegistrationEvent event = capturePublishedEvent(SUPI);
        assertThat(event.getAction()).isEqualTo(RegistrationAction.DEREGISTER);
        assertThat(event.getNetworkId()).isEqualTo(NETWORK_ID);
        assertThat(event.getSupi()).isEqualTo(SUPI);
        // Only the SUPI is needed to remove the context at the UDM.
        assertThat(event.getUeContext()).isNull();
    }

    @Test
    @DisplayName("the topic is derived from the networkId, so each gateway gets its own")
    void publishUsesThePerGatewayTopic() {
        int otherNetworkId = 42;

        registerService.publishRegistration(otherNetworkId, sampleBody());

        verify(kafkaTemplate).send(
                eq(otherNetworkId + KafkaTopicsConstants.SMSF_UDM_REGISTER_TOPIC_SUFFIX),
                eq(SUPI), any(String.class));
    }

    @Test
    @DisplayName("a Kafka failure is swallowed: the UE context is already persisted and the AMF was answered")
    void publishSwallowsKafkaFailures() {
        doThrow(new IllegalStateException("broker down"))
                .when(kafkaTemplate).send(eq(REGISTER_TOPIC), eq(SUPI), any(String.class));

        registerService.publishRegistration(NETWORK_ID, sampleBody());
        registerService.publishDeregistration(NETWORK_ID, SUPI);

        verifyNoInteractions(scyllaManager);
    }

    private static UeSmsContextData sampleBody() {
        return new UeSmsContextData()
                .supi(SUPI)
                .gpsi("msisdn-8613915900000")
                .pei("imei-123456789012345")
                .accessType(AccessType._3_GPP_ACCESS);
    }

    private static String expectedEtag(UeSmsContextData body) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(Converter.valueAsString(body).getBytes(StandardCharsets.UTF_8));
            return "\"" + HexFormat.of().formatHex(md.digest()).substring(0, 16) + "\"";
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 should be available in the test JVM", ex);
        }
    }
}
