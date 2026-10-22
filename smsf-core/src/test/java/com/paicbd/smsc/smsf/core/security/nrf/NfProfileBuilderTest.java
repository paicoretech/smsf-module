package com.paicbd.smsc.smsf.core.security.nrf;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.smsf.core.config.AppProperties;
import com.paicore.sba.stack.openapi.model.NFProfile;
import com.paicore.sba.stack.openapi.model.NFService;
import com.paicore.sba.stack.openapi.model.NFServiceStatus;
import com.paicore.sba.stack.openapi.model.NFStatus;
import com.paicore.sba.stack.openapi.model.NFType;
import com.paicore.sba.stack.openapi.model.ServiceName;
import com.paicore.sba.stack.openapi.model.TransportProtocol;
import com.paicore.sba.stack.openapi.model.UriScheme;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NfProfileBuilderTest {

    private AppProperties props;

    @BeforeEach
    void setUp() {
        props = mock(AppProperties.class);
        when(props.getSmsfSbiServerBasePath()).thenReturn("/nsmsf-sms/v2");
        when(props.getNrfApiFullVersion()).thenReturn("2.0.0");
        when(props.getNrfHeartbeatDefaultSeconds()).thenReturn(60);
        when(props.getSmsfSbiPublicAddress()).thenReturn("10.20.30.40");
        when(props.getServerPort()).thenReturn(9070);
    }

    @Test
    void buildProfileFillsMandatoryFields() {
        UUID nfInstanceId = UUID.randomUUID();
        Http2Gateway gateway = Http2Gateway.builder()
                .networkId(11)
                .nfInstanceId(nfInstanceId)
                .build();

        NFProfile profile = NfProfileBuilder.build(gateway, props);

        assertEquals(nfInstanceId, profile.getNfInstanceId());
        assertEquals(NFType.SMSF, profile.getNfType());
        assertEquals(NFStatus.REGISTERED, profile.getNfStatus());
        assertEquals(60, profile.getHeartBeatTimer());
        assertTrue(profile.getPlmnList() == null || profile.getPlmnList().isEmpty());
        assertEquals(1, profile.getIpv4Addresses().size());
        assertEquals("10.20.30.40", profile.getIpv4Addresses().get(0));
    }

    @Test
    @DisplayName("a gateway with no nf_instance_id is rejected instead of getting a generated one")
    void buildProfileRejectsAGatewayWithoutNfInstanceId() {
        // It used to mint a random UUID here. That registered the SMSF under an identity the
        // platform did not know, and a new one on every restart orphaned the previous profile in
        // the NRF. The identity belongs to smsc-management-be and arrives through Redis.
        Http2Gateway gateway = Http2Gateway.builder().networkId(7).build();

        assertThatThrownBy(() -> NfProfileBuilder.build(gateway, props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Gateway 7 has no nf_instance_id");
    }

    @Test
    void buildProfileFillsNfServiceWithExpectedEndpoint() {
        UUID nfInstanceId = UUID.randomUUID();
        Http2Gateway gateway = Http2Gateway.builder()
                .networkId(42)
                .nfInstanceId(nfInstanceId)
                .build();

        NFProfile profile = NfProfileBuilder.build(gateway, props);

        String serviceInstanceId = nfInstanceId + "-nsmsf-sms";
        assertEquals(1, profile.getNfServiceList().size());
        NFService service = profile.getNfServiceList().get(serviceInstanceId);
        assertNotNull(service);
        assertEquals(serviceInstanceId, service.getServiceInstanceId());
        assertEquals(ServiceName.NSMSF_SMS, service.getServiceName());
        assertEquals(UriScheme.HTTPS, service.getScheme());
        assertEquals(NFServiceStatus.REGISTERED, service.getNfServiceStatus());
        assertEquals("/nsmsf-sms/v2/42", service.getApiPrefix());
        assertEquals(1, service.getVersions().size());
        assertEquals("v2", service.getVersions().get(0).getApiVersionInUri());
        assertEquals("2.0.0", service.getVersions().get(0).getApiFullVersion());
        assertEquals(1, service.getIpEndPoints().size());
        assertEquals("10.20.30.40", service.getIpEndPoints().get(0).getIpv4Address());
        assertEquals(9070, service.getIpEndPoints().get(0).getPort());
        assertEquals(TransportProtocol.TCP, service.getIpEndPoints().get(0).getTransport());
    }
}
