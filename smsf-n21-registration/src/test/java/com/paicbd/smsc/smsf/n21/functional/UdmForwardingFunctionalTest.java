package com.paicbd.smsc.smsf.n21.functional;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.Http2GatewayUDMSecurity;
import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenCache;
import com.paicbd.smsc.smsf.common.utils.Http2GatewayCreator;
import com.paicbd.smsc.smsf.n21.config.AppProperties;
import com.paicbd.smsc.smsf.n21.stack.N21StackManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.smsf.common.event.SmsfRegistrationEvent;
import com.paicbd.smsc.smsf.common.event.RegistrationAction;
import com.paicore.sba.stack.openapi.model.AccessType;
import com.paicore.sba.stack.openapi.model.UeSmsContextData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.kafka.core.ConsumerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.paicbd.smsc.smsf.common.constants.Constants.ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Functional test of the N21 forwarding: a real Kafka payload driven through the module's listener,
 * out over real HTTP/2 to a simulated UDM, carrying the bearer the token cache holds.
 *
 * <p>Nothing between {@code N21Client} and the wire is mocked. Kafka is represented by a
 * {@code ConsumerRecord} handed to the same listener the container would call, and Redis by the
 * token cache at its boundary.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class UdmForwardingFunctionalTest {

    private static final int NETWORK_ID = 10;
    private static final String SUPI = "imsi-460000000000001";
    private static final String REGISTRATION_PATH = "/nudm-uecm/v1/{supi}/registrations/smsf-3gpp-access";
    private static final String EXPECTED_URL = "/nudm-uecm/v1/" + SUPI + "/registrations/smsf-3gpp-access";
    private static final String BEARER = "Bearer udm-access-token";

    @Mock
    private AppProperties appProperties;

    @Mock
    private ConsumerFactory<String, String> consumerFactory;

    @Mock
    private SmsfTokenCache tokenCache;

    private WireMockServer udm;
    private int udmPort;
    private Http2Gateway gateway;
    private N21StackManager manager;

    @BeforeEach
    void setUp() {
        udm = new WireMockServer(wireMockConfig().dynamicPort());
        udm.start();
        udmPort = udm.port();

        gateway = Http2GatewayCreator.buildHttp2GatewayWithMtls("smsf-gw", NETWORK_ID, "amf.local:8443", ENABLED);
        // The registration payload is built around it; without one the mapper refuses to build.
        gateway.setNfInstanceId(java.util.UUID.fromString("11111111-2222-3333-4444-555555555555"));
        Http2GatewayUDMSecurity udmSecurity = new Http2GatewayUDMSecurity();
        udmSecurity.setForwardRegistrationEnabled(true);
        udmSecurity.setOauth2Enabled(true);
        udmSecurity.setAuthorizationHeader("authorization");
        // Direct apiRoot: NF discovery is exercised by its own tests, the forwarding is what is under test.
        udmSecurity.setNrfDiscoveryEnabled(false);
        udmSecurity.setApiRoot("http://localhost:" + udmPort);
        udmSecurity.setRegistrationPath(REGISTRATION_PATH);
        udmSecurity.setDeregistrationPath(REGISTRATION_PATH);
        udmSecurity.setMtlsEnabled(false);
        gateway.setUdmSecurity(udmSecurity);

        when(appProperties.getOutboundTokenRefreshMarginSeconds()).thenReturn(30L);
        when(appProperties.getTokenCheckIntervalSeconds()).thenReturn(10L);
        when(appProperties.getN21RestartMinIntervalSeconds()).thenReturn(30L);
        when(appProperties.getN21DiscoveryCacheTtlSeconds()).thenReturn(300);
        when(appProperties.getN21MaxEndpoints()).thenReturn(4);
        // Without a home PLMN the mapper drops the registration rather than inventing one, which is
        // the documented behaviour; a functional test of the forwarding has to supply it.
        when(appProperties.getN21HomePlmnMcc()).thenReturn("460");
        when(appProperties.getN21HomePlmnMnc()).thenReturn("01");
        when(tokenCache.isAvailable(NETWORK_ID, Constants.NF_TYPE_UDM)).thenReturn(true);
        when(tokenCache.authorizationHeader(NETWORK_ID, Constants.NF_TYPE_UDM))
                .thenReturn(Optional.of(BEARER));

        manager = new N21StackManager(gateway, appProperties, tokenCache, consumerFactory);
    }

    @AfterEach
    void tearDown() {
        udm.stop();
    }

    /** Builds the N21 client and marks the stack started, without the Kafka container. */
    private void startClient() throws Exception {
        setStarted();
        invoke("startN21Client");
        assertThat(manager.getN21Client())
                .as("the N21 client should have been built against the simulated UDM")
                .isNotNull();
    }

    private static UeSmsContextData ueContext() {
        return new UeSmsContextData()
                .supi(SUPI)
                .gpsi("msisdn-8613915900000")
                .accessType(AccessType._3_GPP_ACCESS);
    }

    private String registerEvent() {
        return Converter.valueAsString(SmsfRegistrationEvent.builder()
                .networkId(NETWORK_ID).supi(SUPI).action(RegistrationAction.REGISTER)
                .ueContext(ueContext()).timestamp(Instant.now().toEpochMilli()).build());
    }

    private String deregisterEvent() {
        return Converter.valueAsString(SmsfRegistrationEvent.builder()
                .networkId(NETWORK_ID).supi(SUPI).action(RegistrationAction.DEREGISTER)
                .timestamp(Instant.now().toEpochMilli()).build());
    }

    // --- real traffic ---------------------------------------------------------

    @Test
    @DisplayName("a REGISTER event becomes a real PUT to the UDM carrying the cached bearer")
    void registerEventReachesTheUdm() throws Exception {
        udm.stubFor(put(urlPathMatching(".*/registrations/smsf-3gpp-access")).willReturn(aResponse()
                .withStatus(201).withHeader("content-type", "application/json").withBody("{}")));
        startClient();

        onEvent(registerEvent());

        udm.verify(putRequestedFor(urlPathMatching(EXPECTED_URL))
                .withHeader("authorization", equalTo(BEARER))
                .withRequestBody(containing("smsfInstanceId")));
    }

    @Test
    @DisplayName("a DEREGISTER event becomes a real DELETE, with no body needed")
    void deregisterEventReachesTheUdm() throws Exception {
        udm.stubFor(delete(urlPathMatching(".*/registrations/smsf-3gpp-access"))
                .willReturn(aResponse().withStatus(204)));
        startClient();

        onEvent(deregisterEvent());

        udm.verify(deleteRequestedFor(urlPathMatching(EXPECTED_URL))
                .withHeader("authorization", equalTo(BEARER)));
    }

    @Test
    @DisplayName("the SMSF advertises SBI-based MT delivery, so the UDM can select it")
    void registrationAdvertisesSbiSupport() throws Exception {
        udm.stubFor(put(urlPathMatching(".*/registrations/smsf-3gpp-access"))
                .willReturn(aResponse().withStatus(201).withBody("{}")));
        startClient();

        onEvent(registerEvent());

        udm.verify(putRequestedFor(urlPathMatching(EXPECTED_URL))
                .withRequestBody(containing("smsfSbiSupInd")));
    }

    @Test
    @DisplayName("a UDM rejection is handled without breaking the listener")
    void udmRejectionIsHandled() throws Exception {
        udm.stubFor(put(urlPathMatching(".*/registrations/smsf-3gpp-access")).willReturn(aResponse()
                .withStatus(403).withHeader("content-type", "application/problem+json")
                .withBody("{\"detail\":\"subscriber not allowed\"}")));
        startClient();

        onEvent(registerEvent());

        udm.verify(putRequestedFor(urlPathMatching(EXPECTED_URL)));
        assertThat(manager.isStarted()).isTrue();
    }

    @Test
    @DisplayName("a UDM that is down does not propagate the failure out of the listener")
    void udmDownIsSwallowed() throws Exception {
        startClient();
        udm.stop();

        onEvent(registerEvent());

        assertThat(manager.isStarted()).isTrue();
    }

    @Test
    @DisplayName("without a cached token nothing is sent to the UDM, rather than an unauthenticated call")
    void noTokenMeansNoTraffic() throws Exception {
        when(tokenCache.isAvailable(NETWORK_ID, Constants.NF_TYPE_UDM)).thenReturn(false);
        when(tokenCache.authorizationHeader(NETWORK_ID, Constants.NF_TYPE_UDM)).thenReturn(Optional.empty());
        udm.stubFor(put(urlPathMatching(".*")).willReturn(aResponse().withStatus(201).withBody("{}")));
        startClient();

        onEvent(registerEvent());

        udm.verify(0, anyRequestedFor(anyUrl()));
    }

    @Test
    @DisplayName("a malformed payload is discarded before any traffic is generated")
    void malformedPayloadGeneratesNoTraffic() throws Exception {
        udm.stubFor(put(urlPathMatching(".*")).willReturn(aResponse().withStatus(201).withBody("{}")));
        startClient();

        onEvent("{not-json");

        udm.verify(0, anyRequestedFor(anyUrl()));
    }

    @Test
    @DisplayName("forwarding disabled on the gateway means no client and no traffic")
    void forwardingDisabledGeneratesNoTraffic() throws Exception {
        gateway.getUdmSecurity().setForwardRegistrationEnabled(false);
        udm.stubFor(put(urlPathMatching(".*")).willReturn(aResponse().withStatus(201).withBody("{}")));
        setStarted();
        invoke("startN21Client");

        assertThat(manager.getN21Client()).isNull();
        onEvent(registerEvent());

        udm.verify(0, anyRequestedFor(anyUrl()));
    }

    // --- helpers --------------------------------------------------------------

    private void onEvent(String payload) throws Exception {
        Method method = N21StackManager.class.getDeclaredMethod("onRegistrationEvent", ConsumerRecord.class);
        method.setAccessible(true);
        method.invoke(manager, new ConsumerRecord<>(
                NETWORK_ID + ".smsf.udm.register", 0, 0L, SUPI, payload));
    }

    private void invoke(String name) throws Exception {
        Method method = N21StackManager.class.getDeclaredMethod(name);
        method.setAccessible(true);
        method.invoke(manager);
    }

    private void setStarted() throws Exception {
        Field field = N21StackManager.class.getDeclaredField("started");
        field.setAccessible(true);
        ((AtomicBoolean) field.get(manager)).set(true);
    }
}
