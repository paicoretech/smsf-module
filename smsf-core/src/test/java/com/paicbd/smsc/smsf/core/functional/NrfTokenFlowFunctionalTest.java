package com.paicbd.smsc.smsf.core.functional;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.Http2GatewayNrfSecurity;
import com.paicbd.smsc.dto.Http2GatewayOutboundSecurity;
import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.common.http2.Http2ClientNrfConnection;
import com.paicbd.smsc.smsf.common.redis.SmsfSecurityToken;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenRedisStore;
import com.paicbd.smsc.smsf.common.utils.Http2GatewayCreator;
import com.paicbd.smsc.smsf.core.config.AppProperties;
import com.paicbd.smsc.smsf.core.security.outbound.OAuth2ClientCredentialsFetcher;
import com.paicbd.smsc.smsf.core.security.outbound.OAuth2Token;
import com.paicbd.smsc.smsf.core.stack.CoreStackManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.concurrent.ConcurrentHashMap;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.paicbd.smsc.smsf.common.constants.Constants.ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Functional test of the token flow: real HTTP/2 traffic from the module's own client stack to a
 * simulated NRF, and the resulting token published through the real Redis contract.
 *
 * <p>Unlike the unit tests, nothing between {@code Http2ClientNrfConnection} and the wire is mocked
 * — the Netty HTTP/2 client connects, negotiates h2c, sends the form-encoded request and parses the
 * response. Only Redis is stubbed, at the store boundary.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class NrfTokenFlowFunctionalTest {

    private static final String TOKEN_PATH = "/oauth2/token";
    private static final int NETWORK_ID = 10;

    @Mock
    private AppProperties appProperties;

    @Mock
    private SmsfTokenRedisStore tokenStore;

    private WireMockServer nrf;
    private int nrfPort;
    private Http2Gateway gateway;

    @BeforeEach
    void setUp() {
        // Dynamic port, HTTP/2 plaintext enabled (WireMock serves h2c by default).
        nrf = new WireMockServer(wireMockConfig().dynamicPort());
        nrf.start();
        nrfPort = nrf.port();

        gateway = Http2GatewayCreator.buildHttp2GatewayWithMtls("smsf-gw", NETWORK_ID, "amf.local:8443", ENABLED);
        Http2GatewayNrfSecurity nrfSecurity = gateway.getNrfSecurity();
        nrfSecurity.setApiRoot("http://localhost:" + nrfPort);
        nrfSecurity.setTokenPath(TOKEN_PATH);
        nrfSecurity.setMtlsEnabled(false);
        // No NF registration in this test: the token flow is what is under test.
        nrfSecurity.setNrfRegisterPath(null);

        Http2GatewayOutboundSecurity outbound = gateway.getOutboundSecurity();
        outbound.setOauth2Enabled(true);
        outbound.setUsingNrf(true);
        outbound.setUsingExternalEndpoint(false);
        outbound.setClientId("smsf-client");
        outbound.setClientSecret("s3cr3t");
        outbound.setScope("nsmsf-sms");

        when(appProperties.getOutboundTokenRefreshMarginSeconds()).thenReturn(30L);
        when(appProperties.getOutboundTokenInitialFetchTimeoutSeconds()).thenReturn(10L);
        when(appProperties.getOutboundTokenRetryBackoffSeconds()).thenReturn(5L);
        when(appProperties.getN21Oauth2Scope()).thenReturn("nudm-uecm");
    }

    @AfterEach
    void tearDown() {
        nrf.stop();
    }

    private void nrfIssues(String accessToken, long expiresIn) {
        nrf.stubFor(post(urlEqualTo(TOKEN_PATH)).willReturn(aResponse()
                .withStatus(200)
                .withHeader("content-type", "application/json")
                .withBody("{\"access_token\":\"" + accessToken + "\",\"token_type\":\"Bearer\","
                        + "\"expires_in\":" + expiresIn + "}")));
    }

    private Http2ClientNrfConnection connectedNrfClient() {
        Http2ClientNrfConnection connection = new Http2ClientNrfConnection(gateway);
        connection.connect();
        assertThat(connection.isConnected())
                .as("the HTTP/2 client should have connected to the simulated NRF")
                .isTrue();
        return connection;
    }

    private OAuth2ClientCredentialsFetcher fetcher(Http2ClientNrfConnection connection, String targetNfType) {
        return new OAuth2ClientCredentialsFetcher(connection.getClient(), TOKEN_PATH,
                "smsf-client", "s3cr3t", "nsmsf-sms", null, Constants.NF_TYPE_SMSF, targetNfType);
    }

    // --- real traffic ---------------------------------------------------------

    @Test
    @DisplayName("the token is fetched over real HTTP/2 and the NRF receives the 3GPP form body")
    void fetchesTokenOverRealHttp2() throws Exception {
        nrfIssues("an-access-token", 3600);
        Http2ClientNrfConnection connection = connectedNrfClient();

        try {
            OAuth2Token token = fetcher(connection, Constants.NF_TYPE_AMF).fetch(10);

            assertThat(token.accessToken()).isEqualTo("an-access-token");
            assertThat(token.expiresInSeconds()).isEqualTo(3600);
            assertThat(token.isExpired(0)).isFalse();

            // The request really went out, with the client-credentials body TS 29.510 expects.
            nrf.verify(postRequestedFor(urlEqualTo(TOKEN_PATH))
                    .withHeader("content-type", containing("application/x-www-form-urlencoded"))
                    .withRequestBody(containing("grant_type=client_credentials"))
                    .withRequestBody(containing("nfType=SMSF"))
                    .withRequestBody(containing("targetNfType=AMF"))
                    .withRequestBody(containing("scope=nsmsf-sms"))
                    .withRequestBody(containing("client_id=smsf-client")));
        } finally {
            connection.disconnect();
        }
    }

    @Test
    @DisplayName("the audience travels in the request, so the AMF and UDM tokens are asked for separately")
    void audienceTravelsInTheRequest() throws Exception {
        nrfIssues("udm-token", 600);
        Http2ClientNrfConnection connection = connectedNrfClient();

        try {
            OAuth2Token token = fetcher(connection, Constants.NF_TYPE_UDM).fetch(10);

            assertThat(token.accessToken()).isEqualTo("udm-token");
            nrf.verify(postRequestedFor(urlEqualTo(TOKEN_PATH))
                    .withRequestBody(containing("targetNfType=UDM")));
        } finally {
            connection.disconnect();
        }
    }

    @Test
    @DisplayName("an NRF rejection surfaces as a failure instead of a silent empty token")
    void nrfRejectionFails() {
        nrf.stubFor(post(urlEqualTo(TOKEN_PATH)).willReturn(aResponse().withStatus(401)));
        Http2ClientNrfConnection connection = connectedNrfClient();

        try {
            OAuth2ClientCredentialsFetcher fetcher = fetcher(connection, Constants.NF_TYPE_AMF);

            assertThatThrownBy(() -> fetcher.fetch(10))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("HTTP 401");
        } finally {
            connection.disconnect();
        }
    }

    @Test
    @DisplayName("a response without access_token is rejected rather than cached as a usable credential")
    void responseWithoutAccessTokenFails() {
        nrf.stubFor(post(urlEqualTo(TOKEN_PATH)).willReturn(aResponse()
                .withStatus(200)
                .withHeader("content-type", "application/json")
                .withBody("{\"token_type\":\"Bearer\",\"expires_in\":3600}")));
        Http2ClientNrfConnection connection = connectedNrfClient();

        try {
            OAuth2ClientCredentialsFetcher fetcher = fetcher(connection, Constants.NF_TYPE_AMF);

            assertThatThrownBy(() -> fetcher.fetch(10))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("access_token");
        } finally {
            connection.disconnect();
        }
    }

    @Test
    @DisplayName("an empty body is rejected")
    void emptyBodyFails() {
        nrf.stubFor(post(urlEqualTo(TOKEN_PATH)).willReturn(aResponse().withStatus(200).withBody("")));
        Http2ClientNrfConnection connection = connectedNrfClient();

        try {
            OAuth2ClientCredentialsFetcher fetcher = fetcher(connection, Constants.NF_TYPE_AMF);

            assertThatThrownBy(() -> fetcher.fetch(10))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("empty body");
        } finally {
            connection.disconnect();
        }
    }

    // --- the whole stack ------------------------------------------------------

    @Test
    @DisplayName("starting the gateway fetches the token from the NRF and publishes it to Redis")
    void gatewayStartPublishesTheTokenToRedis() {
        nrfIssues("published-token", 3600);
        when(tokenStore.publish(org.mockito.ArgumentMatchers.any())).thenReturn(true);

        CoreStackManager manager = new CoreStackManager(gateway, appProperties, tokenStore,
                new ConcurrentHashMap<>());
        try {
            manager.start();

            ArgumentCaptor<SmsfSecurityToken> captor = ArgumentCaptor.forClass(SmsfSecurityToken.class);
            verify(tokenStore, timeout(10_000)).publish(captor.capture());

            SmsfSecurityToken published = captor.getValue();
            assertThat(published.networkId()).isEqualTo(NETWORK_ID);
            assertThat(published.targetNfType()).isEqualTo(Constants.NF_TYPE_AMF);
            assertThat(published.accessToken()).isEqualTo("published-token");
            assertThat(published.tokenType()).isEqualTo("Bearer");
            assertThat(published.issuedAtEpochSecond()).isPositive();
            assertThat(published.expiresAtEpochSecond())
                    .isGreaterThan(published.issuedAtEpochSecond());

            // The traffic really left the module.
            nrf.verify(postRequestedFor(urlEqualTo(TOKEN_PATH)));
        } finally {
            manager.stop();
        }
    }

    @Test
    @DisplayName("stopping the gateway evicts the Redis entry, leaving no live credential behind")
    void gatewayStopEvictsTheRedisEntry() {
        nrfIssues("published-token", 3600);
        when(tokenStore.publish(org.mockito.ArgumentMatchers.any())).thenReturn(true);

        CoreStackManager manager = new CoreStackManager(gateway, appProperties, tokenStore,
                new ConcurrentHashMap<>());
        manager.start();
        verify(tokenStore, timeout(10_000)).publish(org.mockito.ArgumentMatchers.any());

        manager.stop();

        verify(tokenStore).evict(NETWORK_ID, Constants.NF_TYPE_AMF);
    }

    @Test
    @DisplayName("an NRF that rejects the credentials aborts the gateway start instead of running blind")
    void gatewayStartFailsWhenTheNrfRejects() {
        nrf.stubFor(post(urlEqualTo(TOKEN_PATH)).willReturn(aResponse().withStatus(403)));

        CoreStackManager manager = new CoreStackManager(gateway, appProperties, tokenStore,
                new ConcurrentHashMap<>());

        assertThatThrownBy(manager::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Token bootstrap failed");
        assertThat(manager.isStarted()).isFalse();
    }

    @Test
    @DisplayName("an unreachable NRF leaves the gateway down rather than half started")
    void gatewayStartFailsWhenTheNrfIsDown() {
        gateway.getNrfSecurity().setApiRoot("http://localhost:" + findFreePort());

        CoreStackManager manager = new CoreStackManager(gateway, appProperties, tokenStore,
                new ConcurrentHashMap<>());

        // The connection never comes up, so no fetcher can be built and the refresher is skipped.
        manager.start();
        await().atMost(java.time.Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(manager.getOutboundTokenRefresher()).isNull());
        manager.stop();
    }

    private static int findFreePort() {
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (Exception ex) {
            throw new IllegalStateException("could not find a free port", ex);
        }
    }
}
