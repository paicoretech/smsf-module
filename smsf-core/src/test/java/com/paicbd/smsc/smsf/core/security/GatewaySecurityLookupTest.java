package com.paicbd.smsc.smsf.core.security;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.Http2GatewayIncomingSecurity;
import com.paicbd.smsc.dto.Http2GatewayNrfSecurity;
import com.paicbd.smsc.dto.Http2GatewayOutboundSecurity;
import com.paicbd.smsc.smsf.core.config.AppProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class GatewaySecurityLookupTest {

    @Mock
    private AppProperties appProperties;

    @Mock
    private ApplicationEventPublisher eventPublisher;

    private GatewaySecurityLookup lookup;

    @BeforeEach
    void setUp() {
        lenient().when(appProperties.getSecurityAllowedJwsAlgorithms()).thenReturn(List.of("RS256", "ES256"));
        lenient().when(appProperties.getSecurityExpectedScope()).thenReturn("nsmsf-sms");
        lenient().when(appProperties.getSecurityClockSkewSeconds()).thenReturn(30L);
        lookup = new GatewaySecurityLookup(appProperties, eventPublisher);
    }

    @Test
    @DisplayName("findByNetworkId returns empty when no gateway is registered yet")
    void findByNetworkIdEmptyInitially() {
        assertTrue(lookup.findByNetworkId(1).isEmpty());
        assertTrue(lookup.authenticatorForNetworkId(1).isEmpty());
    }

    @Test
    @DisplayName("register indexes an enabled gateway and builds the authenticator")
    void registerIndexesEnabledGateway() {
        Http2Gateway enabled = gateway(11, 1, "https://nrf.tenant-a/oauth2", "https://nrf.tenant-a/jwks");

        lookup.register(enabled);

        assertTrue(lookup.findByNetworkId(11).isPresent());
        assertTrue(lookup.authenticatorForNetworkId(11).isPresent());
    }

    @Test
    @DisplayName("register skips a disabled gateway from being indexed")
    void registerSkipsDisabledGateway() {
        Http2Gateway disabled = gateway(12, 0, "https://nrf.tenant-b/oauth2", "https://nrf.tenant-b/jwks");

        lookup.register(disabled);

        assertTrue(lookup.findByNetworkId(12).isEmpty());
        assertTrue(lookup.authenticatorForNetworkId(12).isEmpty());
    }

    @Test
    @DisplayName("register replaces the bundle and authenticator when the issuer changes")
    void registerReplacesWhenIssuerChanges() {
        Http2Gateway original = gateway(30, 1, "https://nrf.old/oauth2", "https://nrf.old/jwks");
        lookup.register(original);

        Http2Gateway updated = gateway(30, 1, "https://nrf.new/oauth2", "https://nrf.new/jwks");
        lookup.register(updated);

        assertEquals("https://nrf.new/oauth2",
                lookup.findByNetworkId(30).orElseThrow().incoming().getExpectedIssuer());
        assertTrue(lookup.authenticatorForNetworkId(30).isPresent());
    }

    @Test
    @DisplayName("register that transitions to disabled removes the existing gateway")
    void registerTransitionToDisabledRemovesGateway() {
        Http2Gateway enabled = gateway(70, 1, "https://nrf/oauth2", "https://nrf/jwks");
        lookup.register(enabled);

        Http2Gateway disabled = gateway(70, 0, "https://nrf/oauth2", "https://nrf/jwks");
        lookup.register(disabled);

        assertTrue(lookup.findByNetworkId(70).isEmpty());
        assertTrue(lookup.authenticatorForNetworkId(70).isEmpty());
    }

    @Test
    @DisplayName("evict removes the gateway from both maps and returns the bundle")
    void evictRemovesGateway() {
        Http2Gateway present = gateway(60, 1, "https://nrf/oauth2", "https://nrf/jwks");
        lookup.register(present);

        Optional<GatewaySecurityBundle> evicted = lookup.evict(60);

        assertTrue(evicted.isPresent());
        assertEquals(60, evicted.get().networkId());
        assertTrue(lookup.findByNetworkId(60).isEmpty());
        assertTrue(lookup.authenticatorForNetworkId(60).isEmpty());
    }

    @Test
    @DisplayName("evict returns empty when the gateway is not indexed")
    void evictReturnsEmptyWhenAbsent() {
        assertTrue(lookup.evict(404).isEmpty());
    }

    @Test
    @DisplayName("snapshot returns a copy of the currently indexed gateways")
    void snapshotReturnsRegisteredGateways() {
        Http2Gateway gw = gateway(80, 1, "https://nrf/oauth2", "https://nrf/jwks");
        lookup.register(gw);

        assertEquals(1, lookup.snapshot().size());
        assertEquals(80, lookup.snapshot().iterator().next().networkId());
    }

    @Test
    @DisplayName("snapshot is empty when no gateways are registered")
    void snapshotEmpty() {
        assertTrue(lookup.snapshot().isEmpty());
    }

    @Test
    @DisplayName("inbound oauth2 enabled but outbound oauth2 disabled does not register an authenticator")
    void noAuthenticatorWhenOutboundOauth2Disabled() {
        Http2GatewayIncomingSecurity incoming = new Http2GatewayIncomingSecurity();
        incoming.setOauth2Enabled(true);
        incoming.setExpectedIssuer("https://nrf/oauth2");
        Http2GatewayOutboundSecurity outbound = new Http2GatewayOutboundSecurity();
        outbound.setOauth2Enabled(false);
        Http2Gateway gateway = Http2Gateway.builder()
                .networkId(101).name("gw-101").host("10.0.0.101").mnoId(1)
                .enabled(1).protocol("HTTP2")
                .incomingSecurity(incoming)
                .nrfSecurity(new Http2GatewayNrfSecurity())
                .outboundSecurity(outbound)
                .build();

        lookup.register(gateway);

        assertTrue(lookup.findByNetworkId(101).isPresent());
        assertTrue(lookup.authenticatorForNetworkId(101).isEmpty());
    }

    @Test
    @DisplayName("inbound oauth2 disabled registers the gateway without an authenticator")
    void noAuthenticatorWhenInboundOauth2Disabled() {
        Http2GatewayIncomingSecurity incoming = new Http2GatewayIncomingSecurity();
        incoming.setOauth2Enabled(false);
        Http2Gateway gateway = Http2Gateway.builder()
                .networkId(102).name("gw-102").host("10.0.0.102").mnoId(1)
                .enabled(1).protocol("HTTP2")
                .incomingSecurity(incoming)
                .nrfSecurity(new Http2GatewayNrfSecurity())
                .outboundSecurity(new Http2GatewayOutboundSecurity())
                .build();

        lookup.register(gateway);

        assertTrue(lookup.findByNetworkId(102).isPresent());
        assertTrue(lookup.authenticatorForNetworkId(102).isEmpty());
    }

    @Test
    @DisplayName("outbound oauth2 enabled but missing nrf api_root/secret_path yields no authenticator")
    void noAuthenticatorWhenNrfTokenEndpointMissing() {
        Http2GatewayIncomingSecurity incoming = new Http2GatewayIncomingSecurity();
        incoming.setOauth2Enabled(true);
        Http2GatewayOutboundSecurity outbound = new Http2GatewayOutboundSecurity();
        outbound.setOauth2Enabled(true);
        outbound.setUsingNrf(true);
        Http2Gateway gateway = Http2Gateway.builder()
                .networkId(103).name("gw-103").host("10.0.0.103").mnoId(1)
                .enabled(1).protocol("HTTP2")
                .incomingSecurity(incoming)
                .nrfSecurity(new Http2GatewayNrfSecurity())
                .outboundSecurity(outbound)
                .build();

        lookup.register(gateway);

        assertTrue(lookup.findByNetworkId(103).isPresent());
        assertTrue(lookup.authenticatorForNetworkId(103).isEmpty());
    }

    @Test
    @DisplayName("authenticator is derived from NRF api_root + secret_path regardless of outbound flags")
    void authenticatorAlwaysUsesNrfTokenEndpoint() {
        Http2GatewayIncomingSecurity incoming = new Http2GatewayIncomingSecurity();
        incoming.setOauth2Enabled(true);
        incoming.setExpectedIssuer("https://nrf.tenant-c/oauth2");
        Http2GatewayNrfSecurity nrf = new Http2GatewayNrfSecurity();
        nrf.setApiRoot("https://nrf.tenant-c");
        nrf.setSecretPath("/oauth2/jwks");
        Http2GatewayOutboundSecurity outbound = new Http2GatewayOutboundSecurity();
        outbound.setOauth2Enabled(false);
        outbound.setUsingExternalEndpoint(true);
        outbound.setTokenEndpoint("https://ext/oauth2/token");
        Http2Gateway gateway = Http2Gateway.builder()
                .networkId(104).name("gw-104").host("10.0.0.104").mnoId(1)
                .enabled(1).protocol("HTTP2")
                .incomingSecurity(incoming)
                .nrfSecurity(nrf)
                .outboundSecurity(outbound)
                .build();

        lookup.register(gateway);

        assertTrue(lookup.findByNetworkId(104).isPresent());
        assertTrue(lookup.authenticatorForNetworkId(104).isPresent());
    }

    @Test
    @DisplayName("invalid NRF api_root URI yields no authenticator")
    void noAuthenticatorWhenTokenEndpointMalformed() {
        Http2GatewayIncomingSecurity incoming = new Http2GatewayIncomingSecurity();
        incoming.setOauth2Enabled(true);
        Http2GatewayNrfSecurity nrf = new Http2GatewayNrfSecurity();
        nrf.setApiRoot("ht tp://not a uri:::");
        nrf.setSecretPath("/oauth2/jwks");
        Http2Gateway gateway = Http2Gateway.builder()
                .networkId(105).name("gw-105").host("10.0.0.105").mnoId(1)
                .enabled(1).protocol("HTTP2")
                .incomingSecurity(incoming)
                .nrfSecurity(nrf)
                .outboundSecurity(new Http2GatewayOutboundSecurity())
                .build();

        lookup.register(gateway);

        assertTrue(lookup.findByNetworkId(105).isPresent());
        assertTrue(lookup.authenticatorForNetworkId(105).isEmpty());
    }

    @Test
    @DisplayName("parseAlgorithms falls back to defaults when the property is empty")
    void emptyAlgorithmsListFallsBackToDefaults() {
        lenient().when(appProperties.getSecurityAllowedJwsAlgorithms()).thenReturn(List.of());
        Http2Gateway gw = gateway(110, 1, "https://nrf/oauth2", "https://nrf/jwks");

        lookup.register(gw);

        assertTrue(lookup.authenticatorForNetworkId(110).isPresent());
    }

    @Test
    @DisplayName("parseAlgorithms skips null/blank entries and adds the rest")
    void algorithmsListSkipsNullAndBlank() {
        java.util.List<String> raw = new java.util.ArrayList<>();
        raw.add(null);
        raw.add("");
        raw.add("RS256");
        lenient().when(appProperties.getSecurityAllowedJwsAlgorithms()).thenReturn(raw);
        Http2Gateway gw = gateway(111, 1, "https://nrf/oauth2", "https://nrf/jwks");

        lookup.register(gw);

        assertTrue(lookup.authenticatorForNetworkId(111).isPresent());
    }

    @Test
    @DisplayName("parseAlgorithms falls back to defaults when every value is blank")
    void algorithmsAllBlankFallsBackToDefaults() {
        lenient().when(appProperties.getSecurityAllowedJwsAlgorithms())
                .thenReturn(List.of("   ", ""));
        Http2Gateway gw = gateway(112, 1, "https://nrf/oauth2", "https://nrf/jwks");

        lookup.register(gw);

        assertTrue(lookup.authenticatorForNetworkId(112).isPresent());
    }

    @Test
    @DisplayName("parseScopes uses an empty set when the property is blank")
    void blankScopeProducesEmptySet() {
        lenient().when(appProperties.getSecurityExpectedScope()).thenReturn("   ");
        Http2Gateway gw = gateway(113, 1, "https://nrf/oauth2", "https://nrf/jwks");

        lookup.register(gw);

        assertTrue(lookup.authenticatorForNetworkId(113).isPresent());
    }

    @Test
    @DisplayName("parseScopes splits whitespace-separated values into a set")
    void multipleScopesAreParsed() {
        lenient().when(appProperties.getSecurityExpectedScope()).thenReturn("nsmsf-sms   nsmsf-admin");
        Http2Gateway gw = gateway(114, 1, "https://nrf/oauth2", "https://nrf/jwks");

        lookup.register(gw);

        assertTrue(lookup.authenticatorForNetworkId(114).isPresent());
    }

    @Test
    @DisplayName("evict on present gateway invalidates the JWKS cache when a URI was derived")
    void evictInvalidatesJwksCache() {
        Http2Gateway gw = gateway(116, 1, "https://nrf/oauth2", "https://nrf/jwks");
        lookup.register(gw);

        Optional<GatewaySecurityBundle> evicted = lookup.evict(116);

        assertTrue(evicted.isPresent());
        assertTrue(lookup.findByNetworkId(116).isEmpty());
    }

    @Test
    @DisplayName("authenticator is built when api_root carries a trailing slash and secret_path lacks a leading slash")
    void apiRootAndSecretPathSlashNormalization() {
        Http2GatewayIncomingSecurity incoming = new Http2GatewayIncomingSecurity();
        incoming.setOauth2Enabled(true);
        Http2GatewayNrfSecurity nrf = new Http2GatewayNrfSecurity();
        nrf.setApiRoot("https://nrf.tenant-d/");
        nrf.setSecretPath("oauth2/jwks");
        Http2Gateway gateway = Http2Gateway.builder()
                .networkId(117).name("gw-117").host("10.0.0.117").mnoId(1)
                .enabled(1).protocol("HTTP2")
                .incomingSecurity(incoming)
                .nrfSecurity(nrf)
                .outboundSecurity(new Http2GatewayOutboundSecurity())
                .build();

        lookup.register(gateway);

        assertTrue(lookup.authenticatorForNetworkId(117).isPresent());
    }

    @Test
    @DisplayName("authenticator built using mTLS NRF settings populates the JWKS client with TLS material")
    void authenticatorWhenNrfMtlsEnabled() {
        Http2GatewayIncomingSecurity incoming = new Http2GatewayIncomingSecurity();
        incoming.setOauth2Enabled(true);
        Http2GatewayNrfSecurity nrf = new Http2GatewayNrfSecurity();
        nrf.setApiRoot("https://nrf.tenant-e:8443");
        nrf.setSecretPath("/oauth2/jwks");
        nrf.setMtlsEnabled(true);
        nrf.setClientCertificatePath("/path/cert.pem");
        nrf.setClientPrivateKeyPath("/path/key.pem");
        nrf.setTruststoreCaPath("/path/ca.pem");
        Http2Gateway gateway = Http2Gateway.builder()
                .networkId(118).name("gw-118").host("10.0.0.118").mnoId(1)
                .enabled(1).protocol("HTTP2")
                .incomingSecurity(incoming)
                .nrfSecurity(nrf)
                .outboundSecurity(new Http2GatewayOutboundSecurity())
                .build();

        lookup.register(gateway);

        assertTrue(lookup.authenticatorForNetworkId(118).isPresent());
    }

    @Test
    @DisplayName("authenticator using http api_root falls back to plain port 80 when none is given")
    void authenticatorWhenHttpAndNoPortFallsBackTo80() {
        Http2GatewayIncomingSecurity incoming = new Http2GatewayIncomingSecurity();
        incoming.setOauth2Enabled(true);
        Http2GatewayNrfSecurity nrf = new Http2GatewayNrfSecurity();
        nrf.setApiRoot("http://nrf.tenant-f");
        nrf.setSecretPath("/oauth2/jwks");
        Http2Gateway gateway = Http2Gateway.builder()
                .networkId(119).name("gw-119").host("10.0.0.119").mnoId(1)
                .enabled(1).protocol("HTTP2")
                .incomingSecurity(incoming)
                .nrfSecurity(nrf)
                .outboundSecurity(new Http2GatewayOutboundSecurity())
                .build();

        lookup.register(gateway);

        assertTrue(lookup.authenticatorForNetworkId(119).isPresent());
    }

    @Test
    @DisplayName("missing api_root with present secret_path yields no authenticator")
    void noAuthenticatorWhenApiRootBlank() {
        Http2GatewayIncomingSecurity incoming = new Http2GatewayIncomingSecurity();
        incoming.setOauth2Enabled(true);
        Http2GatewayNrfSecurity nrf = new Http2GatewayNrfSecurity();
        nrf.setApiRoot(" ");
        nrf.setSecretPath("/oauth2/jwks");
        Http2Gateway gateway = Http2Gateway.builder()
                .networkId(120).name("gw-120").host("10.0.0.120").mnoId(1)
                .enabled(1).protocol("HTTP2")
                .incomingSecurity(incoming)
                .nrfSecurity(nrf)
                .outboundSecurity(new Http2GatewayOutboundSecurity())
                .build();

        lookup.register(gateway);

        assertTrue(lookup.findByNetworkId(120).isPresent());
        assertTrue(lookup.authenticatorForNetworkId(120).isEmpty());
    }

    @Test
    @DisplayName("present api_root with blank secret_path yields no authenticator")
    void noAuthenticatorWhenSecretPathBlank() {
        Http2GatewayIncomingSecurity incoming = new Http2GatewayIncomingSecurity();
        incoming.setOauth2Enabled(true);
        Http2GatewayNrfSecurity nrf = new Http2GatewayNrfSecurity();
        nrf.setApiRoot("https://nrf.tenant-g");
        nrf.setSecretPath("");
        Http2Gateway gateway = Http2Gateway.builder()
                .networkId(121).name("gw-121").host("10.0.0.121").mnoId(1)
                .enabled(1).protocol("HTTP2")
                .incomingSecurity(incoming)
                .nrfSecurity(nrf)
                .outboundSecurity(new Http2GatewayOutboundSecurity())
                .build();

        lookup.register(gateway);

        assertTrue(lookup.findByNetworkId(121).isPresent());
        assertTrue(lookup.authenticatorForNetworkId(121).isEmpty());
    }

    @Test
    @DisplayName("api_root host is missing then no authenticator is built")
    void noAuthenticatorWhenApiRootHasNoHost() {
        Http2GatewayIncomingSecurity incoming = new Http2GatewayIncomingSecurity();
        incoming.setOauth2Enabled(true);
        Http2GatewayNrfSecurity nrf = new Http2GatewayNrfSecurity();
        nrf.setApiRoot("https:///");
        nrf.setSecretPath("/oauth2/jwks");
        Http2Gateway gateway = Http2Gateway.builder()
                .networkId(122).name("gw-122").host("10.0.0.122").mnoId(1)
                .enabled(1).protocol("HTTP2")
                .incomingSecurity(incoming)
                .nrfSecurity(nrf)
                .outboundSecurity(new Http2GatewayOutboundSecurity())
                .build();

        lookup.register(gateway);

        assertTrue(lookup.findByNetworkId(122).isPresent());
        assertTrue(lookup.authenticatorForNetworkId(122).isEmpty());
    }

    @Test
    @DisplayName("registering a disabled gateway after a previous registration evicts the previous bundle")
    void registerDisabledGatewayAfterEnabledEvictsPrevious() {
        Http2Gateway enabled = gateway(123, 1, "https://nrf/oauth2", "https://nrf/jwks");
        lookup.register(enabled);
        assertTrue(lookup.findByNetworkId(123).isPresent());

        Http2Gateway disabled = gateway(123, 0, "https://nrf/oauth2", "https://nrf/jwks");
        lookup.register(disabled);

        assertTrue(lookup.findByNetworkId(123).isEmpty());
        assertTrue(lookup.authenticatorForNetworkId(123).isEmpty());
    }

    @Test
    @DisplayName("resolveJwksUri returns null when NRF security is absent")
    void resolveJwksUriReturnsNullWhenNrfSecurityAbsent() throws Exception {
        GatewaySecurityBundle bundle = new GatewaySecurityBundle(
                124, new Http2GatewayIncomingSecurity(), null, new Http2GatewayOutboundSecurity());

        URI jwksUri = invokeResolveJwksUri(bundle);

        assertNull(jwksUri);
    }

    @Test
    @DisplayName("buildJwksClientSettings returns null when NRF security is absent")
    void buildJwksClientSettingsReturnsNullWhenNrfSecurityAbsent() throws Exception {
        GatewaySecurityBundle bundle = new GatewaySecurityBundle(
                125, new Http2GatewayIncomingSecurity(), null, new Http2GatewayOutboundSecurity());

        Object settings = invokeBuildJwksClientSettings(bundle, URI.create("https://nrf.tenant-h/oauth2/jwks"));

        assertNull(settings);
    }

    @Test
    @DisplayName("buildJwksClientSettings uses slash path when the JWKS URI raw path is empty")
    void buildJwksClientSettingsUsesSlashWhenRawPathIsEmpty() throws Exception {
        Http2GatewayNrfSecurity nrf = new Http2GatewayNrfSecurity();
        nrf.setApiRoot("https://nrf.tenant-i");
        nrf.setSecretPath("/oauth2/jwks");
        GatewaySecurityBundle bundle = new GatewaySecurityBundle(
                126, new Http2GatewayIncomingSecurity(), nrf, new Http2GatewayOutboundSecurity());

        com.paicore.sba.stack.transport.http2.config.Http2ClientSettings settings =
                invokeBuildJwksClientSettings(bundle, URI.create("https://nrf.tenant-i"));

        assertEquals("/", settings.getPath());
        assertEquals(443, settings.getPort());
    }

    private URI invokeResolveJwksUri(GatewaySecurityBundle bundle) throws Exception {
        Method method = GatewaySecurityLookup.class.getDeclaredMethod("resolveJwksUri", GatewaySecurityBundle.class);
        method.setAccessible(true);
        return (URI) method.invoke(lookup, bundle);
    }

    private com.paicore.sba.stack.transport.http2.config.Http2ClientSettings invokeBuildJwksClientSettings(
            GatewaySecurityBundle bundle, URI jwksUri) throws Exception {
        Method method = GatewaySecurityLookup.class.getDeclaredMethod(
                "buildJwksClientSettings", GatewaySecurityBundle.class, URI.class);
        method.setAccessible(true);
        return (com.paicore.sba.stack.transport.http2.config.Http2ClientSettings) method.invoke(lookup, bundle, jwksUri);
    }

    private static Http2Gateway gateway(int networkId, int enabled, String issuer, String tokenEndpoint) {
        Http2GatewayIncomingSecurity incoming = new Http2GatewayIncomingSecurity();
        incoming.setOauth2Enabled(true);
        incoming.setExpectedIssuer(issuer);
        return Http2Gateway.builder()
                .networkId(networkId)
                .name("gw-" + networkId)
                .host("10.0.0." + networkId)
                .mnoId(1)
                .enabled(enabled)
                .protocol("HTTP2")
                .incomingSecurity(incoming)
                .nrfSecurity(nrfSecurity(tokenEndpoint))
                .outboundSecurity(outboundSecurityUsingNrf())
                .multipartParsingEnabled(true)
                .binarySmsEnabled(true)
                .maxMessageSize(1048576)
                .defaultDataCodingId(0)
                .requestTimeoutMs(5000)
                .connectionTimeoutMs(3000)
                .idleTimeoutMs(10000)
                .maxResponseTimeMs(15000)
                .threadPoolSize(8)
                .messagesPerSecond(100)
                .rateLimitingEnabled(true)
                .requestDlr(2)
                .build();
    }

    private static Http2GatewayNrfSecurity nrfSecurity(String jwksUrl) {
        Http2GatewayNrfSecurity nrf = new Http2GatewayNrfSecurity();
        int schemeEnd = jwksUrl.indexOf("://");
        int pathStart = schemeEnd >= 0 ? jwksUrl.indexOf('/', schemeEnd + 3) : jwksUrl.indexOf('/');
        if (pathStart >= 0) {
            nrf.setApiRoot(jwksUrl.substring(0, pathStart));
            nrf.setSecretPath(jwksUrl.substring(pathStart));
        } else {
            nrf.setApiRoot(jwksUrl);
            nrf.setSecretPath("/oauth2/jwks");
        }
        return nrf;
    }

    private static Http2GatewayOutboundSecurity outboundSecurityUsingNrf() {
        Http2GatewayOutboundSecurity outbound = new Http2GatewayOutboundSecurity();
        outbound.setOauth2Enabled(true);
        outbound.setUsingNrf(true);
        return outbound;
    }
}
