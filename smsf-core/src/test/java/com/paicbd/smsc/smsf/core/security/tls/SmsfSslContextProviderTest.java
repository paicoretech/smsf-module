package com.paicbd.smsc.smsf.core.security.tls;

import com.paicbd.smsc.dto.Http2GatewayIncomingSecurity;
import com.paicbd.smsc.dto.Http2GatewayNrfSecurity;
import com.paicbd.smsc.dto.Http2GatewayOutboundSecurity;
import com.paicbd.smsc.smsf.core.config.AppProperties;
import com.paicbd.smsc.smsf.core.security.GatewayLookupChangedEvent;
import com.paicbd.smsc.smsf.core.security.GatewaySecurityBundle;
import com.paicbd.smsc.smsf.core.security.GatewaySecurityLookup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SmsfSslContextProviderTest {

    private static final String CERT_DIR = resolveCertDir();

    private static String resolveCertDir() {
        try {
            return Path.of(Objects.requireNonNull(
                    SmsfSslContextProviderTest.class.getClassLoader().getResource("certs")
            ).toURI()).toString() + "/";
        } catch (URISyntaxException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private AppProperties appProperties;
    private GatewaySecurityLookup gatewayLookup;

    @BeforeEach
    void setUp() {
        appProperties = mock(AppProperties.class);
        gatewayLookup = mock(GatewaySecurityLookup.class);
        when(appProperties.getTlsProtocols()).thenReturn(List.of("TLSv1.3", "TLSv1.2"));
        when(appProperties.getTlsServerKeyPassword()).thenReturn("");
        when(gatewayLookup.snapshot()).thenReturn(List.of());
    }

    @Test
    @DisplayName("no gateway with server cert/key leaves SslContext null (server runs h2c)")
    void noGatewayLeavesContextNull() {
        SmsfSslContextProvider provider = new SmsfSslContextProvider(appProperties, gatewayLookup);
        provider.initialize();

        assertThat(provider.get()).isNull();
    }

    @Test
    @DisplayName("gateway with blank server cert/key is skipped, SslContext stays null")
    void blankCertGatewayLeavesContextNull() {
        when(gatewayLookup.snapshot()).thenReturn(List.of(
                serverBundle(11, "", "")));

        SmsfSslContextProvider provider = new SmsfSslContextProvider(appProperties, gatewayLookup);
        provider.initialize();

        assertThat(provider.get()).isNull();
    }

    @Test
    @DisplayName("the configured session cache is applied, so reconnects can resume instead of re-handshaking")
    void appliesTheConfiguredSessionCache() {
        // This was configured and documented but never wired: under connection churn every reconnect
        // paid a full handshake, private-key operation included.
        when(appProperties.getTlsSessionCacheSize()).thenReturn(8192);
        when(appProperties.getTlsSessionTimeoutSeconds()).thenReturn(600L);
        when(gatewayLookup.snapshot()).thenReturn(List.of(
                serverBundle(11, CERT_DIR + "smsf.cert.pem", CERT_DIR + "smsf.key.pem")));

        SmsfSslContextProvider provider = new SmsfSslContextProvider(appProperties, gatewayLookup);
        provider.initialize();

        assertThat(provider.get().sessionContext().getSessionCacheSize()).isEqualTo(8192);
        assertThat(provider.get().sessionContext().getSessionTimeout()).isEqualTo(600);
    }

    @Test
    @DisplayName("builds Netty SslContext from first gateway with server cert/key configured")
    void buildsContextFromGatewayCert() {
        when(gatewayLookup.snapshot()).thenReturn(List.of(
                serverBundle(11, CERT_DIR + "smsf.cert.pem", CERT_DIR + "smsf.key.pem")));

        SmsfSslContextProvider provider = new SmsfSslContextProvider(appProperties, gatewayLookup);
        provider.initialize();

        assertThat(provider.get()).isNotNull();
        assertThat(provider.get().isServer()).isTrue();
    }

    @Test
    @DisplayName("aggregates per-gateway CA paths into the truststore")
    void aggregatesGatewayCas() {
        when(gatewayLookup.snapshot()).thenReturn(List.of(
                fullBundle(11, CERT_DIR + "smsf.cert.pem", CERT_DIR + "smsf.key.pem",
                        true, CERT_DIR + "ca.cert.pem", ""),
                fullBundle(12, "", "",
                        true, CERT_DIR + "rogue-ca.cert.pem", CERT_DIR + "rogue.cert.pem"),
                fullBundle(13, "", "",
                        false, CERT_DIR + "ca.cert.pem", "")));

        SmsfSslContextProvider provider = new SmsfSslContextProvider(appProperties, gatewayLookup);
        provider.initialize();

        assertThat(provider.get()).isNotNull();
    }

    @Test
    @DisplayName("bundle with null incoming is skipped when looking for the server cert")
    void nullIncomingIsSkipped() {
        when(gatewayLookup.snapshot()).thenReturn(List.of(
                new GatewaySecurityBundle(11, null,
                        new Http2GatewayNrfSecurity(), new Http2GatewayOutboundSecurity()),
                serverBundle(12, CERT_DIR + "smsf.cert.pem", CERT_DIR + "smsf.key.pem")));

        SmsfSslContextProvider provider = new SmsfSslContextProvider(appProperties, gatewayLookup);
        provider.initialize();

        assertThat(provider.get()).isNotNull();
    }

    @Test
    @DisplayName("bundle with cert path set but blank key path is skipped")
    void blankKeyButCertSkippedDuringDiscovery() {
        when(gatewayLookup.snapshot()).thenReturn(List.of(
                serverBundle(11, CERT_DIR + "smsf.cert.pem", "")));

        SmsfSslContextProvider provider = new SmsfSslContextProvider(appProperties, gatewayLookup);
        provider.initialize();

        assertThat(provider.get()).isNull();
    }

    @Test
    @DisplayName("non-blank server key password is forwarded to the builder")
    void usesServerKeyPasswordWhenConfigured() {
        when(appProperties.getTlsServerKeyPassword()).thenReturn("super-secret");
        when(gatewayLookup.snapshot()).thenReturn(List.of(
                serverBundle(11, CERT_DIR + "smsf.cert.pem", CERT_DIR + "smsf.key.pem")));

        SmsfSslContextProvider provider = new SmsfSslContextProvider(appProperties, gatewayLookup);
        assertThatThrownBy(provider::initialize).isInstanceOf(RuntimeException.class);
    }

    @Test
    @DisplayName("require_mtls=true with null/blank CA and peer paths produces no trust entries")
    void requireMtlsWithoutTrustMaterialFallsBackToNoneClientAuth() {
        when(gatewayLookup.snapshot()).thenReturn(List.of(
                fullBundle(11, CERT_DIR + "smsf.cert.pem", CERT_DIR + "smsf.key.pem",
                        true, null, null)));

        SmsfSslContextProvider provider = new SmsfSslContextProvider(appProperties, gatewayLookup);
        provider.initialize();

        assertThat(provider.get()).isNotNull();
    }

    @Test
    @DisplayName("require_mtls=false ignores CA paths even when configured")
    void requireMtlsFalseSkipsCaPaths() {
        when(gatewayLookup.snapshot()).thenReturn(List.of(
                fullBundle(11, CERT_DIR + "smsf.cert.pem", CERT_DIR + "smsf.key.pem",
                        false, CERT_DIR + "ca.cert.pem", CERT_DIR + "amf.cert.pem")));

        SmsfSslContextProvider provider = new SmsfSslContextProvider(appProperties, gatewayLookup);
        provider.initialize();

        assertThat(provider.get()).isNotNull();
    }

    @Test
    @DisplayName("onGatewayLookupChanged REFRESHED rebuilds the SslContext")
    void rebuildOnRefreshedEvent() {
        SmsfSslContextProvider provider = new SmsfSslContextProvider(appProperties, gatewayLookup);
        provider.initialize();
        assertThat(provider.get()).isNull();

        when(gatewayLookup.snapshot()).thenReturn(List.of(
                serverBundle(11, CERT_DIR + "smsf.cert.pem", CERT_DIR + "smsf.key.pem")));

        provider.onGatewayLookupChanged(new GatewayLookupChangedEvent(11,
                GatewayLookupChangedEvent.ChangeType.REFRESHED));

        assertThat(provider.get()).isNotNull();
    }

    @Test
    @DisplayName("onGatewayLookupChanged EVICTED drops the SslContext when no gateway remains")
    void evictDropsSslContext() {
        when(gatewayLookup.snapshot()).thenReturn(List.of(
                serverBundle(11, CERT_DIR + "smsf.cert.pem", CERT_DIR + "smsf.key.pem")));
        SmsfSslContextProvider provider = new SmsfSslContextProvider(appProperties, gatewayLookup);
        provider.initialize();
        assertThat(provider.get()).isNotNull();

        when(gatewayLookup.snapshot()).thenReturn(List.of());

        provider.onGatewayLookupChanged(new GatewayLookupChangedEvent(11,
                GatewayLookupChangedEvent.ChangeType.EVICTED));

        assertThat(provider.get()).isNull();
    }

    private GatewaySecurityBundle serverBundle(int networkId, String serverCert, String serverKey) {
        return fullBundle(networkId, serverCert, serverKey, false, "", "");
    }

    private GatewaySecurityBundle fullBundle(int networkId, String serverCert, String serverKey,
                                             boolean requireMtls, String caPath, String expectedAmfPath) {
        Http2GatewayIncomingSecurity inc = new Http2GatewayIncomingSecurity();
        inc.setServerCertificatePath(serverCert);
        inc.setServerPrivateKeyPath(serverKey);
        inc.setRequireMtls(requireMtls);
        inc.setCaCertificatePath(caPath);
        inc.setExpectedAmfCertificatePath(expectedAmfPath);
        return new GatewaySecurityBundle(networkId, inc,
                new Http2GatewayNrfSecurity(), new Http2GatewayOutboundSecurity());
    }
}
