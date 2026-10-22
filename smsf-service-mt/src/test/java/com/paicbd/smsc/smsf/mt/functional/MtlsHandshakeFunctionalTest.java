package com.paicbd.smsc.smsf.mt.functional;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.smsf.common.e2e.SmsfE2EPorts;
import com.paicbd.smsc.smsf.common.utils.Http2GatewayCreator;
import com.paicbd.smsc.smsf.mt.http2.Http2ClientConnection;
import com.paicore.sba.stack.testing.mock.Http2MockServer;
import com.paicore.sba.stack.transport.http2.dto.Http2Request;
import com.paicore.sba.stack.transport.http2.dto.Http2Response;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.paicbd.smsc.smsf.common.constants.Constants.ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A real TLS handshake, not a settings assertion.
 *
 * <p>Everything else about mTLS in this repository is checked by reading configuration back out of
 * an {@code SslContext}. That proves the context was built, not that two peers can actually agree
 * on one. Here the client is the production {@link Http2ClientConnection} and the server is the
 * library's mock with a context that demands a client certificate, so the assertions are decided by
 * the handshake itself.</p>
 *
 * <p>The PKI in {@code src/test/resources/certs} is a real one: {@code ca} signs {@code smsf} (the
 * server) and {@code amf} (the client), while {@code rogue} is signed by a different CA and is
 * therefore the negative case. Both leaf certificates carry {@code localhost} and {@code 127.0.0.1}
 * in their SAN, so hostname verification is exercised rather than bypassed.</p>
 */
class MtlsHandshakeFunctionalTest {

    private static final int NETWORK_ID = 10;
    private static final String CERTS = certsDirectory();

    private Http2MockServer amf;
    private int amfPort;
    private Http2ClientConnection connection;

    @AfterEach
    void tearDown() {
        if (Objects.nonNull(connection)) {
            connection.disconnect();
        }
        if (Objects.nonNull(amf)) {
            amf.close();
        }
        SmsfE2EPorts.release(amfPort);
    }

    @Test
    @DisplayName("a client certificate signed by the trusted CA completes the handshake and carries a request")
    void trustedClientCertificateIsAccepted() {
        startAmfRequiringClientCertificate();
        connection = clientPresenting("amf.cert.pem", "amf.key.pem");

        connection.connect();
        assertThat(connection.isConnected()).isTrue();

        // The session has to survive and carry data. Under TLS 1.3 the client finishes before the
        // server has judged its certificate, so a connected socket alone proves nothing.
        Http2Response response = connection.send(probe()).orTimeout(10, TimeUnit.SECONDS).join();

        assertThat(response).isNotNull();
        assertThat(amf.requestCount())
                .as("the AMF should have received the request over the mutual TLS session")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("a client certificate from another CA never gets a request through")
    void untrustedClientCertificateIsRejected() {
        startAmfRequiringClientCertificate();
        connection = clientPresenting("rogue.cert.pem", "rogue.key.pem");

        connection.connect();

        // The rejection arrives as an alert after the client's own handshake completes, so what is
        // asserted is the outcome that matters: the session dies and no request ever lands.
        await().atMost(Duration.ofSeconds(10)).until(() -> !connection.isConnected());
        assertThat(amf.requestCount())
                .as("a certificate signed by the rogue CA must not get anything through to the AMF")
                .isZero();
    }

    /** Any request will do — the point is whether the TLS session carries it, not what it says. */
    private static Http2Request probe() {
        return Http2Request.builder()
                .method("GET")
                .path("/nsmsf-sms/v2/ue-contexts/imsi-460000000000010/sendsms")
                .build();
    }

    /** The AMF end: serves h2 over TLS and will not talk to a client it cannot verify. */
    private void startAmfRequiringClientCertificate() {
        amfPort = SmsfE2EPorts.allocate();
        amf = Http2MockServer.startTls(serverContext(), amfPort);
    }

    private static SslContext serverContext() {
        try {
            return SslContextBuilder
                    .forServer(new File(CERTS, "smsf.cert.pem"), new File(CERTS, "smsf.key.pem"))
                    .trustManager(new File(CERTS, "ca.cert.pem"))
                    .clientAuth(ClientAuth.REQUIRE)
                    .applicationProtocolConfig(new ApplicationProtocolConfig(
                            ApplicationProtocolConfig.Protocol.ALPN,
                            ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                            ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                            ApplicationProtocolNames.HTTP_2))
                    .build();
        } catch (Exception ex) {
            throw new IllegalStateException("Could not build the AMF server context", ex);
        }
    }

    /** The SMSF end: the production client, configured with the given identity. */
    private Http2ClientConnection clientPresenting(String certificate, String privateKey) {
        Http2Gateway gateway = Http2GatewayCreator.buildHttp2Gateway("smsf-gw", NETWORK_ID,
                "https://localhost:" + amfPort + "/nsmsf-sms/v2/ue-contexts/{supi}/sendsms", ENABLED);
        gateway.getOutboundSecurity().setMtlsEnabled(true);
        gateway.getOutboundSecurity().setOauth2Enabled(false);
        gateway.getOutboundSecurity().setClientCertificatePath(new File(CERTS, certificate).getAbsolutePath());
        gateway.getOutboundSecurity().setClientPrivateKeyPath(new File(CERTS, privateKey).getAbsolutePath());
        gateway.getOutboundSecurity().setTruststoreCaPath(new File(CERTS, "ca.cert.pem").getAbsolutePath());
        return new Http2ClientConnection(gateway);
    }

    private static String certsDirectory() {
        return new File(Objects.requireNonNull(
                MtlsHandshakeFunctionalTest.class.getClassLoader().getResource("certs"),
                "the test PKI is missing from src/test/resources/certs").getFile()).getAbsolutePath();
    }
}
