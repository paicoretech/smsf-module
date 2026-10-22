package com.paicbd.smsc.smsf.core.http2;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.smsf.common.http2.Http2HostUtils;
import com.paicbd.smsc.dto.Http2GatewayOutboundSecurity;
import com.paicore.sba.stack.transport.http2.NettyHttp2Client;
import com.paicore.sba.stack.transport.http2.config.ConnectionPoolSettings;
import com.paicore.sba.stack.transport.http2.config.Http2ClientSettings;
import com.paicore.sba.stack.transport.http2.config.TlsSettings;
import com.paicore.sba.stack.transport.http2.dto.Http2Request;
import com.paicore.sba.stack.transport.http2.dto.Http2Response;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.paicbd.smsc.smsf.common.constants.Constants.DEFAULT_TOKEN_PORT;

/**
 * Dedicated HTTP/2 client for an external OAuth2 token endpoint. Used when the
 * outbound security is configured with {@code usingExternalEndpoint} instead of
 * obtaining the access token from the NRF. The connection targets the host/port
 * parsed from {@link Http2GatewayOutboundSecurity#getTokenEndpoint()} and honours
 * the outbound mTLS settings.
 */
@Slf4j
public class Http2ClientTokenConnection {

    @Getter
    private final Http2Gateway gateway;
    private final AtomicBoolean connected = new AtomicBoolean(false);

    @Getter
    private NettyHttp2Client client;

    public Http2ClientTokenConnection(Http2Gateway gateway) {
        this.gateway = gateway;
    }

    public synchronized void connect() {
        Http2GatewayOutboundSecurity outbound = gateway.getOutboundSecurity();
        if (Objects.isNull(outbound) || Objects.isNull(outbound.getTokenEndpoint())
                || outbound.getTokenEndpoint().isBlank()) {
            log.warn("External token endpoint missing for networkId {} — skipping token client start",
                    gateway.getNetworkId());
            return;
        }
        if (connected.get()) {
            log.warn("HTTP/2 token client for networkId {} is already connected", gateway.getNetworkId());
            return;
        }
        try {
            Http2ClientSettings settings = buildSettings(outbound.getTokenEndpoint());
            this.client = new NettyHttp2Client(settings);
            this.client.connect();
            connected.set(true);
            log.info("HTTP/2 token client started for networkId {} -> {}:{} (mTLS={})",
                    gateway.getNetworkId(), settings.getHost(), settings.getPort(), isMtlsEnabled());
        } catch (Exception ex) {
            log.error("Failed to start HTTP/2 token client for networkId {}: {}",
                    gateway.getNetworkId(), ex.getMessage(), ex);
            safeClose();
        }
    }

    public synchronized void disconnect() {
        if (!connected.get()) return;
        safeClose();
        log.info("HTTP/2 token client stopped for networkId {}", gateway.getNetworkId());
    }

    public boolean isConnected() {
        return connected.get() && Objects.nonNull(client) && client.isConnected();
    }

    public CompletableFuture<Http2Response> send(Http2Request request) {
        if (!isConnected()) {
            CompletableFuture<Http2Response> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException(
                    "HTTP/2 token client for networkId " + gateway.getNetworkId() + " is not connected"));
            return failed;
        }
        return client.send(request);
    }

    private Http2ClientSettings buildSettings(String tokenEndpoint) {
        Http2HostUtils.HostPort hp = Http2HostUtils.parseHostPort(tokenEndpoint, DEFAULT_TOKEN_PORT);
        ConnectionPoolSettings pool = ConnectionPoolSettings.builder()
                .connectTimeoutMillis(gateway.getConnectionTimeoutMs())
                .readTimeoutMillis(gateway.getRequestTimeoutMs())
                .idleTimeoutSeconds(Math.max(1, gateway.getIdleTimeoutMs() / 1000))
                .build();

        return Http2ClientSettings.builder()
                .host(hp.host())
                .port(hp.port())
                .tls(buildTlsSettings())
                .pool(pool)
                .build();
    }

    private TlsSettings buildTlsSettings() {
        boolean mTls = isMtlsEnabled();
        TlsSettings.TlsSettingsBuilder<?, ?> builder = TlsSettings.builder()
                .enabled(mTls)
                .verifyHostname(mTls);

        if (mTls) {
            Http2GatewayOutboundSecurity sec = gateway.getOutboundSecurity();
            builder.keyStorePath(sec.getClientCertificatePath())
                    .keyPath(sec.getClientPrivateKeyPath())
                    .trustStorePath(sec.getTruststoreCaPath());
        }
        return builder.build();
    }

    private boolean isMtlsEnabled() {
        Http2GatewayOutboundSecurity sec = gateway.getOutboundSecurity();
        return Objects.nonNull(sec) && sec.isMtlsEnabled();
    }

    private void safeClose() {
        connected.set(false);
        if (Objects.nonNull(client)) {
            try {
                client.close();
            } catch (Exception ex) {
                log.warn("Error while closing HTTP/2 token client for networkId {}: {}",
                        gateway.getNetworkId(), ex.getMessage());
            } finally {
                client = null;
            }
        }
    }
}
