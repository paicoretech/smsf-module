package com.paicbd.smsc.smsf.mt.http2;

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

@Slf4j
public class Http2ClientConnection {

    private static final int DEFAULT_HTTP2_PORT = 8443;

    @Getter
    private final Http2Gateway gateway;
    private final AtomicBoolean connected = new AtomicBoolean(false);

    private NettyHttp2Client client;

    public Http2ClientConnection(Http2Gateway gateway) {
        this.gateway = gateway;
    }

    /**
     * Opens the outbound client, or reopens it when the peer went away.
     *
     * <p>The check is {@link #isConnected()} and not the flag alone: after the AMF restarts, the
     * channel is dead while the flag still reads connected, and deciding on the flag would make
     * this a no-op forever — the watchdog would keep calling it, the connection would never come
     * back and the consumers would stay paused until the module was restarted. A client that is
     * held but no longer usable is closed first.</p>
     */
    public synchronized void connect() {
        if (isConnected()) {
            log.warn("HTTP/2 outbound client for networkId {} is already connected", gateway.getNetworkId());
            return;
        }
        if (connected.get()) {
            log.warn("HTTP/2 outbound client for networkId {} lost its connection — reconnecting",
                    gateway.getNetworkId());
            safeClose();
        }
        try {
            Http2ClientSettings settings = buildSettings();
            this.client = new NettyHttp2Client(settings);
            this.client.connect();
            connected.set(true);
            log.info("HTTP/2 outbound client started for networkId {} -> {}:{} (mTLS={})", gateway.getNetworkId(), settings.getHost(), settings.getPort(), isMtlsEnabled());
        } catch (Exception ex) {
            log.error("Failed to start HTTP/2 outbound client for networkId {}: {}", gateway.getNetworkId(), ex.getMessage(), ex);
            safeClose();
        }
    }

    public synchronized void disconnect() {
        if (!connected.get()) return;
        safeClose();
        log.info("HTTP/2 outbound client stopped for networkId {}", gateway.getNetworkId());
    }

    public boolean isConnected() {
        return connected.get() && Objects.nonNull(client) && client.isConnected();
    }

    public CompletableFuture<Http2Response> send(Http2Request request) {
        if (!isConnected()) {
            CompletableFuture<Http2Response> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException(
                    "HTTP/2 outbound client for networkId " + gateway.getNetworkId() + " is not connected"));
            return failed;
        }
        return client.send(request);
    }

    private Http2ClientSettings buildSettings() {
        Http2HostUtils.HostPort hp = Http2HostUtils.parseHostPort(gateway.getHost(), DEFAULT_HTTP2_PORT);
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
                log.warn("Error while closing HTTP/2 outbound client for networkId {}: {}",
                        gateway.getNetworkId(), ex.getMessage());
            } finally {
                client = null;
            }
        }
    }
}
