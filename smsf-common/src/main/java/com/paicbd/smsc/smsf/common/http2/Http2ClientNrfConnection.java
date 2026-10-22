package com.paicbd.smsc.smsf.common.http2;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.Http2GatewayNrfSecurity;
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

import static com.paicbd.smsc.smsf.common.constants.Constants.DEFAULT_NRF_PORT;

@Slf4j
public class Http2ClientNrfConnection {

    @Getter
    private final Http2Gateway gateway;
    private final AtomicBoolean connected = new AtomicBoolean(false);

    @Getter
    private NettyHttp2Client client;

    public Http2ClientNrfConnection(Http2Gateway gateway) {
        this.gateway = gateway;
    }

    public synchronized void connect() {
        Http2GatewayNrfSecurity nrf = gateway.getNrfSecurity();
        if (Objects.isNull(nrf) || Objects.isNull(nrf.getApiRoot()) || nrf.getApiRoot().isBlank()) {
            log.warn("NRF configuration missing for networkId {} — skipping NRF client start",
                    gateway.getNetworkId());
            return;
        }
        if (connected.get()) {
            log.warn("HTTP/2 NRF client for networkId {} is already connected", gateway.getNetworkId());
            return;
        }
        try {
            Http2ClientSettings settings = buildSettings(nrf);
            this.client = new NettyHttp2Client(settings);
            this.client.connect();
            connected.set(true);
            log.info("HTTP/2 NRF client started for networkId {} -> {}:{} (mTLS={})",
                    gateway.getNetworkId(), settings.getHost(), settings.getPort(), nrf.isMtlsEnabled());
        } catch (Exception ex) {
            log.error("Failed to start HTTP/2 NRF client for networkId {}: {}",
                    gateway.getNetworkId(), ex.getMessage(), ex);
            safeClose();
        }
    }

    public synchronized void disconnect() {
        if (!connected.get()) return;
        safeClose();
        log.info("HTTP/2 NRF client stopped for networkId {}", gateway.getNetworkId());
    }

    public boolean isConnected() {
        return connected.get() && Objects.nonNull(client) && client.isConnected();
    }

    public CompletableFuture<Http2Response> send(Http2Request request) {
        if (!isConnected()) {
            CompletableFuture<Http2Response> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException(
                    "HTTP/2 NRF client for networkId " + gateway.getNetworkId() + " is not connected"));
            return failed;
        }
        return client.send(request);
    }

    private Http2ClientSettings buildSettings(Http2GatewayNrfSecurity nrf) {
        Http2HostUtils.HostPort hp = Http2HostUtils.parseHostPort(nrf.getApiRoot(), DEFAULT_NRF_PORT);
        ConnectionPoolSettings pool = ConnectionPoolSettings.builder()
                .connectTimeoutMillis(gateway.getConnectionTimeoutMs())
                .readTimeoutMillis(gateway.getRequestTimeoutMs())
                .idleTimeoutSeconds(Math.max(1, gateway.getIdleTimeoutMs() / 1000))
                .build();

        return Http2ClientSettings.builder()
                .host(hp.host())
                .port(hp.port())
                .tls(buildTlsSettings(nrf))
                .pool(pool)
                .build();
    }

    private TlsSettings buildTlsSettings(Http2GatewayNrfSecurity nrf) {
        TlsSettings.TlsSettingsBuilder<?, ?> builder = TlsSettings.builder()
                .enabled(nrf.isMtlsEnabled())
                .verifyHostname(nrf.isMtlsEnabled());

        if (nrf.isMtlsEnabled()) {
            builder.keyStorePath(nrf.getClientCertificatePath())
                    .keyPath(nrf.getClientPrivateKeyPath())
                    .trustStorePath(nrf.getTruststoreCaPath());
        }
        return builder.build();
    }

    private void safeClose() {
        connected.set(false);
        if (Objects.nonNull(client)) {
            try {
                client.close();
            } catch (Exception ex) {
                log.warn("Error while closing HTTP/2 NRF client for networkId {}: {}",
                        gateway.getNetworkId(), ex.getMessage());
            } finally {
                client = null;
            }
        }
    }
}
