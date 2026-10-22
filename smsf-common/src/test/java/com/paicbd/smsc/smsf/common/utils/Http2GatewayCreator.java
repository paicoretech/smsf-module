package com.paicbd.smsc.smsf.common.utils;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.Http2GatewayNrfSecurity;
import com.paicbd.smsc.dto.Http2GatewayOutboundSecurity;

public class Http2GatewayCreator {

    private Http2GatewayCreator() {
    }

    public static Http2Gateway buildHttp2Gateway(String name, int networkId, String host, int enabled) {
        Http2GatewayOutboundSecurity outbound = new Http2GatewayOutboundSecurity();
        outbound.setOauth2Enabled(false);
        outbound.setMtlsEnabled(false);

        Http2GatewayNrfSecurity nrf = new Http2GatewayNrfSecurity();
        nrf.setMtlsEnabled(false);

        return Http2Gateway.builder()
                .networkId(networkId)
                // Assigned by smsc-management-be in production; deterministic here so a test can
                // assert the exact URL the SMSF builds.
                .nfInstanceId(nfInstanceIdFor(networkId))
                .name(name)
                .host(host)
                .enabled(enabled)
                .protocol("HTTP2")
                .mnoId(1)
                .multipartParsingEnabled(false)
                .binarySmsEnabled(false)
                .maxMessageSize(160)
                .defaultDataCodingId(0)
                .requestTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .idleTimeoutMs(60000)
                .maxResponseTimeMs(5000)
                .threadPoolSize(10)
                .messagesPerSecond(100)
                .messagesPerSecondHigh(50)
                .messagesPerSecondMedium(30)
                .messagesPerSecondLow(20)
                .rateLimitingEnabled(false)
                .requestDlr(0)
                .outboundSecurity(outbound)
                .nrfSecurity(nrf)
                .build();
    }

    public static Http2Gateway buildHttp2GatewayWithMtls(String name, int networkId, String host, int enabled) {
        Http2GatewayOutboundSecurity outbound = new Http2GatewayOutboundSecurity();
        outbound.setOauth2Enabled(false);
        outbound.setMtlsEnabled(true);
        outbound.setClientCertificatePath("/path/to/client.crt");
        outbound.setClientPrivateKeyPath("/path/to/client.key");
        outbound.setTruststoreCaPath("/path/to/truststore.jks");

        Http2GatewayNrfSecurity nrf = new Http2GatewayNrfSecurity();
        nrf.setMtlsEnabled(true);
        nrf.setApiRoot("https://nrf.example.com:8443");
        nrf.setSecretPath("/oauth2/jwks");
        nrf.setTokenPath("/oauth2/token");
        // Service path only. The nfInstanceId is appended by the code, never configured here —
        // and it is a UUID, not the SMSC networkId this fixture used to put in its place.
        nrf.setNrfRegisterPath("/nnrf-nfm/v1/nf-instances");
        nrf.setClientCertificatePath("/path/to/nrf-client.crt");
        nrf.setClientPrivateKeyPath("/path/to/nrf-client.key");
        nrf.setTruststoreCaPath("/path/to/nrf-truststore.jks");

        return Http2Gateway.builder()
                .networkId(networkId)
                .nfInstanceId(nfInstanceIdFor(networkId))
                .name(name)
                .host(host)
                .enabled(enabled)
                .protocol("HTTP2")
                .mnoId(1)
                .multipartParsingEnabled(false)
                .binarySmsEnabled(false)
                .maxMessageSize(160)
                .defaultDataCodingId(0)
                .requestTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .idleTimeoutMs(60000)
                .maxResponseTimeMs(5000)
                .threadPoolSize(10)
                .messagesPerSecond(100)
                .messagesPerSecondHigh(50)
                .messagesPerSecondMedium(30)
                .messagesPerSecondLow(20)
                .rateLimitingEnabled(false)
                .requestDlr(0)
                .outboundSecurity(outbound)
                .nrfSecurity(nrf)
                .build();
    }

    /** Stable per networkId, so the same gateway always gets the same identity across runs. */
    public static java.util.UUID nfInstanceIdFor(int networkId) {
        return java.util.UUID.nameUUIDFromBytes(("smsf-gateway-" + networkId).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
