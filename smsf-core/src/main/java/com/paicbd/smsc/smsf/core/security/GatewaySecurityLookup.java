package com.paicbd.smsc.smsf.core.security;

import com.nimbusds.jose.JWSAlgorithm;
import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.Http2GatewayIncomingSecurity;
import com.paicbd.smsc.dto.Http2GatewayNrfSecurity;
import com.paicbd.smsc.smsf.core.config.AppProperties;
import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicore.sba.stack.security.auth.AsymmetricJwtAuthenticator;
import com.paicore.sba.stack.security.auth.Authenticator;
import com.paicore.sba.stack.transport.http2.config.ConnectionPoolSettings;
import com.paicore.sba.stack.transport.http2.config.Http2ClientSettings;
import com.paicore.sba.stack.transport.http2.config.TlsSettings;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class GatewaySecurityLookup {

    private static final String PATH_SEPARATOR = "/";

    private final AppProperties appProperties;
    private final ApplicationEventPublisher eventPublisher;

    private final ConcurrentMap<Integer, GatewaySecurityBundle> byNetworkId = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, Authenticator> authenticatorByNetworkId = new ConcurrentHashMap<>();

    public Optional<GatewaySecurityBundle> findByNetworkId(int networkId) {
        return Optional.ofNullable(byNetworkId.get(networkId));
    }

    public Collection<GatewaySecurityBundle> snapshot() {
        return List.copyOf(byNetworkId.values());
    }

    public Optional<Authenticator> authenticatorForNetworkId(int networkId) {
        return Optional.ofNullable(authenticatorByNetworkId.get(networkId));
    }

    public synchronized void register(Http2Gateway gateway) {
        int networkId = gateway.getNetworkId();
        GatewaySecurityBundle previous = byNetworkId.remove(networkId);
        authenticatorByNetworkId.remove(networkId);

        if (gateway.getEnabled() != Constants.ENABLED) {
            if (Objects.nonNull(previous)) {
                log.info("HTTP/2 gateway networkId={} evicted from security lookup (disabled)", networkId);
                eventPublisher.publishEvent(new GatewayLookupChangedEvent(networkId, GatewayLookupChangedEvent.ChangeType.EVICTED));
            }
            return;
        }

        GatewaySecurityBundle bundle = toBundle(gateway);
        byNetworkId.put(networkId, bundle);
        Http2GatewayIncomingSecurity incoming = bundle.incoming();
        if (Objects.nonNull(incoming) && incoming.isOauth2Enabled()) {
            buildAuthenticator(bundle).ifPresent(
                    auth -> authenticatorByNetworkId.put(networkId, auth));
        }
        log.info("HTTP/2 gateway networkId={} registered in security lookup", networkId);
        eventPublisher.publishEvent(new GatewayLookupChangedEvent(networkId, GatewayLookupChangedEvent.ChangeType.REFRESHED));
    }

    public synchronized Optional<GatewaySecurityBundle> evict(int networkId) {
        GatewaySecurityBundle removed = byNetworkId.remove(networkId);
        authenticatorByNetworkId.remove(networkId);
        if (Objects.nonNull(removed)) {
            log.info("HTTP/2 gateway networkId={} evicted from security lookup", networkId);
            eventPublisher.publishEvent(new GatewayLookupChangedEvent(networkId, GatewayLookupChangedEvent.ChangeType.EVICTED));
        }
        return Optional.ofNullable(removed);
    }

    private Optional<Authenticator> buildAuthenticator(GatewaySecurityBundle bundle) {
        URI jwksUri = resolveJwksUri(bundle);
        if (Objects.isNull(jwksUri)) {
            return Optional.empty();
        }
        Http2ClientSettings jwksClient = buildJwksClientSettings(bundle, jwksUri);
        if (Objects.isNull(jwksClient)) {
            return Optional.empty();
        }
        String expectedIssuer = Objects.isNull(bundle.incoming()) ? null : bundle.incoming().getExpectedIssuer();
        Set<JWSAlgorithm> allowedAlgorithms = parseAlgorithms(appProperties.getSecurityAllowedJwsAlgorithms());
        Set<String> requiredScopes = parseScopes(appProperties.getSecurityExpectedScope());
        AsymmetricJwtAuthenticator authenticator = AsymmetricJwtAuthenticator.builder()
                .jwksUri(jwksUri)
                .http2BuildSettings(jwksClient)
                .expectedIssuer(expectedIssuer)
                .requiredScopes(requiredScopes)
                .allowedAlgorithms(allowedAlgorithms)
                .clockSkew(Duration.ofSeconds(appProperties.getSecurityClockSkewSeconds()))
                .verificationCacheTtlSeconds(appProperties.getSecurityJwtVerificationCacheTtlSeconds())
                .verificationCacheMaxSize(appProperties.getSecurityJwtVerificationCacheMaxSize())
                .build();
        return Optional.of(authenticator);
    }

    private Http2ClientSettings buildJwksClientSettings(GatewaySecurityBundle bundle, URI jwksUri) {
        Http2GatewayNrfSecurity nrf = bundle.nrf();
        if (Objects.isNull(nrf)) {
            return null;
        }
        String host = jwksUri.getHost();
        if (Objects.isNull(host) || host.isBlank()) {
            log.warn("HTTP/2 gateway networkId={} JWKS URI has no host: {}", bundle.networkId(), jwksUri);
            return null;
        }
        int port = jwksUri.getPort();
        if (port == -1) {
            port = "https".equalsIgnoreCase(jwksUri.getScheme()) ? 443 : 80;
        }
        String path = Objects.isNull(jwksUri.getRawPath()) || jwksUri.getRawPath().isBlank()
                ? "/" : jwksUri.getRawPath();
        boolean tlsEnabled = "https".equalsIgnoreCase(jwksUri.getScheme()) || nrf.isMtlsEnabled();

        TlsSettings.TlsSettingsBuilder<?, ?> tls = TlsSettings.builder()
                .enabled(tlsEnabled)
                .verifyHostname(tlsEnabled);
        if (nrf.isMtlsEnabled()) {
            tls.keyStorePath(nrf.getClientCertificatePath())
                    .keyPath(nrf.getClientPrivateKeyPath())
                    .trustStorePath(nrf.getTruststoreCaPath());
        }

        long timeoutMs = appProperties.getJwksFetchTimeoutMs();
        ConnectionPoolSettings pool = ConnectionPoolSettings.builder()
                .connectTimeoutMillis((int) Math.min(timeoutMs, Integer.MAX_VALUE))
                .readTimeoutMillis((int) Math.min(timeoutMs, Integer.MAX_VALUE))
                .idleTimeoutSeconds(60)
                .build();

        return Http2ClientSettings.builder()
                .key("nrf-jwks-" + bundle.networkId())
                .host(host)
                .port(port)
                .path(path)
                .tls(tls.build())
                .pool(pool)
                .build();
    }

    private URI resolveJwksUri(GatewaySecurityBundle bundle) {
        Http2GatewayNrfSecurity nrf = bundle.nrf();
        if (Objects.isNull(nrf)) {
            log.warn("HTTP/2 gateway networkId={} has no NRF security configured — cannot derive JWKS",
                    bundle.networkId());
            return null;
        }
        String apiRoot = nrf.getApiRoot();
        String secretPath = nrf.getSecretPath();
        if (Objects.isNull(apiRoot) || apiRoot.isBlank()
                || Objects.isNull(secretPath) || secretPath.isBlank()) {
            log.warn("HTTP/2 gateway networkId={} has no NRF api_root/secret_path configured — cannot derive JWKS",
                    bundle.networkId());
            return null;
        }
        String trimmedRoot = apiRoot.endsWith(PATH_SEPARATOR) ? apiRoot.substring(0, apiRoot.length() - 1) : apiRoot;
        String normalizedPath = secretPath.startsWith(PATH_SEPARATOR) ? secretPath : PATH_SEPARATOR + secretPath;
        try {
            return new URI(trimmedRoot + normalizedPath);
        } catch (URISyntaxException ex) {
            log.error("HTTP/2 gateway networkId={} configured an invalid NRF JWKS URI '{}{}': {}",
                    bundle.networkId(), trimmedRoot, normalizedPath, ex.getMessage());
            return null;
        }
    }

    private static GatewaySecurityBundle toBundle(Http2Gateway gateway) {
        return new GatewaySecurityBundle(
                gateway.getNetworkId(),
                gateway.getIncomingSecurity(),
                gateway.getNrfSecurity(),
                gateway.getOutboundSecurity());
    }

    private static Set<JWSAlgorithm> parseAlgorithms(List<String> raw) {
        if (Objects.isNull(raw) || raw.isEmpty()) {
            return Set.of(JWSAlgorithm.RS256, JWSAlgorithm.ES256);
        }
        Set<JWSAlgorithm> result = new LinkedHashSet<>();
        for (String value : raw) {
            if (Objects.isNull(value)) {
                continue;
            }
            String trimmed = value.trim();
            if (!trimmed.isEmpty()) {
                result.add(JWSAlgorithm.parse(trimmed));
            }
        }
        return result.isEmpty() ? Set.of(JWSAlgorithm.RS256, JWSAlgorithm.ES256) : result;
    }

    private static Set<String> parseScopes(String raw) {
        if (Objects.isNull(raw) || raw.isBlank()) {
            return Set.of();
        }
        Set<String> result = new LinkedHashSet<>();
        for (String value : raw.split("\\s+")) {
            if (!value.isBlank()) {
                result.add(value);
            }
        }
        return result;
    }
}
