package com.paicbd.smsc.smsf.core.security.tls;

import com.paicbd.smsc.dto.Http2GatewayIncomingSecurity;
import com.paicbd.smsc.exception.RTException;
import com.paicbd.smsc.smsf.core.config.AppProperties;
import com.paicbd.smsc.smsf.core.security.GatewayLookupChangedEvent;
import com.paicbd.smsc.smsf.core.security.GatewaySecurityBundle;
import com.paicbd.smsc.smsf.core.security.GatewaySecurityLookup;
import com.paicore.sba.stack.tls.PemKeyStoreLoader;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Enumeration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Component
@RequiredArgsConstructor
public class SmsfSslContextProvider {

    private final AppProperties appProperties;
    private final GatewaySecurityLookup gatewayLookup;

    private final AtomicReference<SslContext> sslContext = new AtomicReference<>();

    @PostConstruct
    public synchronized void initialize() {
        rebuild("initial load");
    }

    @EventListener
    public synchronized void onGatewayLookupChanged(GatewayLookupChangedEvent event) {
        rebuild("gateway " + event.networkId() + " " + event.type().name().toLowerCase());
    }

    @EventListener
    public synchronized void onContextRefreshed(ContextRefreshedEvent event) {
        rebuild("context refreshed application context id " + event.getApplicationContext().getId());
    }

    public synchronized void rebuild(String reason) {
        GatewayServerCert serverCert = findGatewayServerCert();
        if (Objects.isNull(serverCert)) {
            if (Objects.nonNull(this.sslContext.get())) {
                log.warn("SMSF TLS disabled ({}): no HTTP/2 gateway has server cert/key configured anymore", reason);
                this.sslContext.set(null);
            } else if ("initial load".equals(reason)) {
                log.debug("SslContext build deferred ({}): no gateways loaded yet, will rebuild on context refresh", reason);
            } else {
                log.info("SslContext not built ({}): no HTTP/2 gateway has server cert/key configured", reason);
            }
            return;
        }
        this.sslContext.set(build(serverCert));
        int trustEntries = trustEntryCount();
        String clientAuth = (trustEntries > 0) ? "OPTIONAL" : "NONE";
        log.info("SMSF TLS rebuilt ({}): server cert={} (from gateway networkId={}), trust entries={}, clientAuth={}, ALPN=h2",
                reason, serverCert.certPath(), serverCert.networkId(), trustEntries, clientAuth);
    }

    public SslContext get() {
        return sslContext.get();
    }

    private GatewayServerCert findGatewayServerCert() {
        for (GatewaySecurityBundle bundle : gatewayLookup.snapshot()) {
            Http2GatewayIncomingSecurity inc = bundle.incoming();
            if (Objects.isNull(inc)) {
                continue;
            }
            String cert = inc.getServerCertificatePath();
            String key = inc.getServerPrivateKeyPath();
            if (Objects.nonNull(cert) && !cert.isBlank()
                    && Objects.nonNull(key) && !key.isBlank()) {
                return new GatewayServerCert(bundle.networkId(), cert, key);
            }
        }
        return null;
    }

    private SslContext build(GatewayServerCert serverCert) {
        try {
            File cert = new File(serverCert.certPath());
            File key = new File(serverCert.keyPath());
            String keyPassword = appProperties.getTlsServerKeyPassword().isBlank()
                    ? null : appProperties.getTlsServerKeyPassword();

            TrustManagerFactory tmf = aggregatedTrustManager();
            ClientAuth clientAuth = (Objects.nonNull(tmf)) ? ClientAuth.OPTIONAL : ClientAuth.NONE;
            SslContextBuilder builder = SslContextBuilder.forServer(cert, key, keyPassword)
                    .protocols(appProperties.getTlsProtocols().toArray(String[]::new))
                    .clientAuth(clientAuth)
                    .applicationProtocolConfig(new ApplicationProtocolConfig(
                            ApplicationProtocolConfig.Protocol.ALPN,
                            ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                            ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                            ApplicationProtocolNames.HTTP_2,
                            ApplicationProtocolNames.HTTP_1_1));

            // Session resumption. Without it every reconnect costs a full handshake, including the
            // server's private-key operation — the dominant CPU cost when a peer opens a new
            // connection per request instead of multiplexing over one. A resumed handshake skips it.
            builder.sessionCacheSize(appProperties.getTlsSessionCacheSize())
                    .sessionTimeout(appProperties.getTlsSessionTimeoutSeconds());

            if (Objects.nonNull(tmf)) {
                builder.trustManager(tmf);
            }
            return builder.build();
        } catch (Exception ex) {
            throw new RTException("Failed to build SMSF SslContext: " + ex.getMessage(), ex);
        }
    }

    private TrustManagerFactory aggregatedTrustManager()
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        KeyStore aggregated = KeyStore.getInstance(KeyStore.getDefaultType());
        aggregated.load(null, null);
        int added = 0;
        for (GatewaySecurityBundle bundle : gatewayLookup.snapshot()) {
            Http2GatewayIncomingSecurity inc = bundle.incoming();
            if (Objects.isNull(inc) || !inc.isRequireMtls()) {
                continue;
            }
            added += copyTrustEntries(aggregated, inc.getCaCertificatePath(),
                    "gw-" + bundle.networkId() + "-ca");
            added += copyTrustEntries(aggregated, inc.getExpectedAmfCertificatePath(),
                    "gw-" + bundle.networkId() + "-peer");
        }
        if (added == 0) {
            return null;
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(aggregated);
        return tmf;
    }

    private int copyTrustEntries(KeyStore target, String path, String aliasPrefix) throws KeyStoreException {
        if (Objects.isNull(path) || path.isBlank()) {
            return 0;
        }
        KeyStore loaded = PemKeyStoreLoader.loadTrustStore(path);
        int count = 0;
        Enumeration<String> aliases = loaded.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            target.setCertificateEntry(aliasPrefix + "-" + alias, loaded.getCertificate(alias));
            count++;
        }
        return count;
    }

    private int trustEntryCount() {
        try {
            TrustManagerFactory tmf = aggregatedTrustManager();
            if (Objects.isNull(tmf)) {
                return 0;
            }
            int count = 0;
            for (var tm : tmf.getTrustManagers()) {
                if (tm instanceof javax.net.ssl.X509TrustManager x509) {
                    count = x509.getAcceptedIssuers().length;
                }
            }
            return count;
        } catch (Exception ex) {
            return -1;
        }
    }

    private record GatewayServerCert(int networkId, String certPath, String keyPath) {
    }
}
