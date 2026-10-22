package com.paicbd.smsc.smsf.core.security.tls;

import com.paicbd.smsc.exception.RTException;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.springframework.core.io.ClassPathResource;

import java.io.InputStream;

public final class SmsfPlaceholderSslContextFactory {

    private SmsfPlaceholderSslContextFactory() {
    }

    public static SslContext build() {
        try (InputStream cert = new ClassPathResource("tls/placeholder.cert.pem").getInputStream();
             InputStream key = new ClassPathResource("tls/placeholder.key.pem").getInputStream()) {
            return SslContextBuilder.forServer(cert, key)
                    .applicationProtocolConfig(new ApplicationProtocolConfig(
                            ApplicationProtocolConfig.Protocol.ALPN,
                            ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                            ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                            ApplicationProtocolNames.HTTP_2,
                            ApplicationProtocolNames.HTTP_1_1))
                    .build();
        } catch (Exception ex) {
            throw new RTException("Failed to build placeholder SslContext: " + ex.getMessage(), ex);
        }
    }
}
