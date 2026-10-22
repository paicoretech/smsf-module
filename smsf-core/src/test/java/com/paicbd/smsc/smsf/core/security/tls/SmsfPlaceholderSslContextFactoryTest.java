package com.paicbd.smsc.smsf.core.security.tls;

import com.paicbd.smsc.exception.RTException;
import io.netty.handler.ssl.SslContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mockConstruction;

class SmsfPlaceholderSslContextFactoryTest {

    @Test
    @DisplayName("build returns a server SslContext loaded from classpath")
    void buildProducesServerContext() {
        SslContext context = SmsfPlaceholderSslContextFactory.build();

        assertThat(context).isNotNull();
        assertThat(context.isServer()).isTrue();
    }

    @Test
    @DisplayName("build returns a new SslContext on every call")
    void buildIsRepeatable() {
        SslContext first = SmsfPlaceholderSslContextFactory.build();
        SslContext second = SmsfPlaceholderSslContextFactory.build();

        assertThat(first).isNotSameAs(second);
    }

    @Test
    @DisplayName("when the bundled placeholder certificate cannot be read, it fails loudly")
    void buildFailsWhenTheResourceIsUnreadable() {
        // The certificate ships inside the jar, so the only way this breaks in production is a
        // corrupted or repackaged artifact. It must surface as an error, never as a null context
        // that would leave the SBI listener silently without TLS.
        try (MockedConstruction<ClassPathResource> ignored = mockConstruction(
                ClassPathResource.class,
                (mock, ctx) -> when(mock.getInputStream()).thenThrow(new IOException("resource gone")))) {

            assertThatThrownBy(SmsfPlaceholderSslContextFactory::build)
                    .isInstanceOf(RTException.class)
                    .hasMessageContaining("Failed to build placeholder SslContext")
                    .hasRootCauseMessage("resource gone");
        }
    }
}
