package com.paicbd.smsc.smsf.core.api.error;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.netty.channel.AbortedException;
import reactor.test.StepVerifier;

import java.io.EOFException;

class DisconnectedClientWebExceptionHandlerTest {

    private final DisconnectedClientWebExceptionHandler handler = new DisconnectedClientWebExceptionHandler();

    private ServerWebExchange exchange() {
        return MockServerWebExchange.from(
                MockServerHttpRequest.post("/nsmsf-sms/v2/4/ue-contexts/imsi-001010000000001/sendsms"));
    }

    @Test
    @DisplayName("an aborted connection completes without reaching the default 500 handler")
    void swallowsAbortedConnection() {
        Mono<Void> result = handler.handle(exchange(), AbortedException.beforeSend());

        StepVerifier.create(result).verifyComplete();
    }

    @Test
    @DisplayName("a client disconnect nested as a cause is also recognised")
    void swallowsNestedClientDisconnect() {
        Mono<Void> result = handler.handle(exchange(),
                new IllegalStateException("write failed", new EOFException("closed")));

        StepVerifier.create(result).verifyComplete();
    }

    @Test
    @DisplayName("any other error is propagated so the default handler still reports it")
    void propagatesRealErrors() {
        IllegalStateException failure = new IllegalStateException("no gateway stack for networkId 4");

        StepVerifier.create(handler.handle(exchange(), failure))
                .expectErrorMatches(ex -> ex == failure)
                .verify();
    }

    @Test
    @DisplayName("a self-referencing cause chain does not loop forever")
    void toleratesSelfReferencingCause() {
        RuntimeException looping = new RuntimeException("boom") {
            @Override
            public synchronized Throwable getCause() {
                return this;
            }
        };

        StepVerifier.create(handler.handle(exchange(), looping))
                .expectError(RuntimeException.class)
                .verify();
    }
}
