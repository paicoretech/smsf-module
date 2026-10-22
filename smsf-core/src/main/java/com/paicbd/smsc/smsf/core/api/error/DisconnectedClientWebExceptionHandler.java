package com.paicbd.smsc.smsf.core.api.error;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebExceptionHandler;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.Set;

@Slf4j
@Component
@Order(-2)
public class DisconnectedClientWebExceptionHandler implements WebExceptionHandler {

    private static final Set<String> CLIENT_DISCONNECT_TYPES = Set.of(
            "AbortedException",
            "ClientAbortException",
            "EOFException",
            "EofException");

    @Override
    public Mono<Void> handle(ServerWebExchange exchange, Throwable ex) {
        if (!isClientDisconnect(ex)) {
            return Mono.error(ex);
        }
        log.debug("Client disconnected before the response was written for {}: {}",
                exchange.getRequest().getPath().value(), ex.getMessage());
        return Mono.empty();
    }

    private boolean isClientDisconnect(Throwable ex) {
        Throwable current = ex;
        while (Objects.nonNull(current)) {
            if (CLIENT_DISCONNECT_TYPES.contains(current.getClass().getSimpleName())) {
                return true;
            }
            current = current.getCause() == current ? null : current.getCause();
        }
        return false;
    }
}
