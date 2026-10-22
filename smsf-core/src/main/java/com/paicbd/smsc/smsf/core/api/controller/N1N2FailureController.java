package com.paicbd.smsc.smsf.core.api.controller;

import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.core.api.service.N1N2FailureService;
import com.paicbd.smsc.smsf.core.api.utils.ProblemResponses;
import com.paicore.sba.stack.openapi.model.N1N2MsgTxfrFailureNotification;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Objects;
import java.util.concurrent.Executors;

/**
 * Receives the {@code N1N2MsgTxfrFailureNotification} the AMF posts to the callback URI the MT
 * advertised in {@code n1n2FailureTxfNotifURI}.
 *
 * <p>TS 29.518 fixes the method, the body and the {@code 204} response; the URI itself is the
 * consumer's to choose. It hangs off the SBI base path so it inherits the inbound security model
 * unchanged: the same TLS listener, the same token validation and the same gateway resolution by
 * {@code networkId} the MO already uses.</p>
 *
 * <p>The delivery is closed here rather than handed to another module: the correlation this module
 * already reads carries everything the CDR and the receipt need.</p>
 */
@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/{networkId}" + Constants.NAMF_CALLBACK_PATH)
public class N1N2FailureController {

    private static final String INSTANCE = Constants.NAMF_CALLBACK_PATH + Constants.N1N2_FAILURE_SUFFIX;
    private static final String CAUSE_MANDATORY_IE_MISSING = "MANDATORY_IE_MISSING";

    private static final Scheduler CALLBACK_SCHEDULER =
            Schedulers.fromExecutorService(Executors.newVirtualThreadPerTaskExecutor());

    private final N1N2FailureService n1n2FailureService;

    @PostMapping(Constants.N1N2_FAILURE_SUFFIX + "/{messageId}")
    public Mono<ResponseEntity<Object>> n1n2Failure(
            @PathVariable int networkId,
            @PathVariable String messageId,
            @RequestBody N1N2MsgTxfrFailureNotification notification) {

        if (Objects.isNull(notification.getCause())) {
            return Mono.just(ProblemResponses.badRequest(
                    INSTANCE, CAUSE_MANDATORY_IE_MISSING, "cause is required"));
        }

        return Mono.fromRunnable(() -> n1n2FailureService.processFailureNotification(networkId, messageId, notification))
                .subscribeOn(CALLBACK_SCHEDULER)
                .thenReturn(ResponseEntity.noContent().build())
                .onErrorResume(ex -> {
                    log.error("Failed to close the delivery on the N1N2 failure networkId={} messageId={}: {}",
                            networkId, messageId, ex.getMessage(), ex);
                    return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
                });
    }
}
