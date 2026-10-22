package com.paicbd.smsc.smsf.core.api.controller;

import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.core.api.exception.DlrCorrelationNotFoundException;
import com.paicbd.smsc.smsf.core.api.service.MoSmsService;
import com.paicbd.smsc.smsf.core.api.utils.ProblemResponses;
import com.paicore.sba.stack.openapi.model.SmsRecordDeliveryData;
import com.paicore.sba.stack.openapi.model.SmsDeliveryStatus;
import com.paicore.sba.stack.validation.SupiValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.Executors;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/{networkId}" + Constants.UE_CONTEXTS_PATH)
public class MoSmsController {

    private static final String CAUSE_INTERNAL_ERROR = "INTERNAL_ERROR";

    private static final Scheduler MO_SCHEDULER =
            Schedulers.fromExecutorService(Executors.newVirtualThreadPerTaskExecutor());

    private final MoSmsService moSmsService;

    @PostMapping(value = "/{supi}" + Constants.SENDSMS_SUFFIX, consumes = Constants.MULTIPART_RELATED_CONTENT_TYPE)
    public Mono<ResponseEntity<Object>> sendSms(
            @PathVariable int networkId,
            @PathVariable String supi,
            @RequestHeader(HttpHeaders.CONTENT_TYPE) String contentType,
            @RequestBody byte[] body) {

        String instance = Constants.UE_CONTEXTS_PATH + "/" + supi + Constants.SENDSMS_SUFFIX;

        if (!SupiValidator.isValid(supi)) {
            return Mono.just(ProblemResponses.badRequest(
                    instance,
                    Constants.CAUSE_INVALID_SUPI,
                    "SUPI does not match the expected format"));
        }

        return Mono.fromCallable(() -> moSmsService.processMoSms(networkId, supi, contentType, body))
                .subscribeOn(MO_SCHEDULER)
                .map(messageId -> {
                    log.debug("MO SMS accepted networkId={} supi={} messageId={}", networkId, supi, messageId);
                    SmsRecordDeliveryData echo = new SmsRecordDeliveryData().smsRecordId(messageId).deliveryStatus(SmsDeliveryStatus.SMSF_ACCEPTED);
                    return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body((Object) echo);
                })
                .onErrorResume(DlrCorrelationNotFoundException.class, ex -> {
                    log.warn("MO SMS DLR correlation not found networkId={} supi={}: {}", networkId, supi, ex.getMessage());
                    return Mono.just(ProblemResponses.notFound(
                            instance,
                            Constants.CAUSE_CONTEXT_NOT_FOUND,
                            ex.getMessage()));
                })
                .onErrorResume(ex -> {
                    log.error("MO SMS rejected networkId={} supi={}: {}", networkId, supi, ex.getMessage(), ex);
                    return Mono.just(ProblemResponses.internalServerError(
                            instance,
                            CAUSE_INTERNAL_ERROR,
                            ex.getMessage()));
                });
    }
}
