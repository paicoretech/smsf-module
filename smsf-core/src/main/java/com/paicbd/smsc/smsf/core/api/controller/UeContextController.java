package com.paicbd.smsc.smsf.core.api.controller;

import com.paicbd.smsc.smsf.core.config.AppProperties;
import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.core.api.service.RegisterService;
import com.paicbd.smsc.smsf.core.api.utils.ProblemResponses;
import com.paicore.sba.stack.openapi.model.UeSmsContextData;
import com.paicore.sba.stack.validation.SupiValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.net.URI;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/{networkId}" + Constants.UE_CONTEXTS_PATH)
public class UeContextController {

    private static final String CAUSE_ETAG_MISMATCH = "ETAG_MISMATCH";

    private final RegisterService registerService;
    private final AppProperties appProperties;

    @PutMapping(value = "/{supi}", consumes = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Object>> upsert(
            @PathVariable int networkId,
            @PathVariable String supi,
            @RequestBody UeSmsContextData body) {

        if (!SupiValidator.isValid(supi)) {
            return Mono.just(ProblemResponses.badRequest(
                    Constants.UE_CONTEXTS_PATH + "/" + supi, Constants.CAUSE_INVALID_SUPI, "SUPI does not match the expected format"));
        }
        if (!supi.equals(body.getSupi())) {
            return Mono.just(ProblemResponses.badRequest(
                    Constants.UE_CONTEXTS_PATH + "/" + supi, Constants.CAUSE_INVALID_SUPI, "Body SUPI does not match the path"));
        }

        return Mono.fromCallable(() -> registerService.upsertUeContext(body))
                .subscribeOn(Schedulers.boundedElastic())
                .doOnNext(result -> registerService.publishRegistration(networkId, body))
                .map(result -> {
                    log.info("ue-context {} supi={} networkId={} etag={}",
                            result.outcome(), supi, networkId, result.etag());
                    if (result.outcome() == RegisterService.Outcome.CREATED) {
                        URI location = URI.create(String.format("%s/%d%s/%s",
                                appProperties.getSmsfSbiServerBasePath(), networkId,
                                Constants.UE_CONTEXTS_PATH, supi));
                        return ResponseEntity.created(location)
                                .eTag(result.etag())
                                .contentType(MediaType.APPLICATION_JSON)
                                .body(body);
                    }
                    return ResponseEntity.noContent().eTag(result.etag()).build();
                });
    }

    @DeleteMapping("/{supi}")
    public Mono<ResponseEntity<Object>> delete(
            @PathVariable int networkId,
            @PathVariable String supi,
            @RequestHeader(value = "If-Match", required = false) String ifMatch) {

        if (!SupiValidator.isValid(supi)) {
            return Mono.just(ProblemResponses.badRequest(
                    Constants.UE_CONTEXTS_PATH + "/" + supi, Constants.CAUSE_INVALID_SUPI, "SUPI does not match the expected format"));
        }

        return Mono.fromCallable(() -> registerService.deleteUeContext(supi, ifMatch))
                .subscribeOn(Schedulers.boundedElastic())
                .doOnNext(result -> {
                    if (result.outcome() == RegisterService.Outcome.DELETED) {
                        registerService.publishDeregistration(networkId, supi);
                    }
                })
                .map(result -> {
                    if (result.outcome() == RegisterService.Outcome.ETAG_MISMATCH) {
                        return ProblemResponses.preconditionFailed(
                                Constants.UE_CONTEXTS_PATH + "/" + supi,
                                CAUSE_ETAG_MISMATCH,
                                "ETag does not match the current resource");
                    }
                    // 204 either way: DELETE stays idempotent so an AMF retry is not turned into an
                    // error. Only the outcome distinguishes them, and only to decide whether the
                    // deregistration is worth publishing.
                    if (result.outcome() == RegisterService.Outcome.NOT_FOUND) {
                        log.info("ue-context delete ignored, supi={} is not stored networkId={}", supi, networkId);
                        return ResponseEntity.noContent().build();
                    }
                    log.info("ue-context deleted supi={} networkId={}", supi, networkId);
                    return ResponseEntity.noContent().build();
                });
    }
}
