package com.paicbd.smsc.smsf.core.api.utils;

import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicore.sba.stack.api.error.ProblemDetailsResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.time.Instant;

public final class ProblemResponses {

    public static final MediaType PROBLEM_JSON = MediaType.valueOf(Constants.PROBLEM_JSON_CONTENT_TYPE);

    private ProblemResponses() {
    }

    public static ResponseEntity<Object> badRequest(String instance, String cause, String detail) {
        return build(HttpStatus.BAD_REQUEST, instance, cause, detail);
    }

    public static ResponseEntity<Object> preconditionFailed(String instance, String cause, String detail) {
        return build(HttpStatus.PRECONDITION_FAILED, instance, cause, detail);
    }

    public static ResponseEntity<Object> notFound(String instance, String cause, String detail) {
        return build(HttpStatus.NOT_FOUND, instance, cause, detail);
    }

    public static ResponseEntity<Object> internalServerError(String instance, String cause, String detail) {
        return build(HttpStatus.INTERNAL_SERVER_ERROR, instance, cause, detail);
    }

    private static ResponseEntity<Object> build(HttpStatus status, String instance, String cause, String detail) {
        ProblemDetailsResponse body = new ProblemDetailsResponse(
                null,
                status.getReasonPhrase(),
                status.value(),
                detail,
                instance,
                cause,
                Instant.now().toString());
        return ResponseEntity.status(status).contentType(PROBLEM_JSON).body(body);
    }
}
