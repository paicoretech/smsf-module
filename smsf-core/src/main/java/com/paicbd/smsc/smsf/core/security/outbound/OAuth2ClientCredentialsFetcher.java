package com.paicbd.smsc.smsf.core.security.outbound;

import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.utils.Converter;
import com.paicore.sba.stack.transport.http2.NettyHttp2Client;
import com.paicore.sba.stack.transport.http2.dto.Http2Request;
import com.paicore.sba.stack.transport.http2.dto.Http2Response;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@RequiredArgsConstructor
public class OAuth2ClientCredentialsFetcher {

    private final NettyHttp2Client httpClient;
    private final String tokenPath;
    private final String clientId;
    private final String clientSecret;
    private final String scope;
    private final String nfInstanceId;
    private final String nfType;
    private final String targetNfType;

    public OAuth2Token fetch(long timeoutSeconds) throws InterruptedException, ExecutionException, TimeoutException {
        if (!httpClient.isConnected()) {
            httpClient.connect();
        }

        String body = buildFormBody();
        Map<String, String> headers = new HashMap<>();
        headers.put(Constants.CONTENT_TYPE_HEADER, Constants.FORM_URLENCODED_CONTENT_TYPE);

        Http2Request request = Http2Request.builder()
                .method("POST")
                .path(tokenPath)
                .headers(headers)
                .body(body.getBytes(StandardCharsets.UTF_8))
                .build();

        Http2Response response = httpClient.send(request).get(timeoutSeconds, TimeUnit.SECONDS);
        int status = response.getStatus();
        if (status < 200 || status >= 300) {
            throw new IllegalStateException("Token endpoint returned HTTP " + status);
        }

        byte[] payload = response.getBody();
        if (Objects.isNull(payload) || payload.length == 0) {
            throw new IllegalStateException("Token endpoint returned an empty body");
        }

        OAuth2TokenResponse tokenResponse = Converter.stringToObject(
                new String(payload, StandardCharsets.UTF_8), OAuth2TokenResponse.class);
        if (Objects.isNull(tokenResponse)
                || Objects.isNull(tokenResponse.getAccessToken())
                || tokenResponse.getAccessToken().isBlank()) {
            throw new IllegalStateException("Token endpoint response missing access_token");
        }
        return new OAuth2Token(tokenResponse.getAccessToken(), tokenResponse.getExpiresIn(), Instant.now());
    }

    private String buildFormBody() {
        StringBuilder sb = new StringBuilder("grant_type=client_credentials");
        appendIfPresent(sb, "nfInstanceId", nfInstanceId);
        appendIfPresent(sb, "nfType", nfType);
        appendIfPresent(sb, "targetNfType", targetNfType);
        appendIfPresent(sb, "scope", scope);
        appendIfPresent(sb, "client_id", clientId);
        appendIfPresent(sb, "client_secret", clientSecret);
        return sb.toString();
    }

    private static void appendIfPresent(StringBuilder sb, String key, String value) {
        if (Objects.nonNull(value) && !value.isBlank()) {
            sb.append('&').append(key).append('=').append(URLEncoder.encode(value, StandardCharsets.UTF_8));
        }
    }
}
