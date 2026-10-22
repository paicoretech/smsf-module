package com.paicbd.smsc.smsf.core.security.outbound;

import com.paicbd.smsc.utils.Converter;
import com.paicore.sba.stack.transport.http2.NettyHttp2Client;
import com.paicore.sba.stack.transport.http2.dto.Http2Request;
import com.paicore.sba.stack.transport.http2.dto.Http2Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OAuth2ClientCredentialsFetcherTest {

    private NettyHttp2Client httpClient;

    @BeforeEach
    void setUp() {
        httpClient = mock(NettyHttp2Client.class);
        when(httpClient.isConnected()).thenReturn(true);
    }

    @Test
    void fetchParsesAccessTokenAndExpiresIn() throws Exception {
        Http2Response response = Http2Response.builder()
                .status(200)
                .body("{\"access_token\":\"abc.def.ghi\",\"expires_in\":600,\"token_type\":\"Bearer\"}"
                        .getBytes(StandardCharsets.UTF_8))
                .build();
        when(httpClient.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        OAuth2ClientCredentialsFetcher fetcher = new OAuth2ClientCredentialsFetcher(
                httpClient, "/oauth2/token", "smsf-id", "secret", "nsmsf-sms", "test-nfInstanceId", "SMSF", "AMF");

        OAuth2Token token = fetcher.fetch(5);

        assertEquals("abc.def.ghi", token.accessToken());
        assertEquals(600L, token.expiresInSeconds());
    }

    @Test
    void fetchBuildsClientCredentialsBody() throws Exception {
        Http2Response response = Http2Response.builder()
                .status(200)
                .body("{\"access_token\":\"t\",\"expires_in\":60}".getBytes(StandardCharsets.UTF_8))
                .build();
        ArgumentCaptor<Http2Request> captor = ArgumentCaptor.forClass(Http2Request.class);
        when(httpClient.send(captor.capture())).thenReturn(CompletableFuture.completedFuture(response));

        OAuth2ClientCredentialsFetcher fetcher = new OAuth2ClientCredentialsFetcher(
                httpClient, "/oauth2/token", "smsf-id", "se cret", "nsmsf-sms", "test-nfInstanceId", "SMSF", "AMF");
        fetcher.fetch(5);

        Http2Request sent = captor.getValue();
        assertEquals("POST", sent.getMethod());
        assertEquals("/oauth2/token", sent.getPath());
        String body = new String(sent.getBody(), StandardCharsets.UTF_8);
        assertTrue(body.contains("grant_type=client_credentials"));
        assertTrue(body.contains("client_id=smsf-id"));
        assertTrue(body.contains("client_secret=se+cret") || body.contains("client_secret=se%20cret"));
        assertTrue(body.contains("scope=nsmsf-sms"));
        assertEquals("application/x-www-form-urlencoded", sent.getHeaders().get("content-type"));
    }

    @Test
    void fetchConnectsLazilyWhenClientNotConnected() throws Exception {
        when(httpClient.isConnected()).thenReturn(false);
        Http2Response response = Http2Response.builder()
                .status(200)
                .body("{\"access_token\":\"t\",\"expires_in\":60}".getBytes(StandardCharsets.UTF_8))
                .build();
        when(httpClient.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        OAuth2ClientCredentialsFetcher fetcher = new OAuth2ClientCredentialsFetcher(
                httpClient, "/t", "id", "sec", null, "test-nfInstanceId", "SMSF", "AMF");

        fetcher.fetch(5);

        verify(httpClient).connect();
    }

    @Test
    void fetchDoesNotReconnectWhenClientAlreadyConnected() throws Exception {
        Http2Response response = Http2Response.builder()
                .status(200)
                .body("{\"access_token\":\"t\",\"expires_in\":60}".getBytes(StandardCharsets.UTF_8))
                .build();
        when(httpClient.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        OAuth2ClientCredentialsFetcher fetcher = new OAuth2ClientCredentialsFetcher(
                httpClient, "/t", "id", "sec", null, "test-nfInstanceId", "SMSF", "AMF");

        fetcher.fetch(5);

        verify(httpClient, never()).connect();
    }

    @Test
    void fetchThrowsOnNon2xxStatus() {
        Http2Response response = Http2Response.builder()
                .status(401)
                .body("nope".getBytes(StandardCharsets.UTF_8))
                .build();
        when(httpClient.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        OAuth2ClientCredentialsFetcher fetcher = new OAuth2ClientCredentialsFetcher(
                httpClient, "/t", "id", "sec", null, "test-nfInstanceId", "SMSF", "AMF");

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> fetcher.fetch(5));
        assertTrue(ex.getMessage().contains("401"));
    }

    @Test
    void fetchThrowsOn1xxStatus() {
        Http2Response response = Http2Response.builder()
                .status(199)
                .body("pending".getBytes(StandardCharsets.UTF_8))
                .build();
        when(httpClient.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        OAuth2ClientCredentialsFetcher fetcher = new OAuth2ClientCredentialsFetcher(
                httpClient, "/t", "id", "sec", null, "test-nfInstanceId", "SMSF", "AMF");

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> fetcher.fetch(5));
        assertTrue(ex.getMessage().contains("199"));
    }

    @Test
    void fetchThrowsOnEmptyBody() {
        Http2Response response = Http2Response.builder().status(200).body(new byte[0]).build();
        when(httpClient.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        OAuth2ClientCredentialsFetcher fetcher = new OAuth2ClientCredentialsFetcher(
                httpClient, "/t", "id", "sec", null, "test-nfInstanceId", "SMSF", "AMF");

        assertThrows(IllegalStateException.class, () -> fetcher.fetch(5));
    }

    @Test
    void fetchThrowsOnNullBody() {
        Http2Response response = Http2Response.builder().status(200).body(null).build();
        when(httpClient.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        OAuth2ClientCredentialsFetcher fetcher = new OAuth2ClientCredentialsFetcher(
                httpClient, "/t", "id", "sec", null, "test-nfInstanceId", "SMSF", "AMF");

        assertThrows(IllegalStateException.class, () -> fetcher.fetch(5));
    }

    @Test
    void fetchThrowsWhenAccessTokenMissing() {
        Http2Response response = Http2Response.builder()
                .status(200)
                .body("{\"expires_in\":60}".getBytes(StandardCharsets.UTF_8))
                .build();
        when(httpClient.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        OAuth2ClientCredentialsFetcher fetcher = new OAuth2ClientCredentialsFetcher(
                httpClient, "/t", "id", "sec", null, "test-nfInstanceId", "SMSF", "AMF");

        assertThrows(IllegalStateException.class, () -> fetcher.fetch(5));
    }

    @Test
    void fetchThrowsWhenTokenResponseIsNull() {
        String payload = "{\"access_token\":\"abc\",\"expires_in\":60}";
        Http2Response response = Http2Response.builder()
                .status(200)
                .body(payload.getBytes(StandardCharsets.UTF_8))
                .build();
        when(httpClient.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        OAuth2ClientCredentialsFetcher fetcher = new OAuth2ClientCredentialsFetcher(
                httpClient, "/t", "id", "sec", null, "test-nfInstanceId", "SMSF", "AMF");

        try (MockedStatic<Converter> converterMock = mockStatic(Converter.class, org.mockito.Mockito.CALLS_REAL_METHODS)) {
            converterMock.when(() -> Converter.stringToObject(payload, OAuth2TokenResponse.class)).thenReturn(null);
            assertThrows(IllegalStateException.class, () -> fetcher.fetch(5));
        }
    }

    @Test
    void fetchThrowsWhenAccessTokenIsBlank() {
        Http2Response response = Http2Response.builder()
                .status(200)
                .body("{\"access_token\":\"   \",\"expires_in\":60}".getBytes(StandardCharsets.UTF_8))
                .build();
        when(httpClient.send(any(Http2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        OAuth2ClientCredentialsFetcher fetcher = new OAuth2ClientCredentialsFetcher(
                httpClient, "/t", "id", "sec", null, "test-nfInstanceId", "SMSF", "AMF");

        assertThrows(IllegalStateException.class, () -> fetcher.fetch(5));
    }

    @Test
    void fetchOmitsBlankAndNullFieldsFromFormBody() throws Exception {
        Http2Response response = Http2Response.builder()
                .status(200)
                .body("{\"access_token\":\"t\",\"expires_in\":60}".getBytes(StandardCharsets.UTF_8))
                .build();
        ArgumentCaptor<Http2Request> captor = ArgumentCaptor.forClass(Http2Request.class);
        when(httpClient.send(captor.capture())).thenReturn(CompletableFuture.completedFuture(response));

        OAuth2ClientCredentialsFetcher fetcher = new OAuth2ClientCredentialsFetcher(
                httpClient, "/oauth2/token", "", "   ", null, "nf-1", "SMSF", "");
        fetcher.fetch(5);

        String body = new String(captor.getValue().getBody(), StandardCharsets.UTF_8);
        assertTrue(body.contains("grant_type=client_credentials"));
        assertTrue(body.contains("nfInstanceId=nf-1"));
        assertTrue(body.contains("nfType=SMSF"));
        assertTrue(!body.contains("scope="));
        assertTrue(!body.contains("client_id="));
        assertTrue(!body.contains("client_secret="));
        assertTrue(!body.contains("targetNfType="));
    }
}
