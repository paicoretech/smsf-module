package com.paicbd.smsc.smsf.common.http2;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class Http2HostUtilsTest {

    private static final int DEFAULT_PORT = 8443;

    @ParameterizedTest
    @DisplayName("parseHostPort, when input is null or blank, then throws IllegalArgumentException")
    @NullAndEmptySource
    @ValueSource(strings = {"   ", "\t"})
    void parseHostPortWhenNullOrBlankThenThrows(String raw) {
        assertThrows(IllegalArgumentException.class, () -> Http2HostUtils.parseHostPort(raw, DEFAULT_PORT));
    }

    @Test
    @DisplayName("parsePath, when host is an absolute URL, then returns its path keeping the supi placeholder")
    void parsePathWhenAbsoluteUrlThenReturnsPath() {
        assertEquals("/nsmsf-sms/v2/ue-contexts/{supi}/sendsms",
                Http2HostUtils.parsePath("http://127.0.0.1/nsmsf-sms/v2/ue-contexts/{supi}/sendsms"));
    }

    @Test
    @DisplayName("parsePath, when host is a URL without a path, then returns an empty string")
    void parsePathWhenUrlWithoutPathThenEmpty() {
        assertEquals("", Http2HostUtils.parsePath("http://127.0.0.1:8443"));
    }

    @Test
    @DisplayName("parsePath, when host is not a URL, then returns the raw value")
    void parsePathWhenNotUrlThenReturnsRaw() {
        assertEquals("smsf.local:8443", Http2HostUtils.parsePath("smsf.local:8443"));
    }

    @ParameterizedTest
    @DisplayName("parsePath, when input is null or blank, then throws IllegalArgumentException")
    @NullAndEmptySource
    @ValueSource(strings = {"   ", "\t"})
    void parsePathWhenNullOrBlankThenThrows(String raw) {
        assertThrows(IllegalArgumentException.class, () -> Http2HostUtils.parsePath(raw));
    }

    @Test
    @DisplayName("parseHostPort, when scheme is https and no explicit port, then port is 443")
    void parseHostPortWhenHttpsSchemeWithoutPortThen443() {
        Http2HostUtils.HostPort hp = Http2HostUtils.parseHostPort("https://nrf.example.com/oauth/token", DEFAULT_PORT);
        assertEquals("nrf.example.com", hp.host());
        assertEquals(443, hp.port());
    }

    @Test
    @DisplayName("parseHostPort, when scheme is http and no explicit port, then port is 80")
    void parseHostPortWhenHttpSchemeWithoutPortThen80() {
        Http2HostUtils.HostPort hp = Http2HostUtils.parseHostPort("http://nrf.example.com", DEFAULT_PORT);
        assertEquals("nrf.example.com", hp.host());
        assertEquals(80, hp.port());
    }

    @Test
    @DisplayName("parseHostPort, when URI has explicit port, then uses that port")
    void parseHostPortWhenUriHasExplicitPortThenUsesIt() {
        Http2HostUtils.HostPort hp = Http2HostUtils.parseHostPort("https://nrf.example.com:9443/path", DEFAULT_PORT);
        assertEquals("nrf.example.com", hp.host());
        assertEquals(9443, hp.port());
    }

    @Test
    @DisplayName("parseHostPort, when URL path contains templated placeholder like {supi}, then still extracts host and port")
    void parseHostPortWhenUrlPathHasSupiPlaceholderThenExtractsHostAndPort() {
        Http2HostUtils.HostPort hp = Http2HostUtils.parseHostPort(
                "http://192.168.1.112:9095/namf-comm/v1/ue-contexts/{supi}/n1-n2-messages", DEFAULT_PORT);
        assertEquals("192.168.1.112", hp.host());
        assertEquals(9095, hp.port());
    }

    @Test
    @DisplayName("parseHostPort, when URL authority is malformed, then falls back to the raw value and default port")
    void parseHostPortWhenUrlAuthorityIsMalformedThenFallsBack() {
        Http2HostUtils.HostPort hp = Http2HostUtils.parseHostPort("http://bad host/path", DEFAULT_PORT);
        assertEquals("http://bad host/path", hp.host());
        assertEquals(DEFAULT_PORT, hp.port());
    }

    @Test
    @DisplayName("parseHostPort, when host:port pair provided, then parses both")
    void parseHostPortWhenHostPortPairThenParsesBoth() {
        Http2HostUtils.HostPort hp = Http2HostUtils.parseHostPort("smsf.local:8080", DEFAULT_PORT);
        assertEquals("smsf.local", hp.host());
        assertEquals(8080, hp.port());
    }

    @Test
    @DisplayName("parseHostPort, when input is trimmed, then leading/trailing spaces are ignored")
    void parseHostPortWhenInputIsTrimmedThenSpacesIgnored() {
        Http2HostUtils.HostPort hp = Http2HostUtils.parseHostPort("  smsf.local:8443   ", DEFAULT_PORT);
        assertEquals("smsf.local", hp.host());
        assertEquals(8443, hp.port());
    }

    @ParameterizedTest(name = "[{index}] raw=\"{0}\" -> host=\"{1}\", port=DEFAULT")
    @DisplayName("parseHostPort, when no explicit port can be derived, then keeps host and uses default port")
    @CsvSource(textBlock = """
            ftp://nrf.example.com,    nrf.example.com
            smsf.local,               smsf.local
            smsf.local:abc,           smsf.local:abc
            https://,                 https://
            """)
    void parseHostPortWhenNoPortDerivableThenDefaultPort(String raw, String expectedHost) {
        Http2HostUtils.HostPort hp = Http2HostUtils.parseHostPort(raw, DEFAULT_PORT);
        assertEquals(expectedHost, hp.host());
        assertEquals(DEFAULT_PORT, hp.port());
    }

    @Test
    @DisplayName("HostPort, when constructed, then exposes host and port")
    void hostPortRecordWhenConstructedThenExposesValues() {
        Http2HostUtils.HostPort hp = new Http2HostUtils.HostPort("h", 1234);
        assertEquals("h", hp.host());
        assertEquals(1234, hp.port());
    }

    @Test
    @DisplayName("Http2HostUtils, when constructor invoked via reflection, then throws IllegalStateException")
    void utilityClassConstructorThenThrowsIllegalState() throws NoSuchMethodException {
        Constructor<Http2HostUtils> ctor = Http2HostUtils.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        InvocationTargetException ex = assertThrows(InvocationTargetException.class, ctor::newInstance);
        assertInstanceOf(IllegalStateException.class, ex.getCause());
    }

    @ParameterizedTest
    @CsvSource({
            "https://nrf.example.com/oauth/token?grant_type=client_credentials, https://nrf.example.com",
            "https://nrf.example.com/oauth/token#section, https://nrf.example.com",
            "smsf.local:8443, smsf.local:8443"
    })
    @DisplayName("stripPathAndQuery, when called reflectively, then trims path/query/hash and keeps non URL values unchanged")
    void stripPathAndQueryWhenCalledReflectivelyThenReturnsExpectedValue(String raw, String expected)
            throws ReflectiveOperationException {
        Method method = Http2HostUtils.class.getDeclaredMethod("stripPathAndQuery", String.class);
        method.setAccessible(true);

        String actual = (String) method.invoke(null, raw);

        assertEquals(expected, actual);
    }
}
