package com.paicbd.smsc.smsf.core.security;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatNoException;

class SecurityMetricsTest {

    @Test
    @DisplayName("start/stop lifecycle creates and releases the watchers without exceptions")
    void lifecycleStartsAndStops() {
        SecurityMetrics metrics = new SecurityMetrics();

        assertThatNoException().isThrownBy(metrics::start);
        assertThatNoException().isThrownBy(metrics::stop);
    }

    @Test
    @DisplayName("stop is safe before start when watchers have not been initialised")
    void stopBeforeStart() {
        SecurityMetrics metrics = new SecurityMetrics();

        assertThatCode(metrics::stop).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("increment counters are safe before the lifecycle starts")
    void incrementsAreSafeBeforeStart() {
        SecurityMetrics metrics = new SecurityMetrics();

        assertThatCode(() -> {
            metrics.incrementAuthSuccess();
            metrics.incrementAuthSuccess();
            metrics.incrementAuthFailure();
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("increment counters update the underlying counters while the watchers run")
    void incrementsAfterStart() throws Exception {
        SecurityMetrics metrics = new SecurityMetrics();
        metrics.start();
        try {
            for (int i = 0; i < 5; i++) {
                metrics.incrementAuthSuccess();
            }
            for (int i = 0; i < 3; i++) {
                metrics.incrementAuthFailure();
            }

            assertThat(counterValue(metrics, "authSuccessCounter")).isEqualTo(5);
            assertThat(counterValue(metrics, "authFailureCounter")).isEqualTo(3);
        } finally {
            metrics.stop();
        }
    }

    private static int counterValue(SecurityMetrics metrics, String fieldName) throws Exception {
        Field field = SecurityMetrics.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return ((AtomicInteger) field.get(metrics)).get();
    }
}
