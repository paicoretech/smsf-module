package com.paicbd.smsc.smsf.n21;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenCache;
import com.paicbd.smsc.smsf.n21.config.AppProperties;
import com.paicbd.smsc.smsf.n21.stack.N21StackManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.RedisManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import static com.paicbd.smsc.smsf.common.constants.Constants.ENABLED;

/**
 * Multi-tenant lifecycle for the MT module: reads the same {@code Http2Gateway} registry from Redis
 * as the other two modules and keeps one {@link N21StackManager} per enabled gateway.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ConnectionManager {

    private final RedisManager redisManager;
    private final ConcurrentMap<Integer, N21StackManager> gatewayStacks;
    private final AppProperties appProperties;
    private final SmsfTokenCache tokenCache;
    private final ConsumerFactory<String, String> consumerFactory;

    @PostConstruct
    public void init() {
        this.startStack();
    }

    public synchronized void updateHttp2Gateway(String networkIdInRaw) throws NumberFormatException {
        int networkId = Integer.parseInt(networkIdInRaw);
        Http2Gateway http2Gateway = getHttp2GatewayFromRedis(networkIdInRaw);
        if (Objects.isNull(http2Gateway)) {
            log.warn("HTTP/2 gateway {} not found in Redis — evicting", networkId);
            stopGatewayStack(networkId);
            return;
        }
        if (http2Gateway.getEnabled() == ENABLED) {
            startGatewayStack(networkId, http2Gateway);
        } else {
            stopGatewayStack(networkId);
        }
    }

    private void startGatewayStack(int networkId, Http2Gateway http2Gateway) {
        N21StackManager existing = gatewayStacks.get(networkId);
        if (Objects.nonNull(existing) && existing.isStarted()) {
            log.warn("HTTP/2 Gateway {} N21 stack is already running — skipping start", networkId);
            return;
        }
        if (Objects.nonNull(existing)) {
            existing.stop();
            gatewayStacks.remove(networkId);
        }
        N21StackManager stack = new N21StackManager(http2Gateway, appProperties, tokenCache, consumerFactory);
        try {
            stack.start();
            gatewayStacks.put(networkId, stack);
            log.info("HTTP/2 Gateway {} N21 stack started", networkId);
        } catch (Exception ex) {
            log.error("Failed to start the N21 stack for HTTP/2 Gateway {}: {}", networkId, ex.getMessage(), ex);
        }
    }

    private void stopGatewayStack(int networkId) {
        N21StackManager stack = gatewayStacks.remove(networkId);
        if (Objects.isNull(stack)) {
            log.warn("HTTP/2 Gateway {} N21 stack not found — nothing to stop", networkId);
            return;
        }
        try {
            stack.stop();
            log.info("HTTP/2 Gateway {} N21 stack stopped", networkId);
        } catch (Exception ex) {
            log.error("Failed to stop the N21 stack for HTTP/2 Gateway {}: {}", networkId, ex.getMessage(), ex);
        }
    }

    private void startStack() {
        log.info("Starting SMSF N21 stack");
        Map<String, String> gateways = redisManager.hgetAll(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME);
        if (Objects.isNull(gateways) || gateways.isEmpty()) {
            log.info("No HTTP/2 gateways found on Redis");
            return;
        }
        gateways.forEach((networkIdRaw, gatewayRaw) -> {
            try {
                Http2Gateway gateway = Converter.stringToObject(gatewayRaw, Http2Gateway.class);
                if (Objects.isNull(gateway) || gateway.getEnabled() != ENABLED) {
                    return;
                }
                startGatewayStack(gateway.getNetworkId(), gateway);
            } catch (Exception ex) {
                log.error("Error starting HTTP/2 gateway {}: {}", networkIdRaw, ex.getMessage(), ex);
            }
        });
    }

    private void stopStack() {
        log.info("Stopping SMSF N21 stack ({} gateway(s))", gatewayStacks.size());
        gatewayStacks.forEach((networkId, stack) -> {
            try {
                stack.stop();
            } catch (Exception ex) {
                log.error("Error stopping HTTP/2 gateway {}: {}", networkId, ex.getMessage(), ex);
            }
        });
        gatewayStacks.clear();
    }

    private Http2Gateway getHttp2GatewayFromRedis(String networkIdInRaw) {
        String gatewayInRaw = redisManager.hget(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME, networkIdInRaw);
        if (Objects.isNull(gatewayInRaw)) {
            return null;
        }
        // Converter returns null on a parse failure, so a malformed entry reads as an absent gateway.
        return Converter.stringToObject(gatewayInRaw, Http2Gateway.class);
    }

    @PreDestroy
    public void destroy() {
        log.info("Destroying ConnectionManager Component");
        this.stopStack();
    }
}
