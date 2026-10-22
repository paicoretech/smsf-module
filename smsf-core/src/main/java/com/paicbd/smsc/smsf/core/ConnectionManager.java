package com.paicbd.smsc.smsf.core;

import com.fasterxml.jackson.core.type.TypeReference;
import com.paicbd.smsc.dto.ErrorCodeMapping;
import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.exception.RTException;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenRedisStore;
import com.paicbd.smsc.smsf.core.config.AppProperties;
import com.paicbd.smsc.smsf.core.security.GatewaySecurityLookup;
import com.paicbd.smsc.smsf.core.stack.CoreStackManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.RedisManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import static com.paicbd.smsc.smsf.common.constants.Constants.ENABLED;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConnectionManager {

    private final RedisManager redisManager;
    private final ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingConcurrentHashMap;
    private final ConcurrentMap<Integer, CoreStackManager> gatewayStacks;
    private final AppProperties appProperties;
    private final SmsfTokenRedisStore tokenStore;
    private final GatewaySecurityLookup gatewaySecurityLookup;

    @PostConstruct
    public void init() {
        this.startStack();
        this.loadErrorCodeMapping();
    }

    private void loadErrorCodeMapping() {
        var errorCodeMappingMap = this.redisManager.hgetAll(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME);
        if (errorCodeMappingMap.isEmpty()) {
            log.warn("No Error code mapping found");
            return;
        }
        errorCodeMappingMap.forEach((key, errorCodeMappingInRaw) -> {
            try {
                this.errorCodeMappingConcurrentHashMap.put(key, parseErrorCodeMapping(errorCodeMappingInRaw));
            } catch (RTException ex) {
                log.error("Error on load the error code mapping on method loadErrorCodeMapping {}", ex.getMessage(), ex);
            }
        });
        log.warn("{} Error code mapping load successfully", this.errorCodeMappingConcurrentHashMap.size());
    }

    public void updateErrorCodeMapping(String mnoId) {
        try {
            String errorCodeMappingInRaw = redisManager.hget(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME, mnoId);
            if (Objects.isNull(errorCodeMappingInRaw)) {
                errorCodeMappingConcurrentHashMap.remove(mnoId);
                return;
            }
            errorCodeMappingConcurrentHashMap.put(mnoId, parseErrorCodeMapping(errorCodeMappingInRaw));
        } catch (RTException ex) {
            log.error("Error on load the error code mapping on method updateErrorCodeMapping {}", ex.getMessage());
        }
    }

    public synchronized void updateHttp2Gateway(String networkIdInRaw) throws NumberFormatException {
        int networkId = Integer.parseInt(networkIdInRaw);
        Http2Gateway http2Gateway = getHttp2GatewayFromRedis(networkIdInRaw);
        if (Objects.isNull(http2Gateway)) {
            log.warn("HTTP/2 gateway {} not found in Redis — evicting", networkId);
            gatewaySecurityLookup.evict(networkId);
            stopGatewayStack(networkId);
            return;
        }
        gatewaySecurityLookup.register(http2Gateway);
        if (http2Gateway.getEnabled() == ENABLED) {
            startGatewayStack(networkId, http2Gateway);
        } else {
            stopGatewayStack(networkId);
        }
    }

    private void startGatewayStack(int networkId, Http2Gateway http2Gateway) {
        CoreStackManager existing = gatewayStacks.get(networkId);
        if (Objects.nonNull(existing) && existing.isStarted()) {
            log.warn("HTTP/2 Gateway {} stack is already running — skipping start", networkId);
            return;
        }
        if (Objects.nonNull(existing)) {
            existing.stop();
            gatewayStacks.remove(networkId);
        }
        CoreStackManager stack = new CoreStackManager(http2Gateway, appProperties, tokenStore,
                errorCodeMappingConcurrentHashMap);
        try {
            stack.start();
            gatewayStacks.put(networkId, stack);
            log.info("HTTP/2 Gateway {} started", networkId);
        } catch (Exception ex) {
            log.error("Failed to start HTTP/2 Gateway {}: {}", networkId, ex.getMessage(), ex);
        }
    }

    private void stopGatewayStack(int networkId) {
        CoreStackManager stack = gatewayStacks.remove(networkId);
        if (Objects.isNull(stack)) {
            log.warn("HTTP/2 Gateway {} stack not found — nothing to stop", networkId);
            return;
        }
        try {
            stack.stop();
            log.info("HTTP/2 Gateway {} stopped", networkId);
        } catch (Exception ex) {
            log.error("Failed to stop HTTP/2 Gateway {}: {}", networkId, ex.getMessage(), ex);
        }
    }

    private void startStack() {
        log.info("Starting SMSF stack");
        Map<String, String> gateways = redisManager.hgetAll(GeneralSmscConstants.HTTP2_GATEWAYS_HASH_NAME);
        if (Objects.isNull(gateways) || gateways.isEmpty()) {
            log.info("No HTTP/2 gateways found on Redis");
            return;
        }
        gateways.forEach((networkIdRaw, gatewayRaw) -> {
            try {
                Http2Gateway gateway = Converter.stringToObject(gatewayRaw, Http2Gateway.class);
                if (Objects.isNull(gateway)) {
                    return;
                }
                gatewaySecurityLookup.register(gateway);
                if (gateway.getEnabled() != ENABLED) {
                    return;
                }
                startGatewayStack(gateway.getNetworkId(), gateway);
            } catch (Exception ex) {
                log.error("Error starting HTTP/2 gateway {}: {}", networkIdRaw, ex.getMessage(), ex);
            }
        });
    }

    private void stopStack() {
        log.info("Stopping SMSF stack ({} gateway(s))", gatewayStacks.size());
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

    private static List<ErrorCodeMapping> parseErrorCodeMapping(String raw) {
        return Converter.stringToObject(raw, new TypeReference<>() {
        });
    }

    @PreDestroy
    public void destroy() {
        log.info("Destroying ConnectionManager Component");
        this.stopStack();
    }
}
