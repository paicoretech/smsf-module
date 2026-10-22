package com.paicbd.smsc.smsf.mt;

import com.fasterxml.jackson.core.type.TypeReference;
import com.paicbd.smsc.dto.ErrorCodeMapping;
import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.exception.RTException;
import com.paicbd.smsc.kafka.KafkaConsumerFactory;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenCache;
import com.paicbd.smsc.smsf.mt.config.AppProperties;
import com.paicbd.smsc.smsf.mt.stack.MtStackManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.RedisManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import static com.paicbd.smsc.smsf.common.constants.Constants.ENABLED;

/**
 * Multi-tenant lifecycle for the MT module: reads the same {@code Http2Gateway} registry from Redis
 * as the other two modules and keeps one {@link MtStackManager} per enabled gateway.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ConnectionManager {

    private final RedisManager redisManager;
    private final ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingConcurrentHashMap;
    private final ConcurrentMap<Integer, MtStackManager> gatewayStacks;
    private final AppProperties appProperties;
    private final KafkaConsumerFactory kafkaConsumerFactory;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ScyllaManager scyllaManager;
    private final SmsfTokenCache tokenCache;

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
        MtStackManager existing = gatewayStacks.get(networkId);
        if (Objects.nonNull(existing) && existing.isStarted()) {
            log.warn("HTTP/2 Gateway {} MT stack is already running — skipping start", networkId);
            return;
        }
        if (Objects.nonNull(existing)) {
            existing.stop();
            gatewayStacks.remove(networkId);
        }
        MtStackManager stack = new MtStackManager(http2Gateway, appProperties, kafkaConsumerFactory,
                kafkaTemplate, scyllaManager, tokenCache, errorCodeMappingConcurrentHashMap);
        try {
            stack.start();
            gatewayStacks.put(networkId, stack);
            log.info("HTTP/2 Gateway {} MT stack started", networkId);
        } catch (Exception ex) {
            log.error("Failed to start the MT stack for HTTP/2 Gateway {}: {}", networkId, ex.getMessage(), ex);
        }
    }

    private void stopGatewayStack(int networkId) {
        MtStackManager stack = gatewayStacks.remove(networkId);
        if (Objects.isNull(stack)) {
            log.warn("HTTP/2 Gateway {} MT stack not found — nothing to stop", networkId);
            return;
        }
        try {
            stack.stop();
            log.info("HTTP/2 Gateway {} MT stack stopped", networkId);
        } catch (Exception ex) {
            log.error("Failed to stop the MT stack for HTTP/2 Gateway {}: {}", networkId, ex.getMessage(), ex);
        }
    }

    private void startStack() {
        log.info("Starting SMSF MT stack");
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
        log.info("Stopping SMSF MT stack ({} gateway(s))", gatewayStacks.size());
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
