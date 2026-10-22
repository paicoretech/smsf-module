package com.paicbd.smsc.smsf.core.api.service;

import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.smsf.common.event.SmsfRegistrationEvent;
import com.paicbd.smsc.smsf.common.event.RegistrationAction;
import com.paicore.sba.stack.openapi.model.UeSmsContextData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;

@Slf4j
@RequiredArgsConstructor
@Service
public class RegisterService {

    private final ScyllaManager scyllaManager;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public UpsertResult upsertUeContext(UeSmsContextData body) {
        boolean existed = scyllaManager.existsSmsfUeSmsContextBySupi(body.getSupi());
        scyllaManager.upsertSmsfUeSmsContext(body);
        return new UpsertResult(existed ? Outcome.UPDATED : Outcome.CREATED, computeEtag(body));
    }

    /**
     * Removes the UE context.
     *
     * <p>Deleting a SUPI that is not stored still answers {@code 204}: an AMF retrying a DELETE it
     * never saw the response to must not start getting errors. It is reported as
     * {@link Outcome#NOT_FOUND} rather than {@link Outcome#DELETED} so the caller can tell the two
     * apart and skip the deregistration event — there is nothing registered at the UDM to remove,
     * and publishing on every retry would send the UDM a second DELETE.</p>
     *
     * <p>The existence check only costs a read when there is no {@code If-Match}, and it is the
     * cheapest one available: {@code SELECT supi ... WHERE supi = ?}, a partition-key lookup that
     * never touches the stored document. The conditional path pays nothing, since it already reads
     * the row to compare the ETag. It also avoids writing a tombstone for a row that was never
     * there.</p>
     */
    public DeleteResult deleteUeContext(String supi, String ifMatch) {
        if (Objects.nonNull(ifMatch) && !"*".equals(ifMatch.trim())) {
            UeSmsContextData current = scyllaManager.getSmsfUeSmsContextBySupi(supi);
            if (Objects.isNull(current)) {
                return new DeleteResult(Outcome.NOT_FOUND);
            }
            if (!ifMatch.trim().equals(computeEtag(current))) {
                return new DeleteResult(Outcome.ETAG_MISMATCH);
            }
        } else if (!scyllaManager.existsSmsfUeSmsContextBySupi(supi)) {
            return new DeleteResult(Outcome.NOT_FOUND);
        }
        scyllaManager.deleteSmsfUeSmsContextBySupi(supi);
        return new DeleteResult(Outcome.DELETED);
    }

    /**
     * Publishes an SMSF registration so {@code smsf-n21-registration} forwards it to the UDM.
     *
     * <p>The core no longer talks to the UDM: it answers the AMF as soon as the context is
     * persisted, and the forwarding happens independently off this event. Publishing is
     * best-effort in the same sense the previous in-process forwarding was — a failure is logged
     * and does not fail the API call, since the UE context is already stored.</p>
     */
    public void publishRegistration(int networkId, UeSmsContextData body) {
        publish(SmsfRegistrationEvent.builder()
                .networkId(networkId)
                .supi(body.getSupi())
                .action(RegistrationAction.REGISTER)
                .ueContext(body)
                .timestamp(Instant.now().toEpochMilli())
                .build());
    }

    /** Publishes an SMSF deregistration. Only the SUPI is needed to remove the context at the UDM. */
    public void publishDeregistration(int networkId, String supi) {
        publish(SmsfRegistrationEvent.builder()
                .networkId(networkId)
                .supi(supi)
                .action(RegistrationAction.DEREGISTER)
                .timestamp(Instant.now().toEpochMilli())
                .build());
    }

    /**
     * Sends the event keyed by SUPI, so a REGISTER and a later DEREGISTER for the same subscriber
     * share a partition and cannot be delivered out of order.
     */
    private void publish(SmsfRegistrationEvent event) {
        String topic = event.getNetworkId() + KafkaTopicsConstants.SMSF_UDM_REGISTER_TOPIC_SUFFIX;
        try {
            kafkaTemplate.send(topic, event.getSupi(), Converter.valueAsString(event));
            log.debug("Registration event published topic={} action={} supi={}",
                    topic, event.getAction(), event.getSupi());
        } catch (Exception ex) {
            log.error("Could not publish the registration event topic={} action={} supi={}: {}",
                    topic, event.getAction(), event.getSupi(), ex.getMessage(), ex);
        }
    }

    private static String computeEtag(UeSmsContextData body) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(Converter.valueAsString(body).getBytes(StandardCharsets.UTF_8));
            return "\"" + HexFormat.of().formatHex(md.digest()).substring(0, 16) + "\"";
        } catch (NoSuchAlgorithmException ex) {
            return "\"" + Long.toHexString(Objects.hashCode(body)) + "\"";
        }
    }

    public enum Outcome { CREATED, UPDATED, DELETED, NOT_FOUND, ETAG_MISMATCH }

    public record UpsertResult(Outcome outcome, String etag) {}

    public record DeleteResult(Outcome outcome) {}
}
