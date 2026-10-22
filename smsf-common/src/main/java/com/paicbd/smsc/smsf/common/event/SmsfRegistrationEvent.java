package com.paicbd.smsc.smsf.common.event;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.paicbd.smsc.utils.Converter;
import com.paicore.sba.stack.openapi.model.UeSmsContextData;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Generated;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Registration event published by {@code smsf-core} once the UE SMS context has been persisted, and
 * consumed by {@code smsf-n21-registration} to forward it to the UDM.
 *
 * <p>It lives here rather than in the 5g-sba-stack library on purpose. This is not a 3GPP structure:
 * it is the internal Kafka contract of <em>this</em> SMSF, and it exists only because this product
 * splits the SBI layer and the N21 forwarding into two processes — {@code networkId} is an SMSC
 * gateway identifier, not a 5G core concept. Keeping it out of the library means the library carries
 * no trace of how this particular SMSF is deployed, and stays usable by any other implementation.</p>
 *
 * <p>It is published to {@code <networkId>.smsf.udm.register} keyed by {@code supi}, so the REGISTER
 * and DEREGISTER of the same subscriber stay on the same partition and cannot be reordered.</p>
 *
 * <p>{@code ueContext} is only carried on a {@link RegistrationAction#REGISTER}; a deregistration
 * needs nothing beyond the SUPI.</p>
 */
@Getter
@Setter
@Generated
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SmsfRegistrationEvent {

    private int networkId;
    private String supi;
    private RegistrationAction action;
    private UeSmsContextData ueContext;
    private long timestamp;

    @Override
    public String toString() {
        return Converter.valueAsString(this);
    }
}
