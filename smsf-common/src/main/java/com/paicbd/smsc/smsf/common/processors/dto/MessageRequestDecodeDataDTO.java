package com.paicbd.smsc.smsf.common.processors.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.paicbd.smsc.utils.Converter;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.restcomm.protocols.ss7.map.api.smstpdu.AddressField;
import org.restcomm.protocols.ss7.map.api.smstpdu.SmsSubmitTpdu;
import org.restcomm.protocols.ss7.map.api.smstpdu.SmsTpduType;
import org.restcomm.protocols.ss7.map.api.smstpdu.UserData;
import org.restcomm.protocols.ss7.map.api.smstpdu.ValidityPeriod;
import org.restcomm.protocols.ss7.map.api.smstpdu.ValidityPeriodFormat;

/**
 * Internal representation of a decoded Mobile Originated (MO) SMS, built from
 * the {@code SmsRecordData} metadata and the decoded 3GPP TS23.040 TPDU.
 *
 * <p>This DTO is the output of {@code MessageTpduProcessor} and isolates the
 * telecom protocol details (handled by the {@code 5g-sba-stack} codec) from the
 * downstream business processing pipeline.
 */
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MessageRequestDecodeDataDTO {
    private String sourceAddr;
    private AddressField addressField;
    private int dataCoding;
    private int protocolId;
    private byte[] encodedData;
    private boolean udhPresent;
    private int networkId;
    private String smsRecordId;
    private SmsTpduType messageType;
    private ValidityPeriod validityPeriod;
    private ValidityPeriodFormat validityPeriodFormat;

    @JsonIgnore
    private UserData userData;

    @JsonIgnore
    private SmsSubmitTpdu smsSubmitTpdu;

    private int messageReference;
    private Integer tpStatus;

    @Override
    public String toString() {
        return Converter.valueAsString(this);
    }
}
