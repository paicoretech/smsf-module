package com.paicbd.smsc.smsf.common.event;

import com.paicbd.smsc.utils.Converter;
import com.paicore.sba.stack.openapi.model.UeSmsContextData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The wire contract between {@code smsf-core} and {@code smsf-n21-registration}. Both sides only
 * ever see this as a JSON string on a Kafka topic, so what matters is that it survives the round
 * trip — a rename or a type change on either side has to fail here, not in production.
 */
class SmsfRegistrationEventTest {

    private static final String SUPI = "imsi-460000000000066";

    @Test
    @DisplayName("a REGISTER survives the Kafka round trip with its UE context intact")
    void registerRoundTrip() {
        UeSmsContextData context = new UeSmsContextData();
        context.setSupi(SUPI);
        context.setGpsi("msisdn-8613915900066");

        SmsfRegistrationEvent sent = SmsfRegistrationEvent.builder()
                .networkId(66)
                .supi(SUPI)
                .action(RegistrationAction.REGISTER)
                .ueContext(context)
                .timestamp(1787291576426L)
                .build();

        SmsfRegistrationEvent received =
                Converter.stringToObject(Converter.valueAsString(sent), SmsfRegistrationEvent.class);

        assertNotNull(received);
        assertEquals(66, received.getNetworkId());
        assertEquals(SUPI, received.getSupi());
        assertEquals(RegistrationAction.REGISTER, received.getAction());
        assertEquals(1787291576426L, received.getTimestamp());
        assertNotNull(received.getUeContext());
        assertEquals(SUPI, received.getUeContext().getSupi());
        assertEquals("msisdn-8613915900066", received.getUeContext().getGpsi());
    }

    @Test
    @DisplayName("a DEREGISTER carries no ueContext, and the field is left out of the JSON entirely")
    void deregisterOmitsTheUeContext() {
        SmsfRegistrationEvent sent = SmsfRegistrationEvent.builder()
                .networkId(66)
                .supi(SUPI)
                .action(RegistrationAction.DEREGISTER)
                .timestamp(1787291576426L)
                .build();

        String json = Converter.valueAsString(sent);

        // NON_NULL: the UDM needs nothing but the SUPI, so the key must not travel as "ueContext":null
        assertFalse(json.contains("ueContext"), json);

        SmsfRegistrationEvent received = Converter.stringToObject(json, SmsfRegistrationEvent.class);
        assertNotNull(received);
        assertEquals(RegistrationAction.DEREGISTER, received.getAction());
        assertNull(received.getUeContext());
    }

    @ParameterizedTest
    @EnumSource(RegistrationAction.class)
    @DisplayName("every action is serialised by name, so the two modules can be deployed independently")
    void everyActionIsSerialisedByName(RegistrationAction action) {
        String json = Converter.valueAsString(
                SmsfRegistrationEvent.builder().supi(SUPI).action(action).build());

        assertTrue(json.contains("\"action\":\"" + action.name() + "\""), json);
        assertEquals(action, RegistrationAction.valueOf(action.name()));
    }

    @Test
    @DisplayName("toString renders the event as the JSON that actually travels")
    void toStringIsTheWireForm() {
        SmsfRegistrationEvent event = SmsfRegistrationEvent.builder()
                .networkId(66).supi(SUPI).action(RegistrationAction.DEREGISTER).build();

        assertEquals(Converter.valueAsString(event), event.toString());
    }
}
