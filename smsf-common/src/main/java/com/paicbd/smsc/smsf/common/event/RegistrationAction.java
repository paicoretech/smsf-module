package com.paicbd.smsc.smsf.common.event;

/**
 * Action carried by a {@link SmsfRegistrationEvent}: whether the SMSF must register the UE against
 * the UDM over N21, or deregister it.
 */
public enum RegistrationAction {
    REGISTER,
    DEREGISTER
}
