package com.paicbd.smsc.smsf.common.redis;

/**
 * Identifies a cached credential. A token is bound to the audience it was minted for, so the
 * gateway alone is not enough: the same {@code networkId} holds one entry for the AMF and another
 * for the UDM.
 */
public record TokenKey(int networkId, String targetNfType) {

    @Override
    public String toString() {
        return networkId + ":" + targetNfType;
    }
}
