package com.paicbd.smsc.smsf.common.constants;

import com.paicbd.smsc.utils.Generated;

@Generated
public class Constants {
    private Constants() {
        throw new IllegalStateException("Utility Class");
    }

    public static final int ENABLED = 1;

    public static final String MSISDN_PREFIX = "msisdn-";
    public static final String SUPI_PLACEHOLDER = "{supi}";
    public static final String UE_CONTEXTS_PATH = "/ue-contexts";
    public static final String SENDSMS_SUFFIX = "/sendsms";
    public static final String NAMF_CALLBACK_PATH = "/namf-cb";
    public static final String N1N2_FAILURE_SUFFIX = "/n1n2-failure";

    public static final String CAUSE_INVALID_SUPI = "INVALID_SUPI";
    public static final String CAUSE_CONTEXT_NOT_FOUND = "CONTEXT_NOT_FOUND";
    public static final String CAUSE_USER_NOT_FOUND = "USER_NOT_FOUND";

    public static final String ORIGIN_PROTOCOL_HTTP2 = "HTTP2";
    public static final String NETWORK_TYPE_GATEWAY = "GW";
    public static final String NETWORK_TYPE_SERVICE_PROVIDER = "SP";
    public static final String HTTP2_MESSAGE_REFERENCE = "HTTP2_MESSAGE_REFERENCE";
    public static final int ESM_CLASS_UDH_PRESENT = 64;

    public static final String NF_TYPE_SMSF = "SMSF";
    public static final String NF_TYPE_AMF = "AMF";
    public static final String NF_TYPE_UDM = "UDM";

    /** Nudm_UECM service name, used both for UDM discovery and for the N21 access-token scope. */
    public static final String SERVICE_NAME_NUDM_UECM = "nudm-uecm";

    public static final String CONTENT_TYPE_HEADER = "content-type";
    public static final String AUTHORIZATION_HEADER = "authorization";
    public static final int DEFAULT_NRF_PORT = 8443;
    public static final int DEFAULT_TOKEN_PORT = 8443;
    public static final String FORM_URLENCODED_CONTENT_TYPE = "application/x-www-form-urlencoded";
    public static final String MULTIPART_RELATED_CONTENT_TYPE = "multipart/related";
    public static final String PROBLEM_JSON_CONTENT_TYPE = "application/problem+json";
    public static final String JSON_CONTENT_TYPE = "application/json";
    public static final String JSON_PATCH_CONTENT_TYPE = "application/json-patch+json";

    public static final String RESPONSE_HTTP2_CLIENT_ENDPOINT = "/app/response-http2-client";
}
