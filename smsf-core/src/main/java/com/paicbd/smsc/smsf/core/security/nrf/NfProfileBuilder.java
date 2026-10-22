package com.paicbd.smsc.smsf.core.security.nrf;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.smsf.core.config.AppProperties;
import com.paicore.sba.stack.openapi.model.IpEndPoint;
import com.paicore.sba.stack.openapi.model.NFProfile;
import com.paicore.sba.stack.openapi.model.NFService;
import com.paicore.sba.stack.openapi.model.NFServiceStatus;
import com.paicore.sba.stack.openapi.model.NFServiceVersion;
import com.paicore.sba.stack.openapi.model.NFStatus;
import com.paicore.sba.stack.openapi.model.NFType;
import com.paicore.sba.stack.openapi.model.ServiceName;
import com.paicore.sba.stack.openapi.model.TransportProtocol;
import com.paicore.sba.stack.openapi.model.UriScheme;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class NfProfileBuilder {

    private NfProfileBuilder() {
    }

    /**
     * @throws IllegalStateException when the gateway carries no {@code nf_instance_id}. The caller
     *                               must not start the stack: an SMSF without its assigned identity
     *                               cannot register on the NRF nor request a token.
     */
    public static UUID requireNfInstanceId(Http2Gateway gateway) {
        UUID nfInstanceId = gateway.getNfInstanceId();
        if (Objects.isNull(nfInstanceId)) {
            throw new IllegalStateException(String.format(
                    "Gateway %d has no nf_instance_id. It is assigned by smsc-management-be and read "
                            + "from Redis; the SMSF never generates one.", gateway.getNetworkId()));
        }
        return nfInstanceId;
    }

    public static NFProfile build(Http2Gateway gateway, AppProperties props) {
        // Never generated here. The nfInstanceId is assigned by smsc-management-be and published to
        // Redis; minting one locally would register the SMSF under an identity the platform does not
        // know, and a fresh one on every restart would orphan the previous profile in the NRF.
        UUID nfInstanceId = requireNfInstanceId(gateway);

        NFProfile profile = new NFProfile();
        profile.setNfInstanceId(nfInstanceId);
        profile.setNfType(NFType.SMSF);
        profile.setNfStatus(NFStatus.REGISTERED);
        profile.setHeartBeatTimer(props.getNrfHeartbeatDefaultSeconds());
        profile.setIpv4Addresses(List.of(props.getSmsfSbiPublicAddress()));

        NFService service = buildService(gateway, props, nfInstanceId);
        profile.setNfServiceList(Map.of(service.getServiceInstanceId(), service));
        return profile;
    }

    private static NFService buildService(Http2Gateway gateway, AppProperties props, UUID nfInstanceId) {
        String basePath = props.getSmsfSbiServerBasePath();
        NFService service = new NFService();
        service.setServiceInstanceId(nfInstanceId + "-nsmsf-sms");
        service.setServiceName(ServiceName.NSMSF_SMS);
        service.setVersions(List.of(version(basePath, props)));
        service.setScheme(UriScheme.HTTPS);
        service.setNfServiceStatus(NFServiceStatus.REGISTERED);
        service.setIpEndPoints(List.of(ipEndPoint(props)));
        service.setApiPrefix(basePath + "/" + gateway.getNetworkId());
        return service;
    }

    private static NFServiceVersion version(String basePath, AppProperties props) {
        NFServiceVersion v = new NFServiceVersion();
        v.setApiVersionInUri(basePath.substring(basePath.lastIndexOf('/') + 1));
        v.setApiFullVersion(props.getNrfApiFullVersion());
        return v;
    }

    private static IpEndPoint ipEndPoint(AppProperties props) {
        IpEndPoint endpoint = new IpEndPoint();
        endpoint.setIpv4Address(props.getSmsfSbiPublicAddress());
        endpoint.setPort(props.getServerPort());
        endpoint.setTransport(TransportProtocol.TCP);
        return endpoint;
    }

}
