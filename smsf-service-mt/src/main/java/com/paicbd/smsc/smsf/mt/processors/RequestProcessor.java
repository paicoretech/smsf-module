package com.paicbd.smsc.smsf.mt.processors;

import com.paicbd.smsc.dto.MessageEvent;
import com.paicore.sba.stack.transport.http2.dto.Http2Request;
import com.paicore.sba.stack.transport.http2.dto.Http2Response;

public interface RequestProcessor {

    void processIncomingRequest(Http2Request request);

    void processOutgoingRequest(MessageEvent event);

    void processResponse(Http2Response response, MessageEvent originalEvent);

    void processError(Throwable error, MessageEvent originalEvent);

    default void stopWatchers() {
        // no-op by default; implementations owning throughput watchers override this
    }
}
