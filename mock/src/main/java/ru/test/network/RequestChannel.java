package ru.test.network;

import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestAndSize;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;

import java.lang.management.MemoryPoolMXBean;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class RequestChannel {

    private final int queueSize;
    ArrayBlockingQueue<BaseRequest> callbackQueue;
    ArrayBlockingQueue<BaseRequest> requestQueue;

    public RequestChannel(int queueSize) {
        this.queueSize = queueSize;
        callbackQueue = new ArrayBlockingQueue<>(queueSize);
        requestQueue = new ArrayBlockingQueue<>(queueSize);
    }

    public BaseRequest receiveRequest(Long timeout) {
        BaseRequest callbackRequest = callbackQueue.poll();
        if (callbackRequest != null) {
            return callbackRequest;
        } else {
            BaseRequest request = null;
            try {
                request = requestQueue.poll(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {

            }
            return request;
        }

    }

    public interface BaseRequest {
    }

    public static class Request {

        volatile Long requestDequeueTimeNanos = -1L;
        volatile Long apiLocalCompleteTimeNanos = -1L;
        volatile Long responseCompleteTimeNanos = -1L;
        volatile Long responseDequeueTimeNanos = -1L;
        volatile Long messageConversionsTimeNanos = 0L;
        volatile Long apiThrottleTimeMs = 0L;
        volatile Long temporaryMemoryBytes = 0L;
//    volatile var recordNetworkThreadTimeCallback: Option[Long => Unit] = None
//        volatile var callbackRequestDequeueTimeNanos: Option[Long] = None
//        volatile var callbackRequestCompleteTimeNanos: Option[Long] = None

        Optional<Request> envelope = null;
        volatile ByteBuffer buffer;
        RequestContext context;
        RequestAndSize bodyAndSize;

        public Request(RequestContext context, ByteBuffer buffer) {
            this.context = context;
            this.buffer = buffer;

            bodyAndSize = context.parseRequest(buffer);
        }

        public RequestHeader header() {
            return context.header;
        }

        public AbstractRequest body() {
            if (bodyAndSize.request != null) {
                return bodyAndSize.request;
            } else {
                throw new RuntimeException();
            }
        }

        public void releaseBuffer() {
            if (envelope.isPresent()) {
                if (buffer != null) {

                }
            } else {
                envelope.get().releaseBuffer();
            }
        }
    }
}
