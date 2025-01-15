package ru.test.server;

import kafka.server.ApiRequestHandler;

import java.util.concurrent.CountDownLatch;

public class KafkaRequestHandler implements Runnable {

    ThreadLocal<kafka.network.RequestChannel> threadRequestChannel = new ThreadLocal<>();
    ThreadLocal<kafka.network.RequestChannel.Request> threadCurrentChannel = new ThreadLocal<>();
    kafka.network.RequestChannel requestChannel;
    kafka.server.ApiRequestHandler apis;
    private final kafka.server.RequestLocal requestLocal = null;
    private CountDownLatch shutdownComplete = new CountDownLatch(1);

    volatile private boolean stopped = false;

    public KafkaRequestHandler(kafka.network.RequestChannel requestChannel, kafka.server.ApiRequestHandler apis) {
        this.requestChannel = requestChannel;
        this.apis = apis;
    }

    @Override
    public void run() {
        threadRequestChannel.set(requestChannel);

        while (!stopped) {
            kafka.network.RequestChannel.BaseRequest req = requestChannel.receiveRequest(300L);

            if (req instanceof kafka.network.RequestChannel.Request request) {
                try {
                    threadCurrentChannel.set(request);
                    apis.handle(request, requestLocal);
                } catch (Exception e) {
                    completeShutdown();
                } finally {
                    threadCurrentChannel.remove();
                    request.releaseBuffer();
                }
            }
        }

        completeShutdown();

    }

    private void completeShutdown() {
        requestLocal.close();
        threadRequestChannel.remove();
        shutdownComplete.countDown();
    }

    public interface ApiRequestHandler {
        void handle(kafka.network.RequestChannel.Request request, RequestLocal requestLocal);

        void tryCompleteActions();
    }
}
