package ru.test.server;

import kafka.network.DataPlaneAcceptor;
import kafka.network.RequestChannel;
import kafka.network.SocketServer;
import kafka.security.CredentialProvider;
import kafka.server.*;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.Time;
import scala.collection.mutable.HashMap;

public class KafkaServer {

    private final RequestChannel requestChannel;
    private final KafkaConfig config;
    private final Metrics metrics;
    private final Time time;
    private final CredentialProvider credentialProvider;

    public KafkaServer(KafkaConfig config, Time time) {
        this.config = config;
        this.time = time;

        metrics = Server.initializeMetrics(config, time, "1test");
        requestChannel = new RequestChannel(
                4,
                "",
                Time.SYSTEM,
                new kafka.network.RequestChannel.Metrics(ApiMessageType.ListenerType.BROKER));

        DelegationTokenCache tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames());
        credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames(), tokenCache);
    }


    public void startup() {

        ApiVersionManager apiVersionManager = ApiVersionManager.apply(
                ApiMessageType.ListenerType.BROKER,
                config,
                null,
                BrokerFeatures.createEmpty(),
                null,
                null);

        SocketServer socketServer = new SocketServer(config, metrics, time, credentialProvider, apiVersionManager);
        RequestChannel requestChannel1 = socketServer.dataPlaneRequestChannel();

        new Thread(new KafkaRequestHandler(requestChannel1, new KafkaApis(requestChannel1))).start();

        socketServer.enableRequestProcessing(new HashMap<>());
//        new KafkaRequestHandlerPool(
//                1,
//                requestChannel,
//                new KafkaApis(requestChannel),
//                Time.SYSTEM,
//                1,
//                "${DataPlaneAcceptor.MetricPrefix}RequestHandlerAvgIdlePercent",
//                "data-plane",
//                "broker");
    }
}
