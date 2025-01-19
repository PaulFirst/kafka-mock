package ru.test.server;

import kafka.network.RequestChannel;
import kafka.network.SocketServer;
import kafka.security.CredentialProvider;
import kafka.server.ApiVersionManager;
import kafka.server.BrokerFeatures;
import kafka.server.KafkaConfig;
import kafka.server.Server;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.Time;
import scala.collection.mutable.HashMap;

public class KafkaServer {

    private final KafkaConfig config;
    private final Metrics metrics;
    private final Time time;
    private final CredentialProvider credentialProvider;

    public KafkaServer(KafkaConfig config, Time time) {
        this.config = config;
        this.time = time;

        metrics = Server.initializeMetrics(config, time, "1test");
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
        RequestChannel requestChannel = socketServer.dataPlaneRequestChannel();

        new Thread(new KafkaRequestHandler(requestChannel, new KafkaApis(requestChannel))).start();

        socketServer.enableRequestProcessing(new HashMap<>());
    }
}
