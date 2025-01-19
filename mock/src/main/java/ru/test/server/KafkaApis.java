package ru.test.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.network.RequestChannel;
import kafka.server.ApiRequestHandler;
import kafka.server.BrokerFeatures;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.utils.Utils;
import scala.Option;
import scala.reflect.ClassTag;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class KafkaApis implements ApiRequestHandler {

    private final RequestChannel requestChannel;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaApis(RequestChannel requestChannel) {
        this.requestChannel = requestChannel;
    }


    @Override
    public void handle(RequestChannel.Request request, kafka.server.RequestLocal requestLocal) {
        try {
            switch (request.header().apiKey()) {
                case API_VERSIONS -> handleApiVersionsRequest(request);
                case METADATA -> handleTopicMetadataRequest(request);
                case INIT_PRODUCER_ID -> handleInitProducerIdRequest(request);
                case PRODUCE -> handleProduceRequest(request);

            }
        } catch (Exception e) {

        }
    }

    @Override
    public void tryCompleteActions() {

    }

    private void handleApiVersionsRequest(kafka.network.RequestChannel.Request request) {
        ApiVersionsRequest apiVersionRequest = request.body(ClassTag.apply(ApiVersionsRequest.class), null);
        if (apiVersionRequest.hasUnsupportedRequestVersion()) {

        } else if (!apiVersionRequest.isValid()) {

        } else {
            ApiVersionsResponseData.ApiVersion apiVersion = new ApiVersionsResponseData.ApiVersion();
            apiVersion.setApiKey((short) 18);
            apiVersion.setMaxVersion((short) 4);

            ApiVersionsResponseData.ApiVersion metadata = new ApiVersionsResponseData.ApiVersion();
            metadata.setApiKey((short) 3);
            metadata.setMaxVersion((short) 12);

            ApiVersionsResponseData.ApiVersion initProducerId = new ApiVersionsResponseData.ApiVersion();
            initProducerId.setApiKey((short) 22);
            initProducerId.setMaxVersion((short) 5);
            List<ApiVersionsResponseData.ApiVersion> apiVersions = new ArrayList<>();
            apiVersions.add(apiVersion);
            apiVersions.add(metadata);
            apiVersions.add(initProducerId);

            NodeApiVersions nodeApiVersions = new NodeApiVersions(apiVersions, new ArrayList<>(), false);
            ApiVersionsResponse apiVersionsResponse = ApiVersionsResponse.createApiVersionsResponse(0,
                    RecordVersion.V2,
                    BrokerFeatures.defaultSupportedFeatures(false),
                    new HashMap<>(),
                    1L,
                    nodeApiVersions,
                    ApiMessageType.ListenerType.BROKER,
                    false,
                    false,
                    false
            );

            System.out.println("Обработан apikey: API_VERSIONS");
            requestChannel.sendResponse(request, apiVersionsResponse, Option.empty());
        }
    }

    private void handleTopicMetadataRequest(kafka.network.RequestChannel.Request request) {
        MetadataRequest metadataRequest = request.body(ClassTag.apply(MetadataRequest.class), null);
        short requestVersion = request.header().apiVersion();

        MetadataResponseData.MetadataResponsePartition partition = new MetadataResponseData.MetadataResponsePartition();
        partition.setErrorCode((short)0);
        partition.setLeaderId(1);
        partition.setLeaderEpoch(8);
        partition.setReplicaNodes(List.of(1));
        partition.setIsrNodes(List.of(1));

        MetadataResponseData.MetadataResponseTopic topic = new MetadataResponseData.MetadataResponseTopic();
        topic.setErrorCode((short) 0);
        topic.setTopicId(Uuid.METADATA_TOPIC_ID);
        topic.setName("test-topic");
        topic.setIsInternal(false);
        topic.setTopicAuthorizedOperations(Integer.MIN_VALUE);
        topic.setPartitions(List.of(partition));

        MetadataResponse metadataResponse = MetadataResponse.prepareResponse(
                requestVersion,
                0,
                List.of(new Node(1, "127.0.0.1", 9092)),
                "1",
                0,
                List.of(topic),
                Integer.MIN_VALUE
        );

        System.out.println("Обработан apikey: METADATA");
        requestChannel.sendResponse(request, metadataResponse, Option.empty());
    }

    private void handleInitProducerIdRequest(kafka.network.RequestChannel.Request request) {
        InitProducerIdRequest initProducerIdRequest = request.body(ClassTag.apply(InitProducerIdRequest.class), null);
        String transactionalId = initProducerIdRequest.data().transactionalId();

        InitProducerIdResponseData responseData = new InitProducerIdResponseData()
                .setProducerId(0L)
                .setProducerEpoch((short) 1)
                .setThrottleTimeMs(0)
                .setErrorCode((short) 0);
        InitProducerIdResponse responseBody = new InitProducerIdResponse(responseData);

        System.out.println("Обработан apikey: INIT_PRODUCER_ID");
        requestChannel.sendResponse(request, responseBody, Option.empty());
    }

    private void handleProduceRequest(kafka.network.RequestChannel.Request request) {
        ProduceRequest produceRequest = request.body(ClassTag.apply(ProduceRequest.class), null);
        short acks = produceRequest.acks();
        ProduceRequestData.TopicProduceDataCollection topicProduceData = produceRequest.data().topicData();
        ProduceRequestData.PartitionProduceData first = topicProduceData.stream().findFirst().get().partitionData().stream().findFirst().get();
        MemoryRecords records = (MemoryRecords) first.records();
        Iterable<Record> records1 = records.records();
        Record next = records1.iterator().next();
        ByteBuffer value = next.value();
        String s = new String(Utils.toNullableArray(value), StandardCharsets.UTF_8);

        ProduceResponseData.PartitionProduceResponse partitionProduceResponse = new ProduceResponseData.PartitionProduceResponse();
        partitionProduceResponse.setIndex(0);

        ProduceResponseData.TopicProduceResponse topicProduceResponse = new ProduceResponseData.TopicProduceResponse();
        topicProduceResponse.setName("test-topic");
        topicProduceResponse.setPartitionResponses(List.of(partitionProduceResponse));

        ProduceResponseData.TopicProduceResponseCollection topicProduceResponses = new ProduceResponseData.TopicProduceResponseCollection();
        topicProduceResponses.add(topicProduceResponse);
        topicProduceResponses.forEach(e -> e.partitionResponses());

        ProduceResponseData produceResponseData = new ProduceResponseData();
        produceResponseData.setResponses(topicProduceResponses);
        ProduceResponse produceResponse = new ProduceResponse(produceResponseData);

        System.out.println("Обработан apikey: PRODUCE");
        requestChannel.sendResponse(request, produceResponse, Option.empty());

        try {
            TestDataClass testDataClass = objectMapper.readValue(s, TestDataClass.class);
        } catch (JsonProcessingException e) {

        }
        System.out.println("Полученное сообщение: " + s);

    }
}
