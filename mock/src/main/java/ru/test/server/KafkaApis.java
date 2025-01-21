package ru.test.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.network.RequestChannel;
import kafka.server.ApiRequestHandler;
import kafka.server.BrokerFeatures;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.LazyDownConversionRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import scala.Option;
import scala.reflect.ClassTag;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;

import static org.apache.kafka.common.requests.JoinGroupRequest.UNKNOWN_MEMBER_ID;

public class KafkaApis implements ApiRequestHandler {

    private final RequestChannel requestChannel;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaApis(RequestChannel requestChannel) {
        this.requestChannel = requestChannel;
    }


    @Override
    public void handle(RequestChannel.Request request, kafka.server.RequestLocal requestLocal) {
        System.out.println("[" + LocalDateTime.now() + "] - " + request.header().apiKey());
        try {
            switch (request.header().apiKey()) {
                case API_VERSIONS -> handleApiVersionsRequest(request);
                case METADATA -> handleTopicMetadataRequest(request);
                case INIT_PRODUCER_ID -> handleInitProducerIdRequest(request);
                case PRODUCE -> handleProduceRequest(request);
                case FIND_COORDINATOR -> handleFindCoordinatorRequest(request);
                case JOIN_GROUP -> handleJoinGroupRequest(request);
                case HEARTBEAT -> handleHeartbeatRequest(request);
                case SYNC_GROUP -> handleSyncGroupRequest(request);
                case OFFSET_FETCH -> handleOffsetFetchRequest(request);
                case FETCH -> handleFetchRequest(request);

            }
        } catch (Exception e) {

        }
    }

    @Override
    public void tryCompleteActions() {

    }

    private void handleApiVersionsRequest(kafka.network.RequestChannel.Request request) {
        System.out.println("API Request connectionId = " + request.context().connectionId);
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
        System.out.println("Meta Request connectionId = " + request.context().connectionId);
        MetadataRequest metadataRequest = request.body(ClassTag.apply(MetadataRequest.class), null);
        short requestVersion = request.header().apiVersion();

        MetadataResponseData.MetadataResponsePartition partition = new MetadataResponseData.MetadataResponsePartition();
        partition.setErrorCode((short)0);
        partition.setLeaderId(77);
        partition.setLeaderEpoch(8);
        partition.setReplicaNodes(List.of(1));
        partition.setIsrNodes(List.of(1));

        MetadataResponseData.MetadataResponseTopic topic = new MetadataResponseData.MetadataResponseTopic();
        topic.setErrorCode((short) 0);
        topic.setTopicId(Uuid.fromString("8yXyiGdERAiJw0Ho16h6cw"));
        topic.setName("test-topic");
        topic.setIsInternal(false);
        topic.setTopicAuthorizedOperations(Integer.MIN_VALUE);
        topic.setPartitions(List.of(partition));

        MetadataResponse metadataResponse = MetadataResponse.prepareResponse(
                requestVersion,
                0,
                List.of(new Node(77, "127.0.0.1", 9092)),
                "1",
                0,
                List.of(topic),
                Integer.MIN_VALUE
        );

        System.out.println("Обработан apikey: METADATA");
        requestChannel.sendResponse(request, metadataResponse, Option.empty());
    }

    private void handleInitProducerIdRequest(kafka.network.RequestChannel.Request request) {
        System.out.println("Init Producer Request connectionId = " + request.context().connectionId);
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
        System.out.println("Produce Request connectionId = " + request.context().connectionId);

        ProduceRequest produceRequest = request.body(ClassTag.apply(ProduceRequest.class), null);
        short acks = produceRequest.acks();
        ProduceRequestData.TopicProduceDataCollection topicProduceData = produceRequest.data().topicData();
        ProduceRequestData.PartitionProduceData first = topicProduceData.stream().findFirst().get().partitionData().stream().findFirst().get();
        MemoryRecords records = (MemoryRecords) first.records();
        ProduceRequest.validateRecords(request.header().apiVersion(), records);

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

    private void handleFindCoordinatorRequest(kafka.network.RequestChannel.Request request) {
        System.out.println("FindCoordinator Request connectionId = " + request.context().connectionId);

        short version = request.header().apiVersion();

        handleFindCoordinatorRequestV4AndAbove(request);
    }

    private void handleFindCoordinatorRequestV4AndAbove(kafka.network.RequestChannel.Request request) {
        FindCoordinatorRequest findCoordinatorRequest = request.body(ClassTag.apply(FindCoordinatorRequest.class), null);

        List<FindCoordinatorResponseData.Coordinator> coordinators = findCoordinatorRequest.data().coordinatorKeys().stream().map(key -> {
            byte b = findCoordinatorRequest.data().keyType();
            FindCoordinatorResponseData.Coordinator coordinator = new FindCoordinatorResponseData.Coordinator();
            coordinator.setErrorCode((short)0);
            coordinator.setKey(key);
            coordinator.setHost("127.0.0.1");
            coordinator.setNodeId(Integer.MAX_VALUE - 77);
            coordinator.setPort(9092);
            return coordinator;
        }).toList();


        FindCoordinatorResponseData findCoordinatorResponseData = new FindCoordinatorResponseData()
                .setCoordinators(coordinators);
        FindCoordinatorResponse findCoordinatorResponse = new FindCoordinatorResponse(findCoordinatorResponseData);

        System.out.println("Обработан apikey: FIND_COORDINATOR");
        requestChannel.sendResponse(request, findCoordinatorResponse, Option.empty());
    }

    private void handleJoinGroupRequest(RequestChannel.Request request) {
        System.out.println("JoinGroup Request connectionId = " + request.context().connectionId);

        short version = request.context().apiVersion();
        JoinGroupRequest joinGroupRequest = request.body(ClassTag.apply(JoinGroupRequest.class), null);
        JoinGroupRequestData data = joinGroupRequest.data();
        boolean isUnknownMember = data.memberId().equals(UNKNOWN_MEMBER_ID);
        data.memberId();
        data.groupId();

        ConsumerPartitionAssignor.Subscription subscription = new ConsumerPartitionAssignor.Subscription(List.of("test-topic"));
        ByteBuffer metadata = ConsumerProtocol.serializeSubscription(subscription);

        JoinGroupResponseData.JoinGroupResponseMember member = new JoinGroupResponseData.JoinGroupResponseMember();
        member.setMemberId("my-member");
        member.setMetadata(Utils.toArray(metadata));

        JoinGroupResponseData joinGroupResponseData = new JoinGroupResponseData();
        joinGroupResponseData.setMembers(List.of(member));
        joinGroupResponseData.setMemberId("my-member");
        joinGroupResponseData.setProtocolName("range");
//        joinGroupResponseData.setProtocolType(null);
        joinGroupResponseData.setLeader("my-member");
//        joinGroupResponseData.setSkipAssignment(false);

        JoinGroupResponse joinGroupResponse = new JoinGroupResponse(joinGroupResponseData, version);

        System.out.println("Обработан apikey: JOIN_GROUP");
        requestChannel.sendResponse(request, joinGroupResponse, Option.empty());
    }

    private void handleHeartbeatRequest(RequestChannel.Request request) {

        HeartbeatResponseData heartbeatResponseData = new HeartbeatResponseData();

        System.out.println("Обработан apikey: HEARTBEAT");
        requestChannel.sendResponse(request, new HeartbeatResponse(heartbeatResponseData), Option.empty());
    }

    private void handleSyncGroupRequest(RequestChannel.Request request) {

        TopicPartition topicPartition = new TopicPartition("test-topic", 0);
        ConsumerPartitionAssignor.Assignment assignment = new ConsumerPartitionAssignor.Assignment(List.of(topicPartition));

        SyncGroupResponseData syncGroupResponseData = new SyncGroupResponseData();
        syncGroupResponseData.setAssignment(Utils.toArray(ConsumerProtocol.serializeAssignment(assignment)));

        SyncGroupResponse syncGroupResponse = new SyncGroupResponse(syncGroupResponseData);

        System.out.println("Обработан apikey: SYNC_GROUP");
        requestChannel.sendResponse(request, syncGroupResponse, Option.empty());
    }

    private void handleOffsetFetchRequest(RequestChannel.Request request) {
        handleOffsetFetchRequestFromCoordinator(request);
    }

    private void handleOffsetFetchRequestFromCoordinator(RequestChannel.Request request) {
        OffsetFetchResponseData.OffsetFetchResponsePartitions partitions = new OffsetFetchResponseData.OffsetFetchResponsePartitions();
        partitions.setMetadata("");
        partitions.setErrorCode((short) 0);
        partitions.setPartitionIndex(0);
        partitions.setCommittedOffset(0);

        OffsetFetchResponseData.OffsetFetchResponseTopics topics = new OffsetFetchResponseData.OffsetFetchResponseTopics();
        topics.setName("test-topic");
        topics.setPartitions(List.of(partitions));

        OffsetFetchResponseData.OffsetFetchResponseGroup group = new OffsetFetchResponseData.OffsetFetchResponseGroup();
        group.setGroupId("my-group");
        group.setErrorCode((short) 0);
        group.setTopics(List.of(topics));

        OffsetFetchResponse offsetFetchResponse = new OffsetFetchResponse(List.of(group), request.context().apiVersion());

        System.out.println("Обработан apikey: OFFSET_FETCH");
        requestChannel.sendResponse(request, offsetFetchResponse, Option.empty());
    }

    private void handleFetchRequest(RequestChannel.Request request) {

        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.fromString("8yXyiGdERAiJw0Ho16h6cw"),  new TopicPartition("test-topic", 0));
//        TopicIdPartition topicIdPartition1 = new TopicPartition(Uuid.randomUuid(), new Pa)
        FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData();
        partitionData.setPartitionIndex(0);
        partitionData.setErrorCode((short) 0);
//        partitionData.setHighWatermark()
        partitionData.setLastStableOffset(0);
        partitionData.setLogStartOffset(0);
//        partitionData.setAbortedTransactions()

        String a = "jello";
        MemoryRecords memoryRecords = MemoryRecords.readableRecords(ByteBuffer.wrap(a.getBytes(StandardCharsets.UTF_8)));
        LazyDownConversionRecords lazyDownConversionRecords = new LazyDownConversionRecords(topicIdPartition.topicPartition(), memoryRecords, (byte) 1, 5L, Time.SYSTEM);
        partitionData.setRecords(lazyDownConversionRecords);
//        partitionData.setPreferredReadReplica()
//        partitionData.setDivergingEpoch()

        FetchResponseData.LeaderIdAndEpoch leaderIdAndEpoch = new FetchResponseData.LeaderIdAndEpoch();
        leaderIdAndEpoch.setLeaderEpoch(0);
        leaderIdAndEpoch.setLeaderId(77);
        partitionData.setCurrentLeader(leaderIdAndEpoch);

        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> topicIdPartitionPartitionDataLinkedHashMap = new LinkedHashMap<>();
        topicIdPartitionPartitionDataLinkedHashMap.put(topicIdPartition, partitionData);


        FetchResponse fetchResponse1 = FetchResponse.of(Errors.NONE, 0, 0, topicIdPartitionPartitionDataLinkedHashMap, List.<Node>of());

        requestChannel.sendResponse(request, fetchResponse1, Option.empty());
    }

}
