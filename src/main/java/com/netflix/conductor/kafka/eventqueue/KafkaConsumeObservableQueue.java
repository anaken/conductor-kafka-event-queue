package com.netflix.conductor.kafka.eventqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.kafka.config.KafkaEventQueueProperties;
import com.netflix.conductor.metrics.Monitors;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class KafkaConsumeObservableQueue extends AbstractKafkaObservableQueue {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumeObservableQueue.class);

    private static final String QUEUE_PARAM_TOPICS = "topics";
    private static final String QUEUE_PARAM_GROUP = "group";
    private static final String QUEUE_PARAM_FILTER_NAME = "filteringHeader";
    private static final String QUEUE_PARAM_FILTER_VALUE = "filteringValue";
    private static final String QUEUE_PARAM_DLQ_TOPIC = "dlq";

    public static final String QUEUE_PARAM_NAME = "name";
    public static final String QUEUE_PARAM_VERSION = "id";

    private static final String QUEUE_FILTER_BY_PARTITION_KEY = "PARTITION_KEY";

    private static final String MSG_KEY_SEPARATOR = ":";
    private static final String TOPICS_SEPARATOR = ",";

    private final String queueURI;

    private final String queueName;

    private List<String> topics;

    private String id;

    private String groupId;

    private String filteringHeader;

    private boolean filterByPartitionKey = false;

    private String filteringValue;

    private String dlqTopic;

    private int pollIntervalInMS = 100;

    private int pollTimeoutInMs = 1000;

    private KafkaConsumer<String, String> consumer;

    private final ObjectMapper objectMapper;

    private final KafkaProducer<String, byte[]> producer;

    private Subscription subscription;

    private ThreadLocal<Map<String, ConsumerRecord<String, String>>> consumedRecords = new ThreadLocal<>();

    public KafkaConsumeObservableQueue(String queueName, Map<String, String> queueParams,
                                       KafkaEventQueueProperties properties,
                                       KafkaProducer<String, byte[]> producer) {
        this.producer = producer;
        this.queueURI = queueName;
        this.groupId = properties.getDefaultGroupId();
        if (!queueParams.containsKey(QUEUE_PARAM_TOPICS)) {
            String error = String.format("Missing required parameter \"%s\".", QUEUE_PARAM_TOPICS);
            logger.error(error);
            throw new IllegalArgumentException(error);
        }
        this.queueName = queueParams.get(QUEUE_PARAM_TOPICS);
        this.id = queueParams.get(QUEUE_PARAM_NAME);
        if (queueParams.containsKey(QUEUE_PARAM_GROUP)) {
            this.groupId = queueParams.get(QUEUE_PARAM_GROUP);
        }
        if (queueParams.containsKey(QUEUE_PARAM_DLQ_TOPIC)) {
            this.dlqTopic = queueParams.get(QUEUE_PARAM_DLQ_TOPIC);
        }
        if (queueParams.containsKey(QUEUE_PARAM_FILTER_NAME) && queueParams.containsKey(QUEUE_PARAM_FILTER_VALUE)) {
            String filterName = queueParams.get(QUEUE_PARAM_FILTER_NAME);
            if (QUEUE_FILTER_BY_PARTITION_KEY.equals(filterName)) {
                this.filterByPartitionKey = true;
            } else {
                this.filteringHeader = filterName;
            }
            this.filteringValue = queueParams.get(QUEUE_PARAM_FILTER_VALUE);
        }
        if (properties.getPollIntervalMs() != null) {
            this.pollIntervalInMS = properties.getPollIntervalMs();
        }
        if (properties.getPollTimeoutMs() != null) {
            this.pollTimeoutInMs = properties.getPollTimeoutMs();
        }
        this.topics = Arrays.asList(this.queueName.split(TOPICS_SEPARATOR));
        this.objectMapper = new ObjectMapper();
        initConsumer(properties);
    }

    /**
     * Initializes the kafka consumer with the defaults. Fails in case of any
     * mandatory configs are missing.
     */
    private void initConsumer(KafkaEventQueueProperties properties) {
        try {
            Properties consumerProperties = new Properties();
            String prop = properties.getBootstrapServers();
            if (prop != null) {
                consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, prop);
            }
            prop = this.groupId;
            if (prop != null) {
                consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, prop);
            }
            prop = properties.getOffset();
            if (prop != null) {
                consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, prop);
            }
            //consumerProperties.put("security.protocol", SecurityProtocol.SASL_PLAINTEXT.name);
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

            consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, queueURI + "_conductor_consumer");
            checkConsumerProps(consumerProperties);

            this.consumer = new KafkaConsumer<>(consumerProperties);
            this.consumer.subscribe(this.topics);
        } catch (KafkaException e) {
            logger.error("Error while configuring kafka consumer", e);
        }
    }

    @Override
    public String getName() {
        return queueURI;
    }

    @Override
    public String getURI() {
        return queueURI;
    }

    public String getId() {
        return id;
    }

    /**
     * Checks mandatory configs are available for kafka consumer.
     */
    private void checkConsumerProps(Properties consumerProps) {
        List<String> mandatoryKeys = Arrays.asList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        List<String> keysNotFound = hasKeyAndValue(consumerProps, mandatoryKeys);
        if (keysNotFound.size() > 0) {
            String error = String.format("Configuration missing for Kafka consumer. %s", keysNotFound);
            logger.error(error);
            throw new RuntimeException(error);
        }
    }

    /**
     * Validates whether the property has given keys.
     */
    private List<String> hasKeyAndValue(Properties prop, List<String> keys) {
        List<String> keysNotFound = new ArrayList<>();
        for (String key : keys) {
            if (!prop.containsKey(key) || Objects.isNull(prop.get(key))) {
                keysNotFound.add(key);
            }
        }
        return keysNotFound;
    }

    @Override
    public Observable<Message> observe() {
        Observable.OnSubscribe<Message> subscriber = getOnSubscribe();
        return Observable.create(subscriber);
    }

    private Observable.OnSubscribe<Message> getOnSubscribe() {
        return subscriber -> {
            Observable<Long> interval = Observable.interval(pollIntervalInMS, TimeUnit.MILLISECONDS);
            subscription = interval.flatMap((Long x) -> Observable.from(receiveMessages()))
                    .subscribe(subscriber::onNext, subscriber::onError);
        };
    }

    @Override
    public List<String> ack(List<Message> messages) {
        Map<TopicPartition, Long> offsetMap = new HashMap<>();
        Map<String, ConsumerRecord<String, String>> failedRecords = consumedRecords.get();
        for (Message message : messages) {
            String[] idParts = message.getId().split(MSG_KEY_SEPARATOR);
            putOffset(offsetMap, idParts[1], Integer.parseInt(idParts[2]), Integer.parseInt(idParts[3]));
            failedRecords.remove(message.getId());
        }
        if (dlqTopic == null || failedRecords.isEmpty()) {
            commitOffsets(offsetMap);
        } else {
            publishToDlqAndCommitOffsets(failedRecords.values(), offsetMap);
        }
        return Collections.emptyList();
    }

    /**
     * Polls the topics and retrieve the messages
     */
    private List<Message> receiveMessages() {
        if (!isRunning()) {
            close();
            return Collections.emptyList();
        }
        return receiveMessages(consumer);
    }

    /**
     * Polls the topics and retrieve the messages for a consumer.
     */
    private List<Message> receiveMessages(KafkaConsumer<String, String> consumer) {
        List<Message> messages = new ArrayList<>();
        try {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutInMs));

            if (records.count() == 0) {
                return messages;
            }

            logger.info("polled {} messages from kafka topic {}.", records.count(), this.queueURI);
            Map<TopicPartition, Long> discardedOffsetMap = new HashMap<>();
            Map<String, ConsumerRecord<String, String>> msgIdRecordMap = new HashMap<>();
            records.forEach(record -> {
                Map<String, String> headers = new HashMap<>();
                record.headers().forEach(header -> headers.put(header.key(),
                        new String(header.value(), StandardCharsets.UTF_8)));
                if (this.filteringHeader != null && !this.filteringValue.equals(headers.get(this.filteringHeader))
                        || this.filterByPartitionKey && !this.filteringValue.equals(record.key())) {
                    putOffset(discardedOffsetMap, record.topic(), record.partition(), record.offset());
                    return;
                }
                removeOffset(discardedOffsetMap, record.topic(), record.partition());
                String headersJson = null;
                try {
                    headersJson = objectMapper.writeValueAsString(headers);
                } catch (JsonProcessingException e) {
                    logger.error("message headers build error.", e);
                }
                logger.debug("Consumer Record: key: {}, value: {}, partition: {}, offset: {}",
                        record.key(), record.value(), record.partition(), record.offset());
                String id = record.key() + MSG_KEY_SEPARATOR + record.topic() + MSG_KEY_SEPARATOR +
                        record.partition() + MSG_KEY_SEPARATOR + record.offset();
                String messagePayload = String.format("{\"eventData\":%s,\"eventHeaders\":%s,\"eventKey\":\"%s\"}",
                        record.value(), headersJson, record.key());

                Message message = new Message(id, messagePayload, "");
                messages.add(message);
                msgIdRecordMap.put(id, record);
            });
            consumedRecords.set(msgIdRecordMap);
            commitOffsets(discardedOffsetMap);
            Monitors.recordEventQueueMessagesProcessed(QUEUE_TYPE, this.queueURI, messages.size());
        } catch (KafkaException e) {
            logger.error("kafka consumer message polling failed.", e);
            Monitors.recordObservableQMessageReceivedErrors(QUEUE_TYPE);
        }
        return messages;
    }

    private void removeOffset(Map<TopicPartition, Long> offsetMap, String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        offsetMap.remove(topicPartition);
    }

    private void putOffset(Map<TopicPartition, Long> offsetMap, String topic, int partition, long offset) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        offsetMap.compute(topicPartition, (k, v) -> v == null ? offset : Math.max(v, offset));
    }

    private Map<TopicPartition, OffsetAndMetadata> buildPartitionOffsetMetadata(Map<TopicPartition, Long> offsetMap) {
        Map<TopicPartition, OffsetAndMetadata> partitionsOffsets = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> offsetEntry : offsetMap.entrySet()) {
            partitionsOffsets.put(offsetEntry.getKey(),
                    new OffsetAndMetadata(offsetEntry.getValue() + 1, "no metadata"));
        }
        return partitionsOffsets;
    }

    private void commitOffsets(Map<TopicPartition, Long> offsetMap) {
        try {
            consumer.commitSync(buildPartitionOffsetMetadata(offsetMap));
        } catch (KafkaException ke) {
            logger.error("kafka consumer commit failed.", ke);
        }
    }

    private void publishToDlqAndCommitOffsets(Collection<ConsumerRecord<String, String>> records,
                                              Map<TopicPartition, Long> offsetMap) {
        producer.beginTransaction();
        for (ConsumerRecord<String, String> consumerRecord : records) {
            final ProducerRecord<String, byte[]> record = new ProducerRecord<>(dlqTopic, consumerRecord.key(),
                    consumerRecord.value().getBytes(StandardCharsets.UTF_8));
            for (Header header : consumerRecord.headers()) {
                record.headers().add(header.key(), header.value());
            }
            producer.send(record);
        }
        producer.sendOffsetsToTransaction(buildPartitionOffsetMetadata(offsetMap), consumer.groupMetadata());
        producer.commitTransaction();
    }

    @Override
    public void publish(List<Message> messages) {
        throw new UnsupportedOperationException("Invalid queue parameters. Publish is not supported");
    }

    @Override
    public void close() {
        logger.info("Closing queue {}", this.queueURI);
        if (this.consumer != null) {
            subscription.unsubscribe();
            consumer.unsubscribe();
            consumer.close();
        }
    }
}
