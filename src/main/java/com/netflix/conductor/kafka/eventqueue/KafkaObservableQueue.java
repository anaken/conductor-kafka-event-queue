package com.netflix.conductor.kafka.eventqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.kafka.config.KafkaEventQueueProperties;
import com.netflix.conductor.metrics.Monitors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observable.OnSubscribe;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaObservableQueue implements ObservableQueue {

    private static final Logger logger = LoggerFactory.getLogger(KafkaObservableQueue.class);

    private static final Pattern QUEUE_PARAMS_PATTERN = Pattern.compile("([^=]+)=([^;]+);?");

    private static final String QUEUE_PARAM_TOPICS = "topics";
    private static final String QUEUE_PARAM_GROUP = "group";
    private static final String QUEUE_PARAM_FILTER_NAME = "filteringHeader";
    private static final String QUEUE_PARAM_FILTER_VALUE = "filteringValue";

    private static final String QUEUE_FILTER_BY_PARTITION_KEY = "PARTITION_KEY";

    private static final String MSG_KEY_SEPARATOR = ":";

    public static final String QUEUE_TYPE = "kafka";

    private final String queueURI;

    private final String queueName;

    private final List<String> topics;

    private String groupId;

    private String filteringHeader;

    private boolean filterByPartitionKey = false;

    private String filteringValue;

    private int pollIntervalInMS = 100;

    private int pollTimeoutInMs = 1000;

    private volatile boolean running;

    private final KafkaProducer<String, String> producer;

    private KafkaConsumer<String, String> consumer;

    private final ObjectMapper objectMapper;

    private ThreadLocal<Map<TopicPartition, Long>> commitOffsetMap = ThreadLocal.withInitial(HashMap::new);

    public KafkaObservableQueue(String queueName, KafkaEventQueueProperties properties,
                                KafkaProducer<String, String> producer) {
        this.queueURI = queueName;
        this.groupId = properties.getGroupId();
        Map<String, String> queueParams = parseParams();
        if (queueParams.isEmpty()) {
            String[] queueNameParts = queueName.split(":");
            if (queueNameParts.length > 1) {
                this.groupId = queueNameParts[0];
                this.queueName = queueNameParts[1];
            } else {
                this.queueName = queueNameParts[0];
            }
        } else {
            this.queueName = queueParams.get(QUEUE_PARAM_TOPICS);
            if (queueParams.containsKey(QUEUE_PARAM_GROUP)) {
                this.groupId = queueParams.get(QUEUE_PARAM_GROUP);
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
        }
        this.topics = Arrays.asList(this.queueName.split(","));

        if (properties.getPollIntervalMs() != null) {
            this.pollIntervalInMS = properties.getPollIntervalMs();
        }
        if (properties.getPollTimeoutMs() != null) {
            this.pollTimeoutInMs = properties.getPollTimeoutMs();
        }
        this.producer = producer;
        this.objectMapper = new ObjectMapper();

        init(properties);
    }

    private Map<String, String> parseParams() {
        Matcher matcher = QUEUE_PARAMS_PATTERN.matcher(this.queueURI);
        Map<String, String> params = new HashMap<>();
        while (matcher.find()) {
            params.put(matcher.group(1), matcher.group(2));
        }
        if (!params.containsKey(QUEUE_PARAM_TOPICS)) {
            return new HashMap<>();
        }
        return params;
    }

    /**
     * Initializes the kafka producer with the defaults. Fails in case of any
     * mandatory configs are missing.
     *
     * @param properties
     */
    private void init(KafkaEventQueueProperties properties) {
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

            consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, queueName + "_conductor_consumer");
            checkConsumerProps(consumerProperties);

            /**
             * Create a consumer for each of the topic's partitions. Create one consumer first so that we can use it
             * to get the partition information.
             */
            this.consumer = new KafkaConsumer<>(consumerProperties);
            this.consumer.subscribe(this.topics);
        } catch (KafkaException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void start() {
        logger.info("Started listening to {}:{}", getClass().getSimpleName(), queueName);
        running = true;
    }

    @Override
    public void stop() {
        logger.info("Stopped listening to {}:{}", getClass().getSimpleName(), queueName);
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * Checks mandatory configs are available for kafka consumer.
     *
     * @param consumerProps
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
     *
     * @param prop
     * @param keys
     * @return
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
        OnSubscribe<Message> subscriber = getOnSubscribe();
        return Observable.create(subscriber);
    }

    @Override
    public List<String> ack(List<Message> messages) {
        commitAsyncOffsets();
        List<String> messageIds = new ArrayList<>();
        for (Message message : messages) {
            String[] idParts = message.getId().split(MSG_KEY_SEPARATOR);
            int partitionNumber = Integer.parseInt(idParts[2]);
            for (String topic : this.topics) {
                for (PartitionInfo partition : consumer.partitionsFor(topic)) {
                    if (partitionNumber == partition.partition()) {
                        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
                        currentOffsets.put(new TopicPartition(idParts[1], partitionNumber),
                                new OffsetAndMetadata(Integer.parseInt(idParts[3]) + 1, "no metadata"));
                        try {
                            consumer.commitSync(currentOffsets);
                        } catch (KafkaException ke) {
                            messageIds.add(message.getId());
                            logger.error("kafka consumer selective commit failed.", ke);
                        }
                    }
                }
            }
        }
        return messageIds;
    }

    public void setUnackTimeout(Message message, long unackTimeout) {
    }

    @Override
    public void publish(List<Message> messages) {
        publishMessages(messages);
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public String getType() {
        return QUEUE_TYPE;
    }

    @Override
    public String getName() {
        return queueURI;
    }

    @Override
    public String getURI() {
        return queueURI;
    }

    /**
     * Polls the topics and retrieve the messages for all consumers of the topic.
     *
     * @return List of messages
     */
    List<Message> receiveMessages() {
        return receiveMessages(consumer);
    }

    /**
     * Polls the topics and retrieve the messages for a consumer.
     *
     * @return List of messages
     */
    List<Message> receiveMessages(KafkaConsumer<String, String> consumer) {
        List<Message> messages = new ArrayList<>();
        try {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutInMs));

            if (records.count() == 0) {
                return messages;
            }

            logger.info("polled {} messages from kafka topic.", records.count());
            records.forEach(record -> {
                Map<String, String> headers = new HashMap<>();
                record.headers().forEach(header -> headers.put(header.key(),
                        new String(header.value(), StandardCharsets.UTF_8)));
                if (this.filteringHeader != null && !this.filteringValue.equals(headers.get(this.filteringHeader))
                        || this.filterByPartitionKey && !this.filteringValue.equals(record.key())) {
                    addAsyncCommitOffset(record.topic(), record.partition(), record.offset());
                    return;
                }
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
                String messagePayload = String.format("{\"eventData\":%s,\"eventHeaders\":%s}",
                        record.value(), headersJson);
                Message message = new Message(id, messagePayload, "");
                messages.add(message);
            });
            if (messages.isEmpty()) {
                commitAsyncOffsets();
            }
            Monitors.recordEventQueueMessagesProcessed(QUEUE_TYPE, this.queueURI, messages.size());
        } catch (KafkaException e) {
            logger.error("kafka consumer message polling failed.", e);
            Monitors.recordObservableQMessageReceivedErrors(QUEUE_TYPE);
        }
        return messages;
    }

    void addAsyncCommitOffset(String topic, int partition, long offset) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        commitOffsetMap.get().compute(topicPartition, (k, v) -> v == null ? offset : Math.max(v, offset));
    }

    void commitAsyncOffsets() {
        Map<TopicPartition, OffsetAndMetadata> currentOffsets;
        for (Map.Entry<TopicPartition, Long> offsetEntry : commitOffsetMap.get().entrySet()) {
            currentOffsets = new HashMap<>();
            currentOffsets.put(offsetEntry.getKey(),
                    new OffsetAndMetadata(offsetEntry.getValue() + 1, "no metadata"));
            try {
                consumer.commitSync(currentOffsets);
            } catch (KafkaException ke) {
                logger.error("kafka consumer filtered messages commit failed.", ke);
            }
        }
        commitOffsetMap.get().clear();
    }

    /**
     * Publish the messages to the given topic.
     *
     * @param messages
     */
    void publishMessages(List<Message> messages) {

        if (messages == null || messages.isEmpty()) {
            return;
        }
        for (Message message : messages) {
            final ProducerRecord<String, String> record = new ProducerRecord<>(queueName, message.getId(),
                    message.getPayload());

            RecordMetadata metadata;
            try {
                metadata = this.producer.send(record).get();
                logger.debug("Producer Record: key {}, value {}, partition {}, offset {}", record.key(), record.value(),
                        metadata.partition(), metadata.offset());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Publish message to kafka topic {} failed with an error: {}", queueName, e.getMessage(),
                        e);
            } catch (ExecutionException e) {
                logger.error("Publish message to kafka topic {} failed with an error: {}", queueName, e.getMessage(),
                        e);
                throw new RuntimeException("Failed to publish the event");
            }
        }
        logger.info("Messages published to kafka topic {}. count {}", queueName, messages.size());

    }

    OnSubscribe<Message> getOnSubscribe() {
        return subscriber -> {
            Observable<Long> interval = Observable.interval(pollIntervalInMS, TimeUnit.MILLISECONDS);
            interval.flatMap((Long x) -> Observable.from(receiveMessages()))
                    .subscribe(subscriber::onNext, subscriber::onError);
        };
    }

    @Override
    public void close() {
        if (this.consumer != null) {
            consumer.unsubscribe();
            consumer.close();
        }
    }
}
