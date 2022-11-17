package com.netflix.conductor.kafka.eventqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.events.queue.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class KafkaPublishObservableQueue extends AbstractKafkaObservableQueue {
    private static final Logger logger = LoggerFactory.getLogger(KafkaPublishObservableQueue.class);

    private static final String MSG_HEADERS = "headers";
    private static final String MSG_VALUE = "payload";
    private static final String MSG_KEY = "key";
    private static final String MSG_WORKFLOW_ID = "workflowInstanceId";

    private static final String RECORD_HEADER_WORKFLOW_ID = "workflowInstanceId";

    private final String queueName;

    private final KafkaProducer<String, byte[]> producer;

    private final ObjectMapper objectMapper;

    public KafkaPublishObservableQueue(String queueName, KafkaProducer<String, byte[]> producer) {
        this.queueName = queueName;
        this.producer = producer;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public String getURI() {
        return queueName;
    }

    @Override
    public void publish(List<Message> messages) {
        publishMessages(messages);
    }

    /**
     * Publish the messages to the given topic.
     */
    private void publishMessages(List<Message> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }
        Map messageMap;
        for (Message message : messages) {
            logger.info("Building Record from message[{}]: id {}, payload {}",
                    new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()), message.getId(), message.getPayload());
            try {
                messageMap = objectMapper.readValue(message.getPayload(), Map.class);
            } catch (JsonProcessingException e) {
                logger.error("Error while parsing json from message payload", e);
                continue;
            }
            if (!messageMap.containsKey(MSG_VALUE)) {
                logger.error("Message payload doesnt contain a value");
                continue;
            }
            String key = messageMap.containsKey(MSG_KEY) ? (String) messageMap.get(MSG_KEY) : message.getId();
            byte[] value;
            try {
                value = objectMapper.writeValueAsBytes(messageMap.get(MSG_VALUE));
            } catch (JsonProcessingException e) {
                logger.error("Error building json from message value");
                continue;
            }

            final ProducerRecord<String, byte[]> record = new ProducerRecord<>(queueName, key, value);

            Map<String, String> headers = (Map) messageMap.get(MSG_HEADERS);
            if (headers != null) {
                for (Map.Entry<String, String> headerEntry : headers.entrySet()) {
                    record.headers().add(headerEntry.getKey(), headerEntry.getValue() == null ? null :
                            headerEntry.getValue().getBytes(StandardCharsets.UTF_8));
                }
            }
            record.headers().add(RECORD_HEADER_WORKFLOW_ID, ((String) messageMap.get(MSG_WORKFLOW_ID))
                    .getBytes(StandardCharsets.UTF_8));

            RecordMetadata metadata;
            try {
                this.producer.beginTransaction();
                metadata = this.producer.send(record).get();
                this.producer.commitTransaction();
                logger.info("Producer Record: key {}, value {}, headers {}, partition {}, offset {}", record.key(),
                        record.value(), record.headers(), metadata.partition(), metadata.offset());
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

    @Override
    public Observable<Message> observe() {
        logger.info("Calling observe from publisher");
        return Observable.never();
    }

    @Override
    public List<String> ack(List<Message> messages) {
        logger.info("Calling publish ack for messages: {}", messages);
        return Collections.emptyList();
    }
}
