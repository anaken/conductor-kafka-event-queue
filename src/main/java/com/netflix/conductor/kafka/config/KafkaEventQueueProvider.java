package com.netflix.conductor.kafka.config;

import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.kafka.eventqueue.KafkaConsumeObservableQueue;
import com.netflix.conductor.kafka.eventqueue.KafkaPublishObservableQueue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.netflix.conductor.kafka.eventqueue.AbstractKafkaObservableQueue.QUEUE_TYPE;
import static com.netflix.conductor.kafka.eventqueue.KafkaConsumeObservableQueue.QUEUE_PARAM_ID;

public class KafkaEventQueueProvider implements EventQueueProvider {
    private static final Logger logger = LoggerFactory.getLogger(KafkaEventQueueProvider.class);

    private static final String PARAMS_QUEUE_CONDITION = "=";
    private static final Pattern QUEUE_PARAMS_PATTERN = Pattern.compile("([^=]+)=([^;]+);?");

    private final Map<String, ObservableQueue> queues = new ConcurrentHashMap<>();

    private final KafkaEventQueueProperties properties;

    private final KafkaProducer<String, byte[]> producer;

    public KafkaEventQueueProvider(KafkaEventQueueProperties properties) {
        this.properties = properties;
        this.producer = createProducer(properties);
    }

    @Override
    public String getQueueType() {
        return QUEUE_TYPE;
    }

    @Override
    @NonNull
    public ObservableQueue getQueue(String queueURI) {
        return queues.computeIfAbsent(queueURI, q -> {
            logger.info("Starting new queue: " + queueURI);
            if (queueURI.contains(PARAMS_QUEUE_CONDITION)) {
                Map<String, String> params = parseQueueParams(queueURI);
                removePreviousQueue(params);
                return new KafkaConsumeObservableQueue(queueURI, params, properties, producer);
            } else {
                return new KafkaPublishObservableQueue(queueURI, producer);
            }
        });
    }

    private void removePreviousQueue(Map<String, String> params) {
        String queueId = params.get(QUEUE_PARAM_ID);
        if (queueId == null) {
            return;
        }
        queues.entrySet().stream()
                .filter(entry -> {
                    ObservableQueue queue = entry.getValue();
                    if (queue instanceof KafkaConsumeObservableQueue) {
                        return queueId.equals(((KafkaConsumeObservableQueue) queue).getId());
                    }
                    return false;
                }).findFirst().ifPresent(entry -> {
                    queues.remove(entry.getKey());
                    entry.getValue().stop();
                });
    }

    private KafkaProducer<String, byte[]> createProducer(KafkaEventQueueProperties properties) {
        Properties producerProperties = new Properties();
        if (properties.getBootstrapServers() == null) {
            logger.error("Configuration \"bootstrap-server\" missing for Kafka producer");
            throw new RuntimeException("Configuration \"bootstrap-server\" missing for Kafka producer.");
        }
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, properties.getClientId());
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "conductor-transactional-id");
        KafkaProducer<String, byte[]> createdProducer = new KafkaProducer<>(producerProperties);
        createdProducer.initTransactions();
        return createdProducer;
    }

    private Map<String, String> parseQueueParams(String queueURI) {
        Matcher matcher = QUEUE_PARAMS_PATTERN.matcher(queueURI);
        Map<String, String> params = new HashMap<>();
        while (matcher.find()) {
            params.put(matcher.group(1), matcher.group(2));
        }
        return params;
    }
}
