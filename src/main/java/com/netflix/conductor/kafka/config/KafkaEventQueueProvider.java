package com.netflix.conductor.kafka.config;

import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.kafka.eventqueue.KafkaConsumeObservableQueue;
import com.netflix.conductor.kafka.eventqueue.KafkaPublishObservableQueue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static com.netflix.conductor.kafka.eventqueue.AbstractKafkaObservableQueue.QUEUE_TYPE;

public class KafkaEventQueueProvider implements EventQueueProvider {
    private static final Logger logger = LoggerFactory.getLogger(KafkaEventQueueProvider.class);

    private final Map<String, ObservableQueue> queues = new ConcurrentHashMap<>();

    private final KafkaEventQueueProperties properties;

    private final KafkaProducer<String, String> producer;

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
            logger.info("starting new queue: " + queueURI);
            if (queueURI.contains("=")) {
                return new KafkaConsumeObservableQueue(queueURI, properties);
            } else {
                return new KafkaPublishObservableQueue(queueURI, producer);
            }
        });
    }

    private KafkaProducer<String, String> createProducer(KafkaEventQueueProperties properties) {
        Properties producerProperties = new Properties();
        if (properties.getBootstrapServers() == null) {
            logger.error("Configuration \"bootstrap-server\" missing for Kafka producer");
            throw new RuntimeException("Configuration \"bootstrap-server\" missing for Kafka producer.");
        }
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, properties.getClientId());
        return new KafkaProducer<>(producerProperties);
    }
}
