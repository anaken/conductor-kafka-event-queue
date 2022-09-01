package com.netflix.conductor.kafka.config;

import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.kafka.eventqueue.KafkaObservableQueue;
import org.springframework.lang.NonNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaEventQueueProvider implements EventQueueProvider {

    private final Map<String, ObservableQueue> queues = new ConcurrentHashMap<>();
    private final KafkaEventQueueProperties properties;

    public KafkaEventQueueProvider(KafkaEventQueueProperties properties) {
        this.properties = properties;
    }

    @Override
    public String getQueueType() {
        return "kafka";
    }

    @Override
    @NonNull
    public ObservableQueue getQueue(String queueURI) {
        return queues.computeIfAbsent(queueURI, q -> new KafkaObservableQueue(queueURI, properties));
    }
}
