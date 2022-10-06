package com.netflix.conductor.kafka.eventqueue;

import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractKafkaObservableQueue implements ObservableQueue {
    private static final Logger logger = LoggerFactory.getLogger(AbstractKafkaObservableQueue.class);

    public static final String QUEUE_TYPE = "kafka";

    private volatile boolean running;

    @Override
    public void start() {
        logger.info("Started listening to {}:{}", getClass().getSimpleName(), getName());
        running = true;
    }

    @Override
    public void stop() {
        logger.info("Stopped listening to {}:{}", getClass().getSimpleName(), getName());
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    public void setUnackTimeout(Message message, long unackTimeout) {
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public String getType() {
        return QUEUE_TYPE;
    }
}
