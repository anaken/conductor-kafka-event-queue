package com.netflix.conductor.kafka.config;

import com.netflix.conductor.core.events.EventQueueProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(KafkaEventQueueProperties.class)
@ConditionalOnProperty(name = "conductor.event-queues.kafka.enabled", havingValue = "true")
public class KafkaEventQueueConfiguration {

    @Bean
    public EventQueueProvider kafkaEventQueueProvider(KafkaEventQueueProperties properties) {
        return new KafkaEventQueueProvider(properties);
    }
}
