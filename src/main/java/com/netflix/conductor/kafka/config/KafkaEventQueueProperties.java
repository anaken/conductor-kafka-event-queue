package com.netflix.conductor.kafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("conductor.event-queues.kafka")
public class KafkaEventQueueProperties {
    private Integer pollIntervalMs;

    private Integer pollTimeoutMs;

    private String bootstrapServers;

    private String defaultGroupId;

    private String offset = "earliest";

    private String clientId = "conductor";

    public Integer getPollIntervalMs() {
        return pollIntervalMs;
    }

    public void setPollIntervalMs(Integer pollIntervalMs) {
        this.pollIntervalMs = pollIntervalMs;
    }

    public Integer getPollTimeoutMs() {
        return pollTimeoutMs;
    }

    public void setPollTimeoutMs(Integer pollTimeoutMs) {
        this.pollTimeoutMs = pollTimeoutMs;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getDefaultGroupId() {
        return defaultGroupId;
    }

    public void setDefaultGroupId(String defaultGroupId) {
        this.defaultGroupId = defaultGroupId;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }
}
