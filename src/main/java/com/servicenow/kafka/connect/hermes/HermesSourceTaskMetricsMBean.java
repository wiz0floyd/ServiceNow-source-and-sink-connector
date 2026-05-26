package com.servicenow.kafka.connect.hermes;

public interface HermesSourceTaskMetricsMBean {
    long getRecordsPollCluster1();
    long getRecordsPollCluster2();
    long getLastPollTimestampMs();
}
