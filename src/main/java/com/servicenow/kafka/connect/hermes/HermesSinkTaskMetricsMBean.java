package com.servicenow.kafka.connect.hermes;

public interface HermesSinkTaskMetricsMBean {
    long getRecordsForwarded();
    long getRecordsFailed();
    long getLastPutTimestampMs();
}
