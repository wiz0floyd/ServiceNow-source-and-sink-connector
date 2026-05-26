package com.servicenow.kafka.connect.hermes;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.*;

public class HermesSinkTaskMetrics implements HermesSinkTaskMetricsMBean {

    private final AtomicLong recordsForwarded = new AtomicLong();
    private final AtomicLong recordsFailed = new AtomicLong();
    private volatile long lastPutTimestampMs = 0L;
    private ObjectName objectName;

    public void register(String connectorName, int taskId) {
        try {
            objectName = new ObjectName(
                "servicenow.kafka.connect:type=HermesSinkTask,connector=" + connectorName + ",task=" + taskId);
            ManagementFactory.getPlatformMBeanServer().registerMBean(this, objectName);
        } catch (Exception e) {
            // Non-fatal — metrics are best-effort
        }
    }

    public void unregister() {
        if (objectName != null) {
            try { ManagementFactory.getPlatformMBeanServer().unregisterMBean(objectName); } catch (Exception ignored) {}
        }
    }

    public void recordForwarded(int count) { recordsForwarded.addAndGet(count); lastPutTimestampMs = System.currentTimeMillis(); }
    public void recordFailed() { recordsFailed.incrementAndGet(); }

    @Override public long getRecordsForwarded() { return recordsForwarded.get(); }
    @Override public long getRecordsFailed() { return recordsFailed.get(); }
    @Override public long getLastPutTimestampMs() { return lastPutTimestampMs; }
}
