package com.servicenow.kafka.connect.hermes;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.*;

public class HermesSourceTaskMetrics implements HermesSourceTaskMetricsMBean {

    private final AtomicLong recordsPollCluster1 = new AtomicLong();
    private final AtomicLong recordsPollCluster2 = new AtomicLong();
    private volatile long lastPollTimestampMs = 0L;
    private ObjectName objectName;

    public void register(String connectorName, int taskId) {
        try {
            objectName = new ObjectName(
                "servicenow.kafka.connect:type=HermesSourceTask,connector=" + connectorName + ",task=" + taskId);
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

    public void recordPollCluster1(int count) { recordsPollCluster1.addAndGet(count); lastPollTimestampMs = System.currentTimeMillis(); }
    public void recordPollCluster2(int count) { recordsPollCluster2.addAndGet(count); lastPollTimestampMs = System.currentTimeMillis(); }

    @Override public long getRecordsPollCluster1() { return recordsPollCluster1.get(); }
    @Override public long getRecordsPollCluster2() { return recordsPollCluster2.get(); }
    @Override public long getLastPollTimestampMs() { return lastPollTimestampMs; }
}
