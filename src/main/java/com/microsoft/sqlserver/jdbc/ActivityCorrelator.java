/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


/**
 * ActivityCorrelator provides the APIs to access the ActivityId in TLS
 */
final class ActivityCorrelator {

    private static Map<Long, ActivityId> activityIdTlsMap = new ConcurrentHashMap<>();

    static void clear() {
        if (null != activityIdTlsMap) {
            activityIdTlsMap.clear();
        }
    }
    
    static void cleanupActivityId() {
        // remove ActivityIds that belongs to this thread or no longer have an associated thread.
        System.out.println("Map before deleting entry: " + ActivityCorrelator.getActivityIdTlsMap());
        activityIdTlsMap.entrySet().removeIf(e ->
                e.getValue() == null ||
                e.getValue().getThread() == null ||
                e.getValue().getThread() == Thread.currentThread() ||
                !e.getValue().getThread().isAlive());
        System.out.println("Map after deleting entry: " + ActivityCorrelator.getActivityIdTlsMap());
    }

    // Get the current ActivityId in TLS
    static ActivityId getCurrent() {
        // get the value in TLS, not reference
        Thread thread = Thread.currentThread();
        if (!activityIdTlsMap.containsKey(thread.getId())) {
            activityIdTlsMap.put(thread.getId(), new ActivityId(thread));
            System.out.println("Map after adding entry: " + ActivityCorrelator.getActivityIdTlsMap());
        }
        
        return activityIdTlsMap.get(thread.getId());
    }

    // Increment the Sequence number of the ActivityId in TLS
    // and return the ActivityId with new Sequence number
    static ActivityId getNext() {
        // Get the current ActivityId in TLS
        ActivityId activityId = getCurrent();

        // Increment the Sequence number
        activityId.increment();

        return activityId;
    }

    static void setCurrentActivityIdSentFlag() {
        ActivityId activityId = getCurrent();
        activityId.setSentFlag();
    }
    
    static Map<Long, ActivityId> getActivityIdTlsMap() {
        return activityIdTlsMap;
    }
    
    /*
     * Prevent instantiation.
     */
    private ActivityCorrelator() {}
}


class ActivityId {
    private final UUID id;
    private final Thread thread;
    private long sequence;
    private boolean isSentToServer;

    ActivityId(Thread thread) {
        id = UUID.randomUUID();
        this.thread = thread;
        sequence = 0;
        isSentToServer = false;
    }
    
    Thread getThread() {
        return thread;
    }

    UUID getId() {
        return id;
    }

    long getSequence() {
        return sequence;
    }

    void increment() {
        if (sequence < 0xffffffffl) // to get to 32-bit unsigned
        {
            ++sequence;
        } else {
            sequence = 0;
        }

        isSentToServer = false;
    }

    void setSentFlag() {
        isSentToServer = true;
    }

    boolean isSentToServer() {
        return isSentToServer;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(id.toString());
        sb.append("-");
        sb.append(sequence);
        return sb.toString();
    }
}
