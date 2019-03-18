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

    static void cleanupActivityId() {
        // remove the ActivityId that belongs to this thread.
        long uniqueThreadId = Thread.currentThread().getId();

        if (activityIdTlsMap.containsKey(uniqueThreadId)) {
            activityIdTlsMap.remove(uniqueThreadId);
        }
    }

    // Get the current ActivityId in TLS
    static ActivityId getCurrent() {
        // get the value in TLS, not reference
        long uniqueThreadId = Thread.currentThread().getId();

        // Since the Id for each thread is unique, this assures that the below if statement is run only once per thread.
        if (!activityIdTlsMap.containsKey(uniqueThreadId)) {
            activityIdTlsMap.put(uniqueThreadId, new ActivityId());
        }

        return activityIdTlsMap.get(uniqueThreadId);
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
    
    /*
     * Prevent instantiation.
     */
    private ActivityCorrelator() {}
}


class ActivityId {
    private final UUID id;
    private long sequence;
    private boolean isSentToServer;

    ActivityId() {
        id = UUID.randomUUID();
        sequence = 0;
        isSentToServer = false;
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
