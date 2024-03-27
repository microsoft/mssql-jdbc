/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


/**
 * ActivityCorrelator provides the APIs to access the ActivityId in a map
 */
final class ActivityCorrelator {

    private static Map<Long, ActivityId> activityIdMap = new ConcurrentHashMap<Long, ActivityId>();

    static ActivityId getCurrent() {
        // get the value, not reference
        @SuppressWarnings("deprecation")
        long uniqueThreadId = Thread.currentThread().getId();

        // Since the Id for each thread is unique, this assures that the below if statement is run only once per thread.
        if (!activityIdMap.containsKey(uniqueThreadId)) {
            activityIdMap.put(uniqueThreadId, new ActivityId());
        }

        return activityIdMap.get(uniqueThreadId);
    }

    // Increment the Sequence number of the ActivityId
    // and return the ActivityId with new Sequence number
    static ActivityId getNext() {
        return getCurrent().getIncrement();
    }

    /*
     * Prevent instantiation.
     */
    private ActivityCorrelator() {}

    static void cleanupActivityId() {
        // remove the ActivityId that belongs to this thread.
        @SuppressWarnings("deprecation")
        long uniqueThreadId = Thread.currentThread().getId();

        if (activityIdMap.containsKey(uniqueThreadId)) {
            activityIdMap.remove(uniqueThreadId);
        }
    }
}


class ActivityId {
    private final UUID id;
    private long sequence;
    // Cache the string since it gets frequently referenced.
    private String cachedToString;

    ActivityId() {
        id = UUID.randomUUID();
        // getNext() is called during prelogin and will be the logical "first call" after
        // instantiation, incrementing this to >= 1 before any activity logs are written.
        sequence = 0;
    }

    UUID getId() {
        return id;
    }

    long getSequence() {
        // Edge case: A new thread re-uses an existing connection. Ensure sequence > 0.
        if (sequence == 0L) {
            ++sequence;
        }

        return sequence;
    }

    ActivityId getIncrement() {
        cachedToString = null;
        if (sequence < 0xffffffffl) // to get to 32-bit unsigned
        {
            ++sequence;
        } else {
            sequence = 0;
        }

        return this;
    }

    @Override
    public String toString() {
        if (cachedToString == null) {
            StringBuilder sb = new StringBuilder(38);
            sb.append(id.toString());
            sb.append("-");
            sb.append(getSequence());
            cachedToString = sb.toString();
        }

        return cachedToString.toString();
    }
}
