/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * ActivityCorrelator provides the APIs to access the ActivityId in TLS
 */
final class ActivityCorrelator {

    private static ActivityId s_ActivityId;
    private static Lock lockObject = new ReentrantLock();

    // Get the current ActivityId in TLS
    static ActivityId getCurrent() {
        if (s_ActivityId == null) {
            lockObject.lock();
            if (s_ActivityId == null) {
                s_ActivityId = new ActivityId();
            }
            lockObject.unlock();
        }

        return s_ActivityId;
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

    /*
     * Prevent instantiation.
     */
    private ActivityCorrelator() {}
}


class ActivityId {
    private final UUID id;
    private long sequence;

    ActivityId() {
        id = UUID.randomUUID();
        sequence = 1;
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
