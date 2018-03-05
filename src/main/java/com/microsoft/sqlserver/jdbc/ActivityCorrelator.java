/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.UUID;

/**
 * ActivityCorrelator provides the APIs to access the ActivityId in TLS
 */
final class ActivityCorrelator {

    private static ThreadLocal<ActivityId> ActivityIdTls = new ThreadLocal<ActivityId>() {
        protected ActivityId initialValue() {
            return new ActivityId();
        }
    };

    // Get the current ActivityId in TLS
    static ActivityId getCurrent() {
        // get the value in TLS, not reference
        return ActivityIdTls.get();
    }

    // Increment the Sequence number of the ActivityId in TLS
    // and return the ActivityId with new Sequence number
    static ActivityId getNext() {
        // We need to call get() method on ThreadLocal to get
        // the current value of ActivityId stored in TLS,
        // then increment the sequence number.

        // Get the current ActivityId in TLS
        ActivityId activityId = getCurrent();

        // Increment the Sequence number
        activityId.Increment();

        return activityId;
    }

    static void setCurrentActivityIdSentFlag() {
        ActivityId activityId = getCurrent();
        activityId.setSentFlag();
    }

}

class ActivityId {
    private final UUID Id;
    private long Sequence;
    private boolean isSentToServer;

    ActivityId() {
        Id = UUID.randomUUID();
        Sequence = 0;
        isSentToServer = false;
    }

    UUID getId() {
        return Id;
    }

    long getSequence() {
        return Sequence;
    }

    void Increment() {
        if (Sequence < 0xffffffffl) // to get to 32-bit unsigned
        {
            ++Sequence;
        }
        else {
            Sequence = 0;
        }

        isSentToServer = false;
    }

    void setSentFlag() {
        isSentToServer = true;
    }

    boolean IsSentToServer() {
        return isSentToServer;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(Id.toString());
        sb.append("-");
        sb.append(Sequence);
        return sb.toString();
    }
}
