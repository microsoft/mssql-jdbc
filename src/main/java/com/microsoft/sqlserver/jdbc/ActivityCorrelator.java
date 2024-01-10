/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.UUID;


/**
 * ActivityCorrelator provides the APIs to access the ActivityId in TLS
 */
final class ActivityCorrelator {

    private static ThreadLocal<ActivityId> t_ActivityId = new ThreadLocal<ActivityId>() {
        @Override
        protected ActivityId initialValue() {
            return new ActivityId();
        }
    };

    static ActivityId getCurrent() {
        return t_ActivityId.get();
    }

    // Increment the Sequence number of the ActivityId in TLS
    // and return the ActivityId with new Sequence number
    static ActivityId getNext() {
        return getCurrent().getIncrement();
    }

    /*
     * Prevent instantiation.
     */
    private ActivityCorrelator() {}
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
