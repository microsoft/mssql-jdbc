/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.lang.ref.WeakReference;
import java.util.AbstractMap.SimpleEntry;
import java.util.UUID;


/**
 * ActivityCorrelator provides access to the current thread's ActivityId.
 */
final class ActivityCorrelator {

    // Pooled connections may be closed by another thread, so state must follow the opener's lifetime.
    // Strongly retain only JDK-loaded objects to avoid pinning an application class loader on a live container thread.
    // Weakly cache the driver wrapper; UUID and sequence survive its collection in the primitive array.
    private static final ThreadLocal<SimpleEntry<long[], WeakReference<ActivityId>>> activityId = ThreadLocal
            .withInitial(() -> {
                UUID id = UUID.randomUUID();
                long[] state = {id.getMostSignificantBits(), id.getLeastSignificantBits(), 0};
                return new SimpleEntry<>(state, new WeakReference<>(new ActivityId(state)));
            });

    static ActivityId getCurrent() {
        SimpleEntry<long[], WeakReference<ActivityId>> entry = activityId.get();
        ActivityId current = entry.getValue().get();
        if (current == null) {
            current = new ActivityId(entry.getKey());
            entry.setValue(new WeakReference<>(current));
        }
        return current;
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
        activityId.remove();
    }
}


class ActivityId {
    private final UUID id;
    // UUID most/least significant bits and sequence, shared with the thread-local JDK-only holder.
    private final long[] state;
    // Cache the string since it gets frequently referenced.
    private String cachedToString;

    ActivityId(long[] state) {
        this.state = state;
        id = new UUID(state[0], state[1]);
    }

    UUID getId() {
        return id;
    }

    long getSequence() {
        // Edge case: A new thread re-uses an existing connection. Ensure sequence > 0.
        if (state[2] == 0L) {
            ++state[2];
        }

        return state[2];
    }

    ActivityId getIncrement() {
        cachedToString = null;
        if (state[2] < 0xffffffffl) // to get to 32-bit unsigned
        {
            ++state[2];
        } else {
            state[2] = 0;
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
