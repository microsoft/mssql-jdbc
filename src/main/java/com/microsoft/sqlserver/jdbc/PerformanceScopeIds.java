/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.util.concurrent.atomic.AtomicLong;

/** Shared process-local allocation for connection and statement telemetry scope IDs. */
final class PerformanceScopeIds {
    private static final AtomicLong IDS = new AtomicLong();

    private PerformanceScopeIds() {}

    static long next() {
        return IDS.incrementAndGet();
    }
}
