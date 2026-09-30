/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.microsoft.sqlserver.testframework.AbstractTest;

public class JMHBenchmarkRunnerTest extends AbstractTest {

    @Test
    @DisplayName("Run JMH Benchmark with GCProfiler")
    public void executeJmhBenchmark() throws Exception {
        System.setProperty("jmh.jdbcUrl", connectionString);

        Options opt = new OptionsBuilder()
                .include(SQLServerBulkCopyJMHBenchmark.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .forks(1)
                .warmupIterations(1)
                .measurementIterations(2)
                .jvmArgs("-Djmh.jdbcUrl=" + connectionString, "-Xms1g", "-Xmx1g")
                .build();

        new Runner(opt).run();
    }
}
