/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * JDK 22+ only helper that backs a {@link ByteBuffer} with a {@code java.lang.foreign.MemorySegment}
 * (the Foreign Function &amp; Memory API, JEP 454), i.e. native/off-heap memory instead of the Java
 * heap.
 *
 * <p>
 * This class lives in a separate source root ({@code src/main/java22}) compiled with
 * {@code --release 22} only for build profiles whose JDK supports it (see the {@code jre25}/
 * {@code jre26} profiles in {@code pom.xml}). It is never referenced directly from the main
 * {@code src/main/java} source set - {@link MemoryUtil} loads it reflectively by name so that the
 * driver still compiles and runs unmodified on JDK 8-21, simply falling back to direct buffers
 * there.
 * </p>
 *
 * <p>
 * Each call allocates its own {@link Arena#ofAuto()}. An automatic arena ties the native memory's
 * lifetime to the reachability of the segment (reclaimed by the garbage collector), which mirrors
 * how JDK direct buffers already behave and avoids needing a new explicit close()/cleanup API on
 * {@code MemoryUtil} for what are, today, only a handful of long-lived per-connection buffers.
 * </p>
 */
final class MemorySegmentBufferAllocator {
    private MemorySegmentBufferAllocator() {
        // No instances - invoked reflectively as a static factory method.
    }

    /**
     * Allocates a {@code size}-byte native buffer and wraps it as a {@link ByteBuffer}.
     *
     * @param size
     *        capacity of the buffer, in bytes
     * @param order
     *        byte order to apply to the returned buffer
     * @return a {@link ByteBuffer} backed by an auto-managed {@link MemorySegment}
     */
    static ByteBuffer allocate(int size, ByteOrder order) {
        MemorySegment segment = Arena.ofAuto().allocate(size, 8);
        return segment.asByteBuffer().order(order);
    }
}
