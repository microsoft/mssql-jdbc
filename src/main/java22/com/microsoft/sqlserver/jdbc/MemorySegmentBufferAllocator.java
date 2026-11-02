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
 * MemorySegment-based buffer allocator for Java 22+.
 * Uses the Foreign Function & Memory API (JEP 454) for off-heap native I/O.
 * Allocates native memory outside the JVM heap for improved performance.
 *
 * Benefits:
 * - Off-heap native memory allocation (avoids GC churn)
 * - Safe, deterministic lifecycle management via Arena
 * - Thread-confined Arena allocation for zero-synchronization overhead
 */
final class MemorySegmentBufferAllocator implements BufferAllocator {
    private Arena arena;

    /**
     * Allocate a MemorySegment-backed ByteBuffer.
     * Uses a thread-confined Arena for high-throughput single-thread packet processing.
     *
     * @param size The size of the buffer to allocate in bytes
     * @return A ByteBuffer backed by native memory with LITTLE_ENDIAN byte order
     */
    @Override
    public ByteBuffer allocate(int size) {
        if (arena == null) {
            arena = Arena.ofConfined();
        }

        // Allocate native memory (8-byte aligned)
        MemorySegment segment = arena.allocate(size, 8);

        // Wrap MemorySegment in a ByteBuffer for API compatibility
        return segment.asByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Clean up the Arena and all associated native memory.
     * Should be called when the allocator is no longer needed.
     */
    @Override
    public void cleanup() {
        if (arena != null) {
            arena.close();
            arena = null;
        }
    }

    /**
     * @return A description of this allocator type
     */
    @Override
    public String getDescription() {
        return "MemorySegmentBufferAllocator (Native Memory, Arena.ofConfined)";
    }
}
