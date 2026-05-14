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
 * Uses the Foreign Function & Memory API (JEP 454) for zero-copy I/O.
 * Allocates native memory outside the JVM heap for improved performance.
 * 
 * Benefits:
 * - Zero-copy I/O (native memory → network, no intermediate heap copy)
 * - Reduced GC pressure (off-heap allocation)
 * - No 2GB size limitation
 * - Better memory safety with Arena lifecycle management
 */
final class MemorySegmentBufferAllocator implements BufferAllocator {
    
    private Arena arena;

    /**
     * Allocate a MemorySegment-backed ByteBuffer.
     * Creates a confined Arena for thread-safe memory management.
     * 
     * @param size
     *        The size of the buffer to allocate in bytes
     * @return A ByteBuffer backed by native memory with LITTLE_ENDIAN byte order
     */
    @Override
    public ByteBuffer allocate(int size) {
        // Clean up previous arena if it exists
        if (arena != null) {
            arena.close();
        }
        
        // Create a new confined arena (thread-safe, auto-cleanup)
        arena = Arena.ofConfined();
        
        // Allocate native memory (8-byte aligned for optimal performance)
        MemorySegment segment = arena.allocate(size, 8);
        
        // Wrap MemorySegment in a ByteBuffer for API compatibility
        return segment.asByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Clean up the Arena and all associated native memory.
     * Should be called when reallocating or when the allocator is no longer needed.
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
        return "MemorySegmentBufferAllocator (Native Memory, Zero-Copy)";
    }
}
