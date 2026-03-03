/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Traditional heap-based ByteBuffer allocator.
 * Uses standard ByteBuffer.allocate() which allocates memory on the JVM heap.
 * Compatible with all Java versions (8+).
 */
final class HeapBufferAllocator implements BufferAllocator {
    
    /**
     * Allocate a heap-based ByteBuffer.
     * 
     * @param size
     *        The size of the buffer to allocate in bytes
     * @return A ByteBuffer allocated on the JVM heap with LITTLE_ENDIAN byte order
     */
    @Override
    public ByteBuffer allocate(int size) {
        return ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * No cleanup needed for heap-based buffers.
     * They are managed by the JVM garbage collector.
     */
    @Override
    public void cleanup() {
        // No resources to clean up for heap buffers
    }

    /**
     * @return A description of this allocator type
     */
    @Override
    public String getDescription() {
        return "HeapBufferAllocator (JVM Heap)";
    }
}
