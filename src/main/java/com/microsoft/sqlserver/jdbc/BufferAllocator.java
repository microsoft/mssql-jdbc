/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.nio.ByteBuffer;

/**
 * Interface for allocating ByteBuffers with different strategies.
 * This allows for version-specific implementations without breaking compatibility.
 */
interface BufferAllocator {
    /**
     * Allocate a ByteBuffer of the specified size.
     * 
     * @param size
     *        The size of the buffer to allocate in bytes
     * @return A ByteBuffer ready for use
     */
    ByteBuffer allocate(int size);

    /**
     * Clean up any resources associated with this allocator.
     * Should be called when the allocator is no longer needed.
     */
    void cleanup();

    /**
     * Get a description of this allocator implementation.
     * Useful for logging and diagnostics.
     * 
     * @return A human-readable description of the allocator type
     */
    String getDescription();
}
