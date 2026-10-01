/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Single, central entry point for every buffer/array allocation used by the TDS I/O layer
 * ({@link IOBuffer}).
 *
 * <p>
 * Small, short-lived scratch allocations ({@link #newArray}, {@link #newCharArray},
 * {@link #newByteBuffer}) always come from the Java heap - that is by far the cheapest strategy
 * for values that live for a handful of method calls, and is unaffected by {@link #CURRENT}.
 * Only the few large, per-connection buffers that back the TDS socket channel
 * ({@code TDSWriter}'s {@code socketBuffer}/{@code stagingBuffer}/{@code logBuffer} and
 * {@code cachedTVPHeaders}) go through {@link #newChannelBuffer}, which is strategy-aware. This
 * mirrors what earlier benchmarking of this idea found: allocating every small encode/decode
 * scratch buffer off-heap is a net loss (direct/MemorySegment allocation has real per-call
 * overhead that heap allocation + escape analysis does not), so the strategy switch is scoped
 * only to the handful of buffers that are large and live for the lifetime of a connection/message.
 * </p>
 *
 * <p>
 * <b>Scope and limitations:</b> this class only covers {@code byte[]}/{@code char[]}/
 * {@link ByteBuffer} allocations in the wire I/O and value-conversion layers (see
 * {@code IOBuffer}, {@code DDC}, {@code dtv}, {@code Util}). It does <i>not</i>, and cannot,
 * cover the autoboxing of primitive values (e.g. {@code Integer}, {@code Long}, {@code Double})
 * that occurs when column/parameter values are stored in {@code DTVImpl}'s {@code Object value}
 * field to satisfy the JDBC {@code getObject()}/{@code setObject()} contract. Boxed wrapper
 * objects are always ordinary Java heap objects - there is no off-heap representation for them
 * in the current JDK - so no allocation strategy selected here can reduce that cost. In practice
 * the JVM's mandatory autobox caching (JLS 5.1.7) already avoids a real allocation for small
 * {@code Integer}/{@code Long}/{@code Short}/{@code Boolean} values; {@code double}/{@code float}
 * values (via {@code getDouble}/{@code getFloat}/{@code setDouble}/{@code setFloat}) are never
 * cached and always allocate. Eliminating that residual cost would require duplicating the
 * Always Encrypted/PLP-streaming conversion pipeline in {@code ServerDTVImpl.getValue()} into
 * primitive-returning variants, which is a separate, higher-risk effort and out of scope here.
 * </p>
 */
final class MemoryUtil {
    private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.MemoryUtil");

    private static final String MEMORY_SEGMENT_ALLOCATOR_CLASS = "com.microsoft.sqlserver.jdbc.MemorySegmentBufferAllocator";

    /**
     * Buffer allocation strategy for the large, per-connection TDS channel buffers. Selected once
     * per process from the {@code mssql.jdbc.bufferMode} system property (values: {@code heap}
     * (default), {@code direct}, {@code memory_segment}; the legacy boolean spelling
     * {@code mssql.jdbc.useMemorySegment=true/false} is also accepted for compatibility).
     */
    enum Strategy {
        /** Heap byte[]-backed ByteBuffer. Always available, the long-standing default. */
        HEAP,

        /** {@code ByteBuffer.allocateDirect(...)}. Available on every JDK version this driver supports. */
        DIRECT,

        /**
         * {@code java.lang.foreign.MemorySegment}-backed buffer (JDK 22+ only, via reflection - see
         * {@code src/main/java22}). Falls back to {@link #DIRECT} transparently when the runtime JDK
         * or build doesn't have the Foreign Function &amp; Memory API available.
         */
        MEMORY_SEGMENT;

        static Strategy fromString(String value) {
            if (null == value || value.isEmpty()) {
                return HEAP;
            }
            if ("true".equalsIgnoreCase(value)) {
                return MEMORY_SEGMENT;
            }
            if ("false".equalsIgnoreCase(value)) {
                return HEAP;
            }
            try {
                return Strategy.valueOf(value.toUpperCase(java.util.Locale.ROOT));
            } catch (IllegalArgumentException e) {
                return HEAP;
            }
        }
    }

    /**
     * Process-wide effective strategy, parsed once. {@code IOBuffer}'s {@code TDSWriter} reads this
     * single value rather than re-parsing system properties on every connection.
     */
    static final Strategy CURRENT = Strategy
            .fromString(System.getProperty("mssql.jdbc.bufferMode", System.getProperty("mssql.jdbc.useMemorySegment")));

    // Cached once resolution has been attempted so that repeated allocation calls don't pay for a
    // failed Class.forName()/reflection lookup over and over on JDKs without MemorySegment support.
    private static volatile Method memorySegmentAllocateMethod;
    private static volatile boolean memorySegmentUnavailable;

    private MemoryUtil() {
        // No instances.
    }

    /**
     * Allocates a new, zero-initialized heap byte array.
     *
     * @param size
     *        number of bytes to allocate
     * @return a new {@code byte[size]}
     */
    static byte[] newArray(int size) {
        return new byte[size];
    }

    /**
     * Allocates a new, zero-initialized heap char array.
     *
     * @param size
     *        number of chars to allocate
     * @return a new {@code char[size]}
     */
    static char[] newCharArray(int size) {
        return new char[size];
    }

    /**
     * Allocates a heap-backed {@link ByteBuffer} of the given size and byte order. Always heap -
     * intended for small, short-lived encode/decode scratch buffers, not for the large per-connection
     * TDS channel buffers (use {@link #newChannelBuffer} for those).
     *
     * @param size
     *        capacity of the buffer, in bytes
     * @param order
     *        byte order to apply to the returned buffer
     * @return a new heap {@link ByteBuffer}
     */
    static ByteBuffer newByteBuffer(int size, ByteOrder order) {
        return ByteBuffer.allocate(size).order(order);
    }

    /**
     * Allocates a {@link ByteBuffer} for one of the large, per-connection TDS channel buffers
     * (socket/staging/log buffers, cached TVP headers), honoring {@link #CURRENT}.
     *
     * @param size
     *        capacity of the buffer, in bytes
     * @param order
     *        byte order to apply to the returned buffer
     * @return a new {@link ByteBuffer} allocated per the current {@link Strategy}
     */
    static ByteBuffer newChannelBuffer(int size, ByteOrder order) {
        if (Strategy.MEMORY_SEGMENT == CURRENT) {
            ByteBuffer segmentBacked = tryAllocateMemorySegment(size, order);
            if (null != segmentBacked) {
                return segmentBacked;
            }
            // MemorySegment unavailable on this JDK/build - direct buffers were found to perform
            // close to MemorySegment for the write path in earlier benchmarking, so fall back there
            // instead of silently dropping all the way back to heap.
        }
        if (Strategy.DIRECT == CURRENT || Strategy.MEMORY_SEGMENT == CURRENT) {
            return ByteBuffer.allocateDirect(size).order(order);
        }
        return ByteBuffer.allocate(size).order(order);
    }

    private static ByteBuffer tryAllocateMemorySegment(int size, ByteOrder order) {
        if (memorySegmentUnavailable) {
            return null;
        }
        try {
            Method allocate = memorySegmentAllocateMethod;
            if (null == allocate) {
                Class<?> allocatorClass = Class.forName(MEMORY_SEGMENT_ALLOCATOR_CLASS);
                allocate = allocatorClass.getDeclaredMethod("allocate", int.class, ByteOrder.class);
                memorySegmentAllocateMethod = allocate;
            }
            return (ByteBuffer) allocate.invoke(null, size, order);
        } catch (ReflectiveOperationException | LinkageError e) {
            memorySegmentUnavailable = true;
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE,
                        "MemorySegment buffer allocation unavailable (requires JDK 22+ build); falling back to direct buffers.",
                        e);
            }
            return null;
        }
    }
}
