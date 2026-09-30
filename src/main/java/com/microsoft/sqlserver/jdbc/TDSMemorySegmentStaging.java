/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Off-heap MemorySegment buffer for TDSWriter. The FFM API is resolved reflectively so the shared source remains
 * compatible with Java versions earlier than Java 22.
 */
final class TDSMemorySegmentStaging implements AutoCloseable {
    private static final int MINIMUM_SUPPORTED_JAVA_VERSION = 22;

    private final AutoCloseable arena;
    private final ByteBuffer stagingBuffer;
    private final ByteBuffer socketBuffer;

    TDSMemorySegmentStaging(int capacity) {
        try {
            Class<?> arenaClass = Class.forName("java.lang.foreign.Arena");
            Object arenaInstance = arenaClass.getMethod("ofShared").invoke(null);
            Class<?> memorySegmentClass = Class.forName("java.lang.foreign.MemorySegment");
            java.lang.reflect.Method allocateMethod = arenaClass.getMethod("allocate", long.class);
            java.lang.reflect.Method asByteBufferMethod = memorySegmentClass.getMethod("asByteBuffer");

            Object segment1 = allocateMethod.invoke(arenaInstance, (long) capacity);
            Object segment2 = allocateMethod.invoke(arenaInstance, (long) capacity);

            arena = (AutoCloseable) arenaInstance;
            stagingBuffer = ((ByteBuffer) asByteBufferMethod.invoke(segment1)).order(ByteOrder.LITTLE_ENDIAN);
            socketBuffer = ((ByteBuffer) asByteBufferMethod.invoke(segment2)).order(ByteOrder.LITTLE_ENDIAN);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Unable to allocate MemorySegment staging buffers.", e);
        }
    }

    static boolean isSupported() {
        String specificationVersion = Util.SYSTEM_SPEC_VERSION;
        if (null == specificationVersion) {
            return false;
        }

        int separator = specificationVersion.indexOf('.');
        String majorVersion = separator >= 0 ? specificationVersion.substring(separator + 1) : specificationVersion;
        try {
            return Integer.parseInt(majorVersion) >= MINIMUM_SUPPORTED_JAVA_VERSION;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    ByteBuffer buffer() {
        return stagingBuffer;
    }

    ByteBuffer stagingBuffer() {
        return stagingBuffer;
    }

    ByteBuffer socketBuffer() {
        return socketBuffer;
    }

    @Override
    public void close() {
        try {
            arena.close();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Unable to close the MemorySegment staging buffer.", e);
        }
    }
}
