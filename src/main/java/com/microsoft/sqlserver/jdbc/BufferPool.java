package com.microsoft.sqlserver.jdbc;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

public class BufferPool {
    private final int bufferSize;
    private final ConcurrentLinkedQueue<byte[]> pool = new ConcurrentLinkedQueue<>();

    BufferPool(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public byte[] rent() {
        byte[] buffer = pool.poll();
        return buffer == null ? new byte[bufferSize] : buffer;
    }

    public void release(byte[] buffer) {
        if (buffer.length == bufferSize) {
            pool.offer(buffer);
            Arrays.fill(buffer, (byte) 0); // Clear the array for re-use
        }
        // If the buffer size does not match, we do not store it in the pool
    }
}
