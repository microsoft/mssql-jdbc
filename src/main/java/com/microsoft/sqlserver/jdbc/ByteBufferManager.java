package com.microsoft.sqlserver.jdbc;

import java.util.concurrent.ConcurrentHashMap;

public class ByteBufferManager {

    private static int[] poolSizes = { 1024, 2048, 4096, 8192, 16384, 32768, 65536 };
    private final static ConcurrentHashMap<Integer, BufferPool> pools = new ConcurrentHashMap<>();

    public ByteBufferManager() {
        for (int size : poolSizes) {
            pools.put(size, new BufferPool(size));
        }
    }
 
    public byte[] rentBytes(int size) {
        int poolSize = getPoolSize(size);

        if (poolSize == -1) {
            return new byte[size];
        }
        return pools.get(poolSize).rent();
    }

    private static int getPoolSize(int size) {
        if (size <= 0) return 1024;

        int nextPower = Integer.highestOneBit(size - 1) << 1;
        return nextPower <= 65536 ? nextPower : -1;
    }

    // Return a previously rented byte array
    public void release(byte[] array) {
        int size = array.length;
        BufferPool pool = pools.get(size);
        if (pool != null) {
            pool.release(array);
        }
        // else discard — no pooling for this size
    }
}