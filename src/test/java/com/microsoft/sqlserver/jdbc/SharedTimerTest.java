package com.microsoft.sqlserver.jdbc;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.concurrent.*;


class SharedTimerTest {

    @Test
    void getTimer() throws InterruptedException, ExecutionException, TimeoutException {
        final int iterations = 500;

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            ArrayList<CompletableFuture<?>> futures = new ArrayList<>(iterations);
            for (int i = 0; i < iterations; i++) {
                futures.add(CompletableFuture.runAsync(() -> SharedTimer.getTimer().removeRef(), executor));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(2, TimeUnit.MINUTES);
        } finally {
            executor.shutdown();
        }
    }
}
