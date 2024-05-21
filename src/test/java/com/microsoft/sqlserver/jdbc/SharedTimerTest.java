package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;


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
            // 5000ms wait time for the AzureDB connection to close, need full test in the
            // test lab for the exact time
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        }

        assertFalse(SharedTimer.isRunning(), TestResource.getResource("R_sharedTimerStopOnNoRef"));
    }
}
