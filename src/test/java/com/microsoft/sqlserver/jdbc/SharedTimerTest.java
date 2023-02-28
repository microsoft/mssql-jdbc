package com.microsoft.sqlserver.jdbc;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class SharedTimerTest {

    @Test
    void getTimer() throws InterruptedException, ExecutionException, TimeoutException {
        var iterations = 500;

        var futures = new ArrayList<CompletableFuture<?>>(iterations);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            for (int i = 0; i < iterations; i++) {
                var cf = CompletableFuture.runAsync(() -> {
                    var t = SharedTimer.getTimer();
                    try {
                        assertTrue(SharedTimer.isRunning());
                    } finally {
                        t.removeRef();
                    }
                }, executor);
                futures.add(cf);
            }
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(2, TimeUnit.MINUTES);
    }
}