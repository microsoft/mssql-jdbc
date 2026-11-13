/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class ActivityCorrelatorTest {
    @BeforeEach
    @AfterEach
    public void cleanup() {
        ActivityCorrelator.cleanupActivityId();
    }

    @Test
    public void testCurrentAndSequence() {
        ActivityId activity = ActivityCorrelator.getCurrent();
        UUID id = activity.getId();
        assertSame(activity, ActivityCorrelator.getCurrent());
        assertEquals(1L, activity.getSequence());
        assertEquals(id + "-1", activity.toString());
        assertSame(activity, ActivityCorrelator.getNext());
        assertEquals(2L, activity.getSequence());
        assertEquals(id + "-2", activity.toString());
    }

    @Test
    public void testNextInitializesFirstSequence() {
        assertEquals(1L, ActivityCorrelator.getNext().getSequence());
    }

    @Test
    public void testCleanupReinitializesActivity() {
        ActivityId previous = ActivityCorrelator.getNext();
        ActivityCorrelator.cleanupActivityId();
        ActivityCorrelator.cleanupActivityId();
        ActivityId current = ActivityCorrelator.getNext();
        assertNotSame(previous, current);
        assertNotEquals(previous.getId(), current.getId());
        assertEquals(1L, current.getSequence());
    }

    @Test
    public void testThreadsHaveIndependentActivitiesAndCleanup() throws Exception {
        ActivityId parent = ActivityCorrelator.getNext();
        FutureTask<Void> task = new FutureTask<>(() -> {
            try {
                ActivityId child = ActivityCorrelator.getNext();
                assertNotEquals(parent.getId(), child.getId());
                assertEquals(1L, child.getSequence());
                ActivityCorrelator.getNext();
                ActivityCorrelator.cleanupActivityId();
                assertNotSame(child, ActivityCorrelator.getCurrent());
                return null;
            } finally {
                ActivityCorrelator.cleanupActivityId();
            }
        });
        Thread worker = new Thread(task);
        worker.start();
        task.get(10, TimeUnit.SECONDS);
        worker.join(10000);
        assertFalse(worker.isAlive());
        assertSame(parent, ActivityCorrelator.getCurrent());
        assertEquals(1L, parent.getSequence());
    }

    @Test
    public void testReusedWorkerRetainsActivityUntilCleanup() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            UUID first = executor.submit(() -> ActivityCorrelator.getNext().getId()).get(10, TimeUnit.SECONDS);
            executor.submit(() -> {
                assertEquals(first, ActivityCorrelator.getNext().getId());
                assertEquals(2L, ActivityCorrelator.getCurrent().getSequence());
                ActivityCorrelator.cleanupActivityId();
            }).get(10, TimeUnit.SECONDS);
            UUID next = executor.submit(() -> ActivityCorrelator.getNext().getId()).get(10, TimeUnit.SECONDS);
            assertNotEquals(first, next);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testSequenceWraparound() throws Exception {
        ActivityId activity = ActivityCorrelator.getCurrent();
        Field sequence = ActivityId.class.getDeclaredField("sequence");
        sequence.setAccessible(true);
        sequence.setLong(activity, 0xffffffffL);
        assertEquals(activity.getId() + "-4294967295", activity.toString());
        assertSame(activity, ActivityCorrelator.getNext());
        assertEquals(activity.getId() + "-1", activity.toString());
    }

    @Test
    public void testTerminatedThreadsReleaseActivitiesWithoutCleanup() throws Exception {
        List<WeakReference<ActivityId>> activities = new ArrayList<>();
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            FutureTask<WeakReference<ActivityId>> task = new FutureTask<>(
                    () -> new WeakReference<>(ActivityCorrelator.getNext()));
            Thread worker = new Thread(task);
            workers.add(worker);
            worker.start();
            activities.add(task.get(10, TimeUnit.SECONDS));
            worker.join(10000);
            assertFalse(worker.isAlive());
        }
        ActivityCorrelator.cleanupActivityId();
        assertActivitiesCollected(activities);
        // Keep the terminated Thread objects reachable: their activity values must still be released.
        for (Thread worker : workers) {
            assertEquals(Thread.State.TERMINATED, worker.getState());
        }
    }

    static void assertActivitiesCollected(List<WeakReference<ActivityId>> activities) throws InterruptedException {
        // With explicit GC disabled, incidental young collections cannot establish that older activities leaked.
        List<String> vmArguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
        assumeTrue(
                vmArguments.lastIndexOf("-XX:+DisableExplicitGC") <= vmArguments.lastIndexOf("-XX:-DisableExplicitGC"),
                "Explicit GC is disabled; cannot reliably verify activity reclamation.");
        WeakReference<Object> control = new WeakReference<>(new Object());
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (activities.stream().anyMatch(reference -> reference.get() != null) && System.nanoTime() < deadline) {
            System.gc();
            Thread.sleep(50);
        }
        // System.gc() may be ignored; retained values only demonstrate a leak if collection was observed.
        assumeTrue(control.get() == null || activities.stream().anyMatch(reference -> reference.get() == null),
                "No garbage collection observed; cannot verify activity reclamation.");
        for (WeakReference<ActivityId> activity : activities) {
            assertNull(activity.get(), "Activity from a terminated thread must not remain strongly reachable.");
        }
    }
}
