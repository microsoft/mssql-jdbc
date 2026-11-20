/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.ref.WeakReference;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class ActivityIDTest extends AbstractTest {

    @Test
    public void testOpenOnShortLivedThreadAndCloseOnAnotherThread() throws Exception {
        List<WeakReference<ActivityId>> activities = new ArrayList<>();
        try {
            for (int i = 0; i < 10; i++) {
                FutureTask<SQLServerConnection> task = new FutureTask<>(() -> {
                    SQLServerConnection con = getConnection();
                    activities.add(new WeakReference<>(ActivityCorrelator.getCurrent()));
                    return con;
                });
                Thread opener = new Thread(task, "activity-test-connection-adder");
                opener.start();
                try (SQLServerConnection con = task.get(60, TimeUnit.SECONDS)) {
                    opener.join(10000);
                    assertFalse("Connection-opening thread should have terminated.", opener.isAlive());
                    try (Statement stmt = con.createStatement(); ResultSet rs = stmt.executeQuery("SELECT 1")) {
                        assertTrue(rs.next());
                        assertEquals(1, rs.getInt(1));
                    }
                }
            }
            ActivityCorrelatorTest.assertActivitiesCollected(activities);
        } finally {
            ActivityCorrelator.cleanupActivityId();
        }
    }

    @Test
    public void testActivityID() throws Exception {
        int numThreads = 20;
        List<UUID> usedIds = new ArrayList<UUID>(numThreads);

        for (int i = 0; i < numThreads; i++) {
            MyThread t = new MyThread(usedIds);
            t.start();
            t.join();
            usedIds.add(t.getUUID());
        }
    }

    public class MyThread extends Thread {

        public MyThread(List<UUID> usedIdsIn) {
            usedIds = usedIdsIn;
        }

        private List<UUID> usedIds;
        private ActivityId id;

        public UUID getUUID() {
            return id.getId();
        }

        public void run() {
            id = ActivityCorrelator.getCurrent();
            UUID uuid = id.getId();
            assertFalse("UUID should be unique across threads.", usedIds.contains(id.getId()));
            assertEquals(1L, id.getSequence(), "First sequence should be 1.");
            id = ActivityCorrelator.getNext();
            assertEquals(uuid, id.getId(), "UUID should remain the same for the same thread.");
            assertEquals(2L, id.getSequence(), "Second sequence should be 2.");
            id = ActivityCorrelator.getNext();
            assertEquals(3L, id.getSequence(), "Third sequence should be 3.");
            assertEquals(uuid, id.getId(), "UUID should remain the same for the same thread.");
        }
    }
}
