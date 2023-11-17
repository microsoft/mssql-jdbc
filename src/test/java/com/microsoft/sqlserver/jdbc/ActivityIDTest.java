/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class ActivityIDTest extends AbstractTest {

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
            assertFalse(usedIds.contains(id.getId()));
            assertEquals(1L, id.getSequence());
            id = ActivityCorrelator.getNext();
            assertEquals(2L, id.getSequence());
            id = ActivityCorrelator.getNext();
            assertEquals(3L, id.getSequence());
        }
    }
}
