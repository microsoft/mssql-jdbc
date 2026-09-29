/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class MemorySegmentStagingTest {
    @Test
    void memorySegmentIsTheDefaultBulkCopyStagingBuffer() {
        SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();

        assertTrue(options.isUseMemorySegment());
        options.setUseMemorySegment(false);
        assertFalse(options.isUseMemorySegment());
    }

    @Test
    void memorySegmentProvidesLittleEndianDirectBuffer() {
        Assumptions.assumeTrue(TDSMemorySegmentStaging.isSupported());

        try (TDSMemorySegmentStaging staging = new TDSMemorySegmentStaging(Integer.BYTES)) {
            ByteBuffer buffer = staging.buffer();
            buffer.putInt(0x01020304);

            assertTrue(buffer.isDirect());
            assertEquals((byte) 0x04, buffer.get(0));
            assertEquals(0x01020304, buffer.getInt(0));
        }
    }
}
