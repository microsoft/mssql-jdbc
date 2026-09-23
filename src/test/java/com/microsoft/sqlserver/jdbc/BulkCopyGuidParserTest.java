/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;


public class BulkCopyGuidParserTest {
    private static final String GUID = "6f9619ff-8b86-d011-b42d-00c04fc964ff";

    @ParameterizedTest
    @MethodSource("acceptedRenderings")
    public void testSqlServerGuidRenderings(String value) {
        assertEquals(UUID.fromString(GUID), SQLServerBulkCopy.parseGuid(value, value.length()));
        assertEquals(UUID.fromString(GUID), SQLServerBulkCopy.parseGuid(value, 8000));
        assertThrows(IllegalArgumentException.class, () -> SQLServerBulkCopy.parseGuid(value, value.length() - 1));
    }

    private static Stream<String> acceptedRenderings() {
        return Stream.of(GUID, GUID.toUpperCase(), "{" + GUID + "}", GUID + "suffix", GUID + " ", GUID + "\t",
                GUID + "\r\n", GUID + "}", "{" + GUID + "}suffix", "{" + GUID + "} ");
    }

    @ParameterizedTest
    @MethodSource("rejectedRenderings")
    public void testRejectsMalformedGuidRenderings(String value) {
        assertThrows(IllegalArgumentException.class, () -> SQLServerBulkCopy.parseGuid(value, 8000));
    }

    private static Stream<String> rejectedRenderings() {
        return Stream.of("", "1-1-1-1-1", "6f9619ff-8b86-d011-b42d-1", GUID.substring(0, 35),
                "6f9619fff-8b86-d011-b42d-00c04fc964ff", "+f9619ff-8b86-d011-b42d-00c04fc964ff",
                "6g9619ff-8b86-d011-b42d-00c04fc964ff", GUID.replace('-', '_'), GUID.replace("-", ""), " " + GUID,
                "\t" + GUID, "\n" + GUID, "(" + GUID + ")", "urn:uuid:" + GUID, "{" + GUID, "{" + GUID + "x",
                "{" + GUID + " }", "{{" + GUID + "}}", "\u0666f9619ff-8b86-d011-b42d-00c04fc964ff",
                "\uff16f9619ff-8b86-d011-b42d-00c04fc964ff");
    }

    @Test
    public void testAllHexPositionsRequireAsciiHexDigits() {
        for (int i = 0; i < GUID.length(); i++) {
            if ('-' != GUID.charAt(i)) {
                String value = GUID.substring(0, i) + "g" + GUID.substring(i + 1);
                assertThrows(IllegalArgumentException.class, () -> SQLServerBulkCopy.parseGuid(value, 36));
            }
        }
    }

    @Test
    public void testGuidBitPatterns() {
        for (String value : new String[] {"00000000-0000-0000-0000-000000000000",
                "ffffffff-ffff-ffff-ffff-ffffffffffff", "00112233-4455-6677-8899-aabbccddeeff"}) {
            assertEquals(UUID.fromString(value), SQLServerBulkCopy.parseGuid(value, 36));
        }
    }
}
