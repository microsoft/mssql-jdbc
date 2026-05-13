/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;

/**
 * Trusted CMK path tests: trusted key path validation, untrusted path rejection,
 * server-scoped trusted path configuration.
 * Ported from FX ae/TrustedCMKPath.java tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxAE)
@Tag(Constants.reqExternalSetup)
public class TrustedKeyPathTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @Test
    public void testSetTrustedMasterKeyPaths() throws SQLException {
        Map<String, List<String>> trustedKeyPaths = new HashMap<>();
        List<String> paths = new ArrayList<>();
        paths.add("testPath1");
        paths.add("testPath2");
        trustedKeyPaths.put("testServer", paths);

        SQLServerConnection.setColumnEncryptionTrustedMasterKeyPaths(trustedKeyPaths);

        Map<String, List<String>> retrievedPaths = SQLServerConnection
                .getColumnEncryptionTrustedMasterKeyPaths();
        assertNotNull(retrievedPaths);
        assertTrue(retrievedPaths.containsKey("testServer"));
        assertTrue(retrievedPaths.get("testServer").contains("testPath1"));
        assertTrue(retrievedPaths.get("testServer").contains("testPath2"));

        // Cleanup
        SQLServerConnection.setColumnEncryptionTrustedMasterKeyPaths(new HashMap<>());
    }

    @Test
    public void testEmptyTrustedKeyPaths() throws SQLException {
        Map<String, List<String>> emptyPaths = new HashMap<>();
        SQLServerConnection.setColumnEncryptionTrustedMasterKeyPaths(emptyPaths);

        Map<String, List<String>> retrievedPaths = SQLServerConnection
                .getColumnEncryptionTrustedMasterKeyPaths();
        assertNotNull(retrievedPaths);
        assertTrue(retrievedPaths.isEmpty());
    }

    @Test
    public void testOverwriteTrustedKeyPaths() throws SQLException {
        Map<String, List<String>> paths1 = new HashMap<>();
        List<String> list1 = new ArrayList<>();
        list1.add("pathA");
        paths1.put("server1", list1);
        SQLServerConnection.setColumnEncryptionTrustedMasterKeyPaths(paths1);

        // Overwrite with new paths
        Map<String, List<String>> paths2 = new HashMap<>();
        List<String> list2 = new ArrayList<>();
        list2.add("pathB");
        paths2.put("server2", list2);
        SQLServerConnection.setColumnEncryptionTrustedMasterKeyPaths(paths2);

        Map<String, List<String>> retrievedPaths = SQLServerConnection
                .getColumnEncryptionTrustedMasterKeyPaths();
        assertTrue(retrievedPaths.containsKey("server2"));
        assertFalse(retrievedPaths.containsKey("server1"), "server1 should no longer be present after full overwrite");

        // Cleanup
        SQLServerConnection.setColumnEncryptionTrustedMasterKeyPaths(new HashMap<>());
    }

    @Test
    public void testMultipleServersInTrustedPaths() throws SQLException {
        Map<String, List<String>> paths = new HashMap<>();
        List<String> list1 = new ArrayList<>();
        list1.add("path1");
        paths.put("server1", list1);

        List<String> list2 = new ArrayList<>();
        list2.add("path2");
        list2.add("path3");
        paths.put("server2", list2);

        SQLServerConnection.setColumnEncryptionTrustedMasterKeyPaths(paths);

        Map<String, List<String>> retrievedPaths = SQLServerConnection
                .getColumnEncryptionTrustedMasterKeyPaths();
        assertTrue(retrievedPaths.containsKey("server1"));
        assertTrue(retrievedPaths.containsKey("server2"));
        assertTrue(retrievedPaths.get("server2").size() == 2);

        // Cleanup
        SQLServerConnection.setColumnEncryptionTrustedMasterKeyPaths(new HashMap<>());
    }
}
