package com.microsoft.sqlserver.jdbc;

import microsoft.sql.DateTimeOffset;

import java.sql.*;
import java.time.*;

public class TestClass {

    public static void main(String[] args) throws Exception {
        missingScaleCheck();
        //Two221();
        //osgiIBMTest();
        //testDateTime();
    }

    public static void testDateTime() throws Exception {
        Instant date = LocalDateTime.of(2023, 9, 18, 13, 50).toInstant(ZoneOffset.ofHours(-7));
        OffsetDateTime odt = OffsetDateTime.of(2023, 9, 18, 13, 50, 0, 0, ZoneOffset.ofHours(-7));
        DateTimeOffset dto = DateTimeOffset.valueOf(Timestamp.valueOf(LocalDateTime.of(2023, 9, 18, 13, 50)), 0);
        String table = "test_table";
        String tableType = "datetimeoffset";

        try (Connection conn = DriverManager.getConnection("jdbc:sqlserver://localhost:1433;DatabaseName=TestDb;encrypt=false;trustServerCertificate=true"
                , "sa", "TestPassword123")) {
            conn.setAutoCommit(true);
            // DDL
            try (Statement statement = conn.createStatement()) {
                TestUtils.dropTableIfExists(table,statement);
                statement.execute("create table " + table + " (test_date " + tableType + ")");
            }
            // INSERT
            try (SQLServerPreparedStatement preparedStatement = (SQLServerPreparedStatement) conn.prepareStatement("insert into " + table + " values (?)")) {
                //preparedStatement.setTimestamp(1, Timestamp.from(date));
                //preparedStatement.setDateTimeOffset(1, dto);
                preparedStatement.setObject(1,odt);
                preparedStatement.execute();
                //System.out.println("timestamp represents LOCAL DATE on the one hand: " + Timestamp.from(date).toString());
                //System.out.println("timestamp represents ~INSTANT on the other hand: " + Timestamp.from(date).getTime()
                //+ " - " + date.toEpochMilli());
            }
            // FETCH
            try (Statement statement = conn.createStatement()) {
                try (ResultSet rs = statement.executeQuery("select * from " + table)) {
                    if (rs.next()) {
                        Timestamp received = rs.getTimestamp(1);
                        System.out.println("ZoneId: " + ZoneId.systemDefault());
                        System.out.println("ORIGIN: " + odt.toString());
                        System.out.println("FETCHED: " + received.toInstant());
                    } else {
                        throw new RuntimeException("TLD");
                    }
                }
            }
        }
    }

    public static void osgiIBMTest() throws Exception {
        Class<?> clazz = null;
        try {
            clazz = Class.forName("com.ibm.lang.management.MemoryUsage");
            // Above causes problem in OSGi environment, maven-bundle-plugin constructs a mandatory import for it
            // Can't just catch the exception, it literally forces OSGi environments to a halt
        } catch (ClassNotFoundException e) {
            //We're using the try-catch to test for IBM jdk, no need to handle exception.
        }
    }

    public static void Two221() throws Exception {

        String hostname = "localhost";
        String username = "sa";
        String password = "TestPassword123";
        String databaseName = "TestDb";
        try (Connection connection = DriverManager.getConnection(
                "jdbc:sqlserver://" + hostname + ":1433;" + "userName=" + username + ";" + "password=" + password +
                        ";" + "database=" + databaseName + ";" + "encrypt=false;" + "useBulkCopyForBatchInsert=true;")) {
            try (Statement statement = connection.createStatement()) {
                TestUtils.dropTableIfExists("TestTableWithIdentity", statement);
                statement.executeUpdate(
                        "CREATE TABLE TestTableWithIdentity (" +
                                "    Id INT NOT NULL IDENTITY, " +
                                "    SomeText VARCHAR(50) NOT NULL" +
                                ")\n");
            }

            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(
                        "SET IDENTITY_INSERT TestTableWithIdentity ON\n");
            }

            try (PreparedStatement pStatement = connection.prepareStatement(
                    """
                            INSERT INTO TestTableWithIdentity (
                                [Id],
                                SomeText
                            )
                            VALUES (
                                5555,
                                ?
                            )
                            """)) {
                //pStatement.setInt(1, 5555);
                pStatement.setString(1, "text for a row 5555");

                pStatement.addBatch();
                pStatement.executeBatch();
            }
        }
    }

    public static void missingScaleCheck() throws Exception {

    }
}
