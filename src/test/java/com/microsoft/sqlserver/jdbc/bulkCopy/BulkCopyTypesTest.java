package com.microsoft.sqlserver.jdbc.bulkCopy;

import com.microsoft.sqlserver.jdbc.ComparisonUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.*;
import com.microsoft.sqlserver.testframework.sqlType.SqlDateTime2;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@RunWith(JUnitPlatform.class)
public class BulkCopyTypesTest extends AbstractTest {

    private static DBTable tableSrc = null;
    private static DBTable tableDest = null;

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    @Test
    void bulkCopy_datetime2Nullable_works() throws SQLException {
        setupVariation();

        try (Connection connnection = PrepUtil
                .getConnection(connectionString);
             Statement statement = connnection.createStatement();
             ResultSet rs = statement.executeQuery("select * from " + tableSrc.getEscapedTableName())) {

            SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connection);
            bcOperation.setDestinationTableName(tableDest.getEscapedTableName());
            bcOperation.writeToServer(rs);
            bcOperation.close();

            ComparisonUtil.compareSrcTableAndDestTableIgnoreRowOrder(new DBConnection(connection), tableSrc, tableDest);
        } finally {
            terminateVariation();
        }
    }

    private void setupVariation() throws SQLException {
        try (DBConnection dbConnection = new DBConnection(connectionString);
             DBStatement dbStmt = dbConnection.createStatement()) {

            tableSrc = new DBTable(false);
            tableSrc.addColumn(new SqlDateTime2());
            tableSrc.addColumn(new SqlDateTime2());
            tableSrc.addColumn(new SqlDateTime2());
            tableSrc.addColumn(new SqlDateTime2());
            tableDest = tableSrc.cloneSchema();

            // needs manual table construction to add NOT NULL to column
            dbStmt.execute(String.format("CREATE TABLE %s(%s DATETIME2 NOT NULL, %s DATETIME2 NULL, %s DATETIME2 NULL, %s DATETIME2 NULL)", tableSrc.getEscapedTableName(), tableSrc.getEscapedColumnName(0), tableSrc.getEscapedColumnName(1), tableSrc.getEscapedColumnName(2), tableSrc.getEscapedColumnName(3)));
            dbStmt.execute(String.format("CREATE TABLE %s(%s DATETIME2 NOT NULL, %s DATETIME2 NULL, %s DATETIME2 NULL, %s DATETIME2 NULL)", tableDest.getEscapedTableName(), tableDest.getEscapedColumnName(0), tableDest.getEscapedColumnName(1), tableDest.getEscapedColumnName(2), tableDest.getEscapedColumnName(3)));
            dbStmt.execute(String.format("INSERT INTO %s VALUES ('2022-01-01 00:00:00.123', NULL, NULL, NULL)", tableSrc.getEscapedTableName()));
            dbStmt.execute(String.format("INSERT INTO %s VALUES ('2022-01-02 01:00:00.123', '2022-02-01 00:00:00.123', '2022-03-01 00:00:00.123', '2022-04-01 00:00:00.123')", tableSrc.getEscapedTableName()));
            dbStmt.execute(String.format("INSERT INTO %s VALUES ('2022-01-03 02:00:00.123', NULL, '2022-05-01 00:00:00.123', '2022-06-01 00:00:00.123')", tableSrc.getEscapedTableName()));
        }
    }

    private void terminateVariation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableSrc.getEscapedTableName(), stmt);
            TestUtils.dropTableIfExists(tableDest.getEscapedTableName(), stmt);
        }
    }
}
