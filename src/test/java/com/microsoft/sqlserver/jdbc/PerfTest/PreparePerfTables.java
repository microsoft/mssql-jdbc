package com.microsoft.sqlserver.jdbc.PerfTest;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;


/**
 * Prepare PERF_MULTIPLIER, and PERF_TABLE
 */
public class PreparePerfTables {
    private final static int MULTIPLIER_MAX = 819_200;

    private final static String SELECT_PERF_MULTIPLIER_COUNT = "IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PERF_MULTIPLIER]')) SELECT COUNT(*) FROM [dbo].[PERF_MULTIPLIER] ELSE SELECT 0";
    private final static String INSERT_INTO_PERF_MULTIPLIER = "INSERT INTO [dbo].[PERF_MULTIPLIER] VALUES (?)";
    private final static String INSERT_INTO_PERF_MULTIPLIER_SELECT_ADD = "INSERT INTO [dbo].[PERF_MULTIPLIER] SELECT ID + ? FROM [dbo].[PERF_MULTIPLIER]";
    private final static String DROP_PERF_MULTIPLIER = "DROP TABLE IF EXISTS [dbo].[PERF_MULTIPLIER]";
    private final static String CREATE_PERF_MULTIPLIER = "CREATE TABLE [dbo].[PERF_MULTIPLIER] ( ID int NOT NULL PRIMARY KEY )";

    final static int COLUMN_REPEAT_COUNT = 100;
    final static int ROW_COUNT = 1000;

    private final static String SELECT_PERF_TABLE_COUNT = "IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PERF_TABLE]')) SELECT COUNT(*) FROM [dbo].[PERF_TABLE] ELSE SELECT 0";
    private final static String DROP_PERF_TABLE = "DROP TABLE IF EXISTS [dbo].[PERF_TABLE]";
    private final static String CREATE_PERF_TABLE = """
            CREATE TABLE [dbo].[PERF_TABLE](
            	[ID] int NOT NULL PRIMARY KEY,
            """;

    private final static String INSERT_INTO_PERF_TABLE = "INSERT INTO [dbo].[PERF_TABLE] VALUES (?";

    public static void preparePerfMultiplier(Connection conn) throws SQLException {
        try (PreparedStatement preparedStatement = conn.prepareStatement(SELECT_PERF_MULTIPLIER_COUNT);
                ResultSet rs = preparedStatement.executeQuery()) {
            while (rs.next()) {
                if (rs.getInt(1) == MULTIPLIER_MAX) {
                    return;
                }
            }
        }

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(DROP_PERF_MULTIPLIER);
        }

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(CREATE_PERF_MULTIPLIER);
        }
        try (PreparedStatement preparedStatement = conn.prepareStatement(INSERT_INTO_PERF_MULTIPLIER)) {
            for (int i = 1; i <= 100; i++) {
                preparedStatement.setInt(1, i);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        }
        try (PreparedStatement preparedStatement = conn.prepareStatement(INSERT_INTO_PERF_MULTIPLIER_SELECT_ADD)) {
            for (int i = 100; i < MULTIPLIER_MAX; i *= 2) {
                preparedStatement.setInt(1, i);
                preparedStatement.execute();
            }
        }
    }

    public static void preparePerfTable(Connection conn) throws SQLException {
        try (PreparedStatement preparedStatement = conn.prepareStatement(SELECT_PERF_TABLE_COUNT);
                ResultSet rs = preparedStatement.executeQuery()) {
            while (rs.next()) {
                if (rs.getInt(1) == ROW_COUNT) {
                    return;
                }
            }
        }

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(DROP_PERF_TABLE);
        }
        StringBuilder createTableSql = new StringBuilder(CREATE_PERF_TABLE);
        StringBuilder insertSql = new StringBuilder(INSERT_INTO_PERF_TABLE);
        for (int i = 1; i <= COLUMN_REPEAT_COUNT; i++) {
            createTableSql.append("[COL_IND_").append(i).append("] char(1) NULL,\n");
            createTableSql.append("[COL_STRING_").append(i).append("] varchar(64) NULL,\n");
            createTableSql.append("[COL_LONG_").append(i).append("] decimal(18,0) NULL,\n");
            createTableSql.append("[COL_FLOAT_").append(i).append("] float NULL,\n");
            createTableSql.append("[COL_DECIMAL_").append(i).append("] decimal(25,4) NULL,\n");
            createTableSql.append("[COL_TIMESTAMP_").append(i).append("] datetime NULL,\n");
            insertSql.append(",?,?,?,?,?,?");
        }
        createTableSql.append(")");
        insertSql.append(")");
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSql.toString());
        }
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long timeEnd = System.currentTimeMillis();
        long timeStart = timeEnd - 365 * 86400;
        try (PreparedStatement stmt = conn.prepareStatement(insertSql.toString())) {
            for (int rowIndex = 0; rowIndex < ROW_COUNT; rowIndex++) {
                stmt.setInt(1, rowIndex + 1);
                int columnIndex = 2;
                for (int i = 1; i <= COLUMN_REPEAT_COUNT; i++) {
                    boolean nulls = random.nextBoolean();
                    if (nulls) {
                        stmt.setString(columnIndex++, null);
                        stmt.setString(columnIndex++, null);
                        stmt.setNull(columnIndex++, Types.BIGINT);
                        stmt.setNull(columnIndex++, Types.FLOAT);
                        stmt.setBigDecimal(columnIndex++, null);
                        stmt.setTimestamp(columnIndex++, null);
                    } else {
                        stmt.setString(columnIndex++, random.nextBoolean() ? "Y" : "N");
                        stmt.setString(columnIndex++, UUID.randomUUID().toString());
                        stmt.setLong(columnIndex++, random.nextLong(1, 1_000_000_000_000L));
                        final double doubleValue = random.nextDouble(1.0d, 1_000_000_000.0d);
                        stmt.setDouble(columnIndex++, doubleValue);
                        stmt.setBigDecimal(columnIndex++,
                                BigDecimal.valueOf(doubleValue).setScale(4, RoundingMode.HALF_UP));
                        stmt.setTimestamp(columnIndex++, new Timestamp(random.nextLong(timeStart, timeEnd)));
                    }
                }
                stmt.execute();
            }
        }
    }
}
