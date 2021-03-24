
import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class BulkInsertTest {

    private static String tempTable = "dbo.foo";
    private static String serverName = "VANLTM03";
    private static String databaseName = "testdb";
    private static String userName = "sa";
    private static String password = "Moonshine4me";

    private static Connection getConnection() throws SQLException {
        Properties connectionProps = new Properties();
        connectionProps.put("user", userName);
        connectionProps.put("password", password);
        String url = "jdbc:sqlserver://" + serverName +
                ";databaseName=" + databaseName +
                ";integratedSecurity=false";

        return DriverManager.getConnection(url, connectionProps);
    }

    private static class BulkRecord implements ISQLServerBulkData {
        boolean anyMoreData = true;
        Object data;

        BulkRecord(Object data) {
            this.data = data;
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            Set<Integer> ords = new HashSet<>();
            ords.add(1);
            return ords;
        }

        @Override
        public String getColumnName(int column) {
            return "testCol";
        }

        @Override
        public int getColumnType(int column) {
            return java.sql.Types.TIMESTAMP;
        }

        @Override
        public int getPrecision(int column) {
            return 0;
        }

        @Override
        public int getScale(int column) {
            return 7;
        }

        @Override
        public Object[] getRowData() {
            return new Object[]{ data };
        }

        @Override
        public boolean next() {
            if (!anyMoreData)
                return false;
            anyMoreData = false;
            return true;
        }
    }

    private static Object createWithNano(int nanos) {
        return LocalDateTime.of(2020, 1, 1, 12, 34, 56, nanos);
    }

    private static void writeData(Object data) throws SQLException {
        try (Connection conn = getConnection()) {
            final SQLServerConnection sqlServerConnection = conn.unwrap(SQLServerConnection.class);

            try (final SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(sqlServerConnection)) {
                bulkCopy.setDestinationTableName(tempTable);
                bulkCopy.writeToServer(new BulkRecord(data));
            }
        }
    }

    public static void main(String... args) throws SQLException {
          writeData(createWithNano(0)); // passes
          writeData(createWithNano(100000000)); // passes
          writeData(createWithNano(120000000)); // passes
          writeData(createWithNano(123000000)); // passes
          writeData(createWithNano(123400000)); // passes
          writeData(createWithNano(123450000)); // passes
          writeData(createWithNano(123456000)); // passes
          writeData(createWithNano(123456700).toString().substring(0, 27)); // passes with String
          //writeData(createWithNano(123456700)); // FAILS with LocalDateTime - should pass
          //writeData(createWithNano(123456780)); // should fail
          //writeData(createWithNano(123456789)); // should fail
    }
}