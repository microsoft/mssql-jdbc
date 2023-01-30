import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

import com.microsoft.sqlserver.jdbc.SQLServerException;


public class Test {

    public static void main(String[] args) throws Exception {
        sqlAuthDriverManager();
    }

    private static void sqlAuthDriverManager() throws SQLException, SQLServerException {
        String jdbcUrl = "jdbc:sqlserver://jdbc-hgs.westeurope.cloudapp.azure.com;databaseName=ContosoHR";
        Properties info = new Properties();
        info.put("user", "employee");
        info.put("password", "Moonshine4me");

        Connection connection = DriverManager.getConnection(jdbcUrl, info);
        String customQuery = "select * from [AzureDW_DIatScale].[六书 / 六書];";
        DatabaseMetaData metadata = connection.getMetaData();
        ResultSet resultSet = metadata.getColumns(null, "AzureDW_DIatScale", "六书 / 六書", null);
        while (resultSet.next()) {
            System.out.println(resultSet.getString("COLUMN_NAME"));
        }
        ResultSet rs = metadata.getPrimaryKeys(null, "AzureDW_DIatScale", "六书 / 六書");
        while (rs.next()) {
            System.out.println(rs.getString("COLUMN_NAME"));
        }
        PreparedStatement preparedStatement = connection.prepareStatement(customQuery);
        ResultSetMetaData metaData1 = preparedStatement.getMetaData();
        int count = metaData1.getColumnCount();
        for (int i = 1; i <= count; i++) {
            System.out.println(metaData1.getColumnName(i));
        }
        System.out.println(count);
    }

}
