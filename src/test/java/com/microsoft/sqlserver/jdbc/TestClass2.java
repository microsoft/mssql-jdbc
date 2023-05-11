package com.microsoft.sqlserver.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TestClass2 {
    public static void main(String[] args) throws SQLException {
        final String url = "jdbc:sqlserver://drivers-ae-vbs-none.database.windows.net;userName=employee;password=Moonshine4me;databaseName=TestDb";

        try (Connection connection = DriverManager.getConnection(url)) {
            /* Assumes connection is an active Connection object. */

            // Create an in-memory data table.
            SQLServerDataTable sourceDataTable = new SQLServerDataTable();

            // Define metadata for the data table.
            sourceDataTable.addColumnMetadata("CategoryID" ,java.sql.Types.INTEGER);
            sourceDataTable.addColumnMetadata("CategoryName" ,java.sql.Types.NVARCHAR);

            // Populate the data table.
            sourceDataTable.addRow(1, "CategoryNameValue1");
            sourceDataTable.addRow(2, "CategoryNameValue2");

            // Pass the data table as a table-valued parameter using a prepared statement.
            SQLServerPreparedStatement pStmt =
                    (SQLServerPreparedStatement) connection.prepareStatement(
                            "INSERT INTO dbo.Categories SELECT * FROM ?;");
            pStmt.setStructured(1, "dbo.CategoryTableType", sourceDataTable);
            pStmt.execute();
        }
    }
}
