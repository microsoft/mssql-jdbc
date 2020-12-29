package com.microsoft.sqlserver.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLServerIntegrityConstraintViolationExceptionTest {


    @Test
    public void testPrimaryKeyDuplicate() {
        SQLServerDataSource ds = CommonSettings.getSQLServerDataSource();

        try (ISQLServerConnection con = (ISQLServerConnection) ds.getConnection();
             ISQLServerStatement stmt = (ISQLServerStatement) con.createStatement();
             ISQLServerStatement stmt1 = (ISQLServerStatement) con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                     ResultSet.CONCUR_UPDATABLE)) {

            createTable(stmt);

            insertSomeValidRows(stmt1);

            insertRowWithSamePK(stmt1);
            updateRowToExistingPK_byResultSet(stmt1);
            updateRowToExistingPK_byExecuteSQL(stmt1);


        } catch (SQLServerException throwables) {
            throwables.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }


    private void updateRowToExistingPK_byResultSet(ISQLServerStatement stmt) {
        String SQL = "SELECT * FROM Department_JDBC_Sample WHERE DepartmentID = 2;";
        try (ISQLServerResultSet rs = (ISQLServerResultSet) stmt.executeQuery(SQL)) {
            // Update the row of data.
            rs.first();
            rs.updateInt("DepartmentID", 1);
            rs.updateRow();
        } catch (SQLException throwables) {
            boolean isNewException = throwables instanceof SQLServerIntegrityConstraintViolationException;
            Assertions.assertEquals(isNewException, true, "Thrown exception is not an instance of SQLServerIntegrityConstraintViolationException");

            //throwables.printStackTrace();
        }
    }
    private void updateRowToExistingPK_byExecuteSQL(ISQLServerStatement stmt) {
        String SQL = "INSERT INTO Department_JDBC_Sample (DepartmentID, Name, GroupName, ModifiedDate) " +
                "VALUES(1, 'HR', 'Executive General and Administration', '08/01/2006');";
        try {
            stmt.execute(SQL);
        } catch (SQLException throwables) {
            boolean isNewException = throwables instanceof SQLServerIntegrityConstraintViolationException;
            Assertions.assertEquals(isNewException, true, "Thrown exception is not an instance of SQLServerIntegrityConstraintViolationException");

            //throwables.printStackTrace();
        }
    }

    private void insertRowWithSamePK(ISQLServerStatement stmt) {
        String SQL = "SELECT * FROM Department_JDBC_Sample;";

        try (ISQLServerResultSet rs = (ISQLServerResultSet) stmt.executeQuery(SQL)) {
            // Insert a row of data.
            rs.moveToInsertRow();
            rs.updateInt("DepartmentID", 1);
            rs.updateString("Name", "2Accounting");
            rs.updateString("GroupName", "2Executive General and Administration");
            rs.updateString("ModifiedDate", "08/01/2006");
            rs.insertRow();
        } catch (SQLException throwables) {
            boolean isNewException = throwables instanceof SQLServerIntegrityConstraintViolationException;
            Assertions.assertEquals(isNewException, true, "Thrown exception is not an instance of SQLServerIntegrityConstraintViolationException");

            //throwables.printStackTrace();
        }
    }
    private void insertSomeValidRows(ISQLServerStatement stmt) {
        String SQL = "SELECT * FROM Department_JDBC_Sample;";

        try (ISQLServerResultSet rs = (ISQLServerResultSet) stmt.executeQuery(SQL)) {
            // Insert a row of data.
            rs.moveToInsertRow();
            rs.updateInt("DepartmentID", 1);
            rs.updateString("Name", "Accounting");
            rs.updateString("GroupName", "Executive General and Administration");
            rs.updateString("ModifiedDate", "08/01/2006");
            rs.insertRow();
            // Insert a row of data.
            rs.moveToInsertRow();
            rs.updateInt("DepartmentID", 2);
            rs.updateString("Name", "Accounting");
            rs.updateString("GroupName", "Executive General and Administration");
            rs.updateString("ModifiedDate", "08/01/2006");
            rs.insertRow();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
    private static void createTable(Statement stmt) throws SQLException {
        stmt.execute("if exists (select * from sys.objects where name = 'Department_JDBC_Sample')"
                + "drop table Department_JDBC_Sample");

        String sql = "CREATE TABLE [Department_JDBC_Sample](" //+ "[DepartmentID] [smallint] IDENTITY(1,1) NOT NULL,"
                + "[DepartmentID] [int] NOT NULL PRIMARY KEY,"
                + "[Name] [varchar](50) ," + "[GroupName] [varchar](50) ,"
                + "[ModifiedDate] [datetime] )";

        stmt.execute(sql);
    }
}


class CommonSettings {
    private static String serverName = "localhost";
    private static String portNumber = "1433";
    private static String databaseName = "DriverDb";
    private static String username = "SA";
    private static String password = "Pa$$w0rd2020";

    public static SQLServerDataSource getSQLServerDataSource() {
        // Establish the connection.
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setServerName(serverName);
        ds.setPortNumber(Integer.parseInt(portNumber));
        ds.setDatabaseName(databaseName);
        ds.setUser(username);
        ds.setPassword(password);

        return ds;
    }
}
