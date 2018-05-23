package com.microsoft.sqlserver.jdbc.resultset;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.SensitivityProperty;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.util.RandomUtil;

@RunWith(JUnitPlatform.class)
public class DataClassificationTest extends AbstractTest {
    private static final String tableName = "[" + RandomUtil.getIdentifier("DataClassification") + "]";
    
    /**
     * Tests data classification metadata information from SQL Server
     * @throws Exception 
     */
    @Test
    public void testDataClassificationMetadata() throws Exception {
        //Run this test only with newer SQL Servers (version>=2018) that support Data Classification
        try (Connection con = DriverManager.getConnection(connectionString);
            Statement stmt = connection.createStatement();){
            if(serverSupportsDataClassification(stmt)) {
                createTable(connection, stmt);
                runTestsForServer(stmt);
                dropTable(stmt);
            }
        }
    }
    
    /**
     * Checks if object SYS.SENSITIVITY_CLASSIFICATIONS exists in SQL Server
     * @param Statement
     * @return boolean
     */
    private boolean serverSupportsDataClassification(Statement stmt) {
        try {
            stmt.execute("SELECT * FROM SYS.SENSITIVITY_CLASSIFICATIONS");
        }catch(SQLException e) {
            if(e.getErrorCode() == 208) { 
                return false;
            }
        }
        return true;
    }
    
    /**
     * Creates a new table in database with data classification column tags
     * Inserts rows of data in the table
     * @param connection
     * @param stmt
     * @param tableName
     * @throws SQLException
     */
    private void createTable(Connection connection,
            Statement stmt) throws SQLException {
        String createQuery = "CREATE TABLE " + tableName + " (" + 
                "[Id] [int] IDENTITY(1,1) NOT NULL," +
                "[CompanyName] [nvarchar](40) NOT NULL," + 
                "[ContactName] [nvarchar](50) NULL," +
                "[ContactTitle] [nvarchar](40) NULL," +
                "[City] [nvarchar](40) NULL," +
                "[Country] [nvarchar](40) NULL," +
                "[Phone] [nvarchar](30) MASKED WITH (FUNCTION = 'default()') NULL," +
                "[Fax] [nvarchar](30) MASKED WITH (FUNCTION = 'default()') NULL)";
        System.out.println(createQuery);
        stmt.execute(createQuery);
        
        stmt.execute("ADD SENSITIVITY CLASSIFICATION TO " + tableName + ".CompanyName WITH (LABEL='PII', LABEL_ID='L1', INFORMATION_TYPE='Company name', INFORMATION_TYPE_ID='COMPANY')");
        stmt.execute("ADD SENSITIVITY CLASSIFICATION TO " + tableName + ".ContactName WITH (LABEL='PII', LABEL_ID='L1', INFORMATION_TYPE='Person name', INFORMATION_TYPE_ID='NAME')");
        stmt.execute("ADD SENSITIVITY CLASSIFICATION TO " + tableName + ".Phone WITH (LABEL='PII', LABEL_ID='L1', INFORMATION_TYPE='Contact Information', INFORMATION_TYPE_ID='CONTACT')");
        stmt.execute("ADD SENSITIVITY CLASSIFICATION TO " + tableName + ".Fax WITH (LABEL='PII', LABEL_ID='L1', INFORMATION_TYPE='Contact Information', INFORMATION_TYPE_ID='CONTACT')");
        
        //INSERT ROWS OF DATA
        try(PreparedStatement ps = connection.prepareStatement("INSERT INTO " + tableName + " VALUES (?,?,?,?,?,?,?)")) {
            
            ps.setString(1,"Exotic Liquids");
            ps.setString(2, "Charlotte Cooper");
            ps.setObject(3, null);
            ps.setObject(4, "London");
            ps.setString(5, "UK");
            ps.setString(6, "(171) 555-2222");
            ps.setString(7, null);
            ps.execute();
    
            ps.setString(1,"New Orleans");
            ps.setString(2, "Cajun Delights");
            ps.setObject(3, null);
            ps.setObject(4, "New Orleans");
            ps.setString(5, "USA");
            ps.setString(6, "(100) 555-4822");
            ps.setString(7, null);
            ps.execute();
    
            ps.setString(1,"Grandma Kelly's Homestead");
            ps.setString(2, "Regina Murphy");
            ps.setObject(3, null);
            ps.setObject(4, "Ann Arbor");
            ps.setString(5, "USA");
            ps.setString(6, "(313) 555-5735");
            ps.setString(7, "(313) 555-3349");
            ps.execute();
        }
    }

    /**
     * Selects data from the table and triggers verifySensitivityClassification method
     * @param stmt
     * @param queries
     * @throws Exception
     */
    private void runTestsForServer(Statement stmt) throws Exception {
        try (SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName)) {
            verifySensitivityClassification(rs);
        }
    }

    /**
     * Verifies resultset recieved to contain data classification information as set.
     * @param rs
     * @throws SQLException
     */
    private void verifySensitivityClassification(SQLServerResultSet rs) throws SQLException
    {
        if(null!= rs.getSensitivityClassification()){
            for (int columnPos = 0; columnPos < rs.getSensitivityClassification().getColumnSensitivities().size(); columnPos++)
            {
                for (SensitivityProperty sp : rs.getSensitivityClassification().getColumnSensitivities().get(columnPos).getSensitivityProperties())
                {
                    if(columnPos==1 || columnPos==2 || columnPos == 6 || columnPos ==7) {
                        assert(sp.getLabel() != null);
                        assert(sp.getLabel().getId().equalsIgnoreCase("L1"));
                        assert(sp.getLabel().getName().equalsIgnoreCase("PII"));

                        assert(sp.getInformationType() != null);
                        assert(sp.getInformationType().getId().equalsIgnoreCase(columnPos==1?"COMPANY":(columnPos==2?"NAME":"CONTACT")));
                        assert(sp.getInformationType().getName().equalsIgnoreCase(columnPos==1?"Company name":(columnPos==2?"Person Name":"Contact Information")));
                    }
                }
            }
        }
    }
    
    /**
     * Drops table from the database
     * @param stmt
     * @throws SQLException
     */
    private void dropTable(Statement stmt) throws SQLException {
        stmt.execute("DROP TABLE " + tableName);
    }
}
