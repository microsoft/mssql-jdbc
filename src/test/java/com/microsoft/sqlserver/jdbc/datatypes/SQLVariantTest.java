/**
 * 
 */
package com.microsoft.sqlserver.jdbc.datatypes;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.testframework.AbstractTest;

/**
 * @author v-afrafi
 *
 */
@RunWith(JUnitPlatform.class)
public class SQLVariantTest extends AbstractTest {
    
    SQLServerConnection con = null;
    Statement stmt = null;
    String tableName = "SqlVariant_Test"; //"charTest"; ////
    
    
    private void createTables() throws SQLServerException {
         try {
            con = (SQLServerConnection) DriverManager.getConnection(connectionString);
            stmt = con.createStatement();
            stmt.executeUpdate("CREATE TABLE " + tableName + " (col1 sql_variant)");
        }
        catch (SQLException e) {
            fail(e.toString());
        }
        finally{
            if (null != con)
                con.close();
        }
         
    }
    
    /**
     * Read from a sql_variant table
     * @throws SQLException 
     * @throws IOException 
     * @throws SecurityException 
     */
    @Test
    public void readFrom() throws SQLException, SecurityException, IOException {
        Handler fh = new FileHandler("C:\\Users\\v-afrafi\\Documents\\mssql-jdbc\\Driver.log");
        fh.setFormatter(new SimpleFormatter());
        fh.setLevel(Level.FINEST);
        Logger.getLogger("").addHandler(fh);
        // By default, Loggers also send their output to their parent logger.  
        // Typically the root Logger is configured with a set of Handlers that essentially act as default handlers for all loggers. 
        Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc");
        logger.setLevel(Level.FINEST);
        
        con = (SQLServerConnection) DriverManager.getConnection(connectionString);
        stmt = con.createStatement();
//      stmt.executeUpdate("INSERT into " + tableName + " values (2)");
//      PreparedStatement pstmt = con.prepareStatement("INSERT into " + tableName + " values (?)");
//      pstmt.setInt(1, 1);
//      pstmt.executeUpdate();
      
//      tableName = "dateTest"; 
      SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM "+ tableName);
      while (rs.next()){
          System.out.println(rs.getObject(1));
//          assertEquals(rs.getObject(1), 1000.12);
      }
      
        
    }
    
    /**
     * drop the tables
     * @throws SQLException
     */
//    @AfterAll
//    public void dropAll() throws SQLException{
//        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') and OBJECTPROPERTY(id, N'IsTable') = 1)"
//                + " DROP TABLE " + tableName);
//    }

}
