/**
 * 
 */
package com.microsoft.sqlserver.jdbc.bvt;

import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.jupiter.api.BeforeAll;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;

/**
 * @author v-afrafi
 *
 */
@RunWith(JUnitPlatform.class)
public class bvtTestSetup extends AbstractTest {
    private static DBConnection conn = null;
    private static DBStatement stmt = null;
    
    static DBTable table1;
    static DBTable table2;
    @BeforeAll
    public static void init() throws SQLException {
        // try {
        // Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        // }
        // catch (ClassNotFoundException e) {
        // e.printStackTrace();
        // }
        
         // Statement stmt = null;
         try {
         // stmt = conn().createStatement();
         conn = new DBConnection(connectionString);
         stmt = conn.createStatement();
        
         // create tables
         table1 = new DBTable(true);
         stmt.createTable(table1);
         stmt.populateTable(table1);
         table2 = new DBTable(true);
         stmt.createTable(table2);
         stmt.populateTable(table2);

        
         // // CREATE the table
         // stmt.executeUpdate(Tables.dropTable(table1));
         // stmt.executeUpdate(Tables.createTable(table1));
         // // CREATE the data to populate the table with
         // Values.createData();
         // Tables.populate(table1, stmt);
         //
         // stmt.executeUpdate(Tables.dropTable(table2));
         // stmt.executeUpdate(Tables.createTable(table2));
         // Tables.populate(table2, stmt);
         }
         finally {
         if (null != stmt) {
         stmt.close();
         }
//         terminateVariation();
         }
         }


}
