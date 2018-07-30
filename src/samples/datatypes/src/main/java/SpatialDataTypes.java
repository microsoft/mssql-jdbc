/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package datatypes.src.main.java;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import com.microsoft.sqlserver.jdbc.Geography;
import com.microsoft.sqlserver.jdbc.Geometry;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;

/**
 * Sample to demonstrate how to use Spatial Datatypes 'Geography' and 'Geometry' in SQL Server with JDBC Driver
 */
public class SpatialDataTypes {

    private static String tableName = "SpatialDataTypesTable_JDBC_Sample";

    public static void main(String[] args) {

        String serverName = null;
        String portNumber = null;
        String databaseName = null;
        String username = null;
        String password = null;

        try (InputStreamReader in = new InputStreamReader(System.in); BufferedReader br = new BufferedReader(in)) {
            System.out.print("Enter server name: ");
            serverName = br.readLine();
            System.out.print("Enter port number: ");
            portNumber = br.readLine();
            System.out.print("Enter database name: ");
            databaseName = br.readLine();
            System.out.print("Enter username: ");
            username = br.readLine();
            System.out.print("Enter password: ");
            password = br.readLine();

            // Establish the connection.
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName(serverName);
            ds.setPortNumber(Integer.parseInt(portNumber));
            ds.setDatabaseName(databaseName);
            ds.setUser(username);
            ds.setPassword(password);

            // Establish the connection.
            try (Connection con = ds.getConnection(); Statement stmt = con.createStatement();) {
                dropAndCreateTable(stmt);
                
                // TODO: Implement Sample code
                String geoWKT = "POINT(3 40 5 6)";
                Geometry geomWKT = Geometry.STGeomFromText(geoWKT, 0);
                Geography geogWKT = Geography.STGeomFromText(geoWKT, 4326);
                
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con
                        .prepareStatement("insert into " + tableName + " values (?, ?)");) {
                    pstmt.setGeometry(1, geomWKT);
                    pstmt.setGeography(2, geogWKT);
                    pstmt.execute();

                    SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("select * from " + tableName);
                    rs.next();
                    
                    System.out.println("Geometry data: " + rs.getGeometry(1));
                    System.out.println("Geography data: " + rs.getGeography(2));
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void dropAndCreateTable(Statement stmt) throws SQLException {
        stmt.executeUpdate("if object_id('" + tableName + "','U') is not null" + " drop table " + tableName);

        stmt.executeUpdate("Create table " + tableName + " (c1 geometry, c2 geography)");
    }
}
