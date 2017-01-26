/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class SparseColumns {

    public static void main(String args[]) {

        // Declare the JDBC objects.
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;

        String serverName = null;
        String portNumber = null;
        String databaseName = null;
        String username = null;
        String password = null;

        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {

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

            // Create a variable for the connection string.
            String connectionUrl = "jdbc:sqlserver://" + serverName + ":" + portNumber + ";" + "databaseName=" + databaseName + ";username="
                    + username + ";password=" + password + ";";

            // Establish the connection.
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            con = DriverManager.getConnection(connectionUrl);

            createColdCallingTable(con);

            stmt = con.createStatement();
            // Determine the column set column
            String columnSetColName = null;
            String strCmd = "SELECT name FROM sys.columns WHERE object_id=(SELECT OBJECT_ID('ColdCalling')) AND is_column_set = 1";
            rs = stmt.executeQuery(strCmd);

            if (rs.next()) {
                columnSetColName = rs.getString(1);
                System.out.println(columnSetColName + " is the column set column!");
            }
            rs.close();

            rs = null;

            strCmd = "SELECT * FROM ColdCalling";
            rs = stmt.executeQuery(strCmd);

            // Iterate through the result set
            ResultSetMetaData rsmd = rs.getMetaData();

            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            InputSource is = new InputSource();
            while (rs.next()) {
                // Iterate through the columns
                for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
                    String name = rsmd.getColumnName(i);
                    String value = rs.getString(i);

                    // If this is the column set column
                    if (name.equalsIgnoreCase(columnSetColName)) {
                        System.out.println(name);

                        // Instead of printing the raw XML, parse it
                        if (value != null) {
                            // Add artificial root node "sparse" to ensure XML is well formed
                            String xml = "<sparse>" + value + "</sparse>";

                            is.setCharacterStream(new StringReader(xml));
                            Document doc = db.parse(is);

                            // Extract the NodeList from the artificial root node that was added
                            NodeList list = doc.getChildNodes();
                            Node root = list.item(0); // This is the <sparse> node
                            NodeList sparseColumnList = root.getChildNodes(); // These are the xml column nodes

                            // Iterate through the XML document
                            for (int n = 0; n < sparseColumnList.getLength(); ++n) {
                                Node sparseColumnNode = sparseColumnList.item(n);
                                String columnName = sparseColumnNode.getNodeName();
                                // Note that the column value is not in the sparseColumNode, it is the value of the first child of it
                                Node sparseColumnValueNode = sparseColumnNode.getFirstChild();
                                String columnValue = sparseColumnValueNode.getNodeValue();

                                System.out.println("\t" + columnName + "\t: " + columnValue);
                            }
                        }
                    }
                    else { // Just print the name + value of non-sparse columns
                        System.out.println(name + "\t: " + value);
                    }
                }
                System.out.println();// New line between rows
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (rs != null) {
                try {
                    rs.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (con != null) {
                try {
                    con.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void createColdCallingTable(Connection con) throws SQLException {

        Statement stmt = con.createStatement();

        stmt.execute("if exists (select * from sys.objects where name = 'ColdCalling')" + "drop table ColdCalling");

        String sql = "CREATE TABLE ColdCalling  (  ID int IDENTITY(1,1) PRIMARY KEY,  [Date] date,  [Time] time,  PositiveFirstName nvarchar(50) SPARSE,  PositiveLastName nvarchar(50) SPARSE,  SpecialPurposeColumns XML COLUMN_SET FOR ALL_SPARSE_COLUMNS  );";
        stmt.execute(sql);

        sql = "INSERT ColdCalling ([Date], [Time])  VALUES ('10-13-09','07:05:24')  ";
        stmt.execute(sql);

        sql = "INSERT ColdCalling ([Date], [Time], PositiveFirstName, PositiveLastName)  VALUES ('07-20-09','05:00:24', 'AA', 'B')  ";
        stmt.execute(sql);

        sql = "INSERT ColdCalling ([Date], [Time], PositiveFirstName, PositiveLastName)  VALUES ('07-20-09','05:15:00', 'CC', 'DD')  ";
        stmt.execute(sql);
    }
}
