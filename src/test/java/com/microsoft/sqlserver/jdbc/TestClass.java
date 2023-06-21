package com.microsoft.sqlserver.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TestClass {

    public static void main(String[] args) throws Exception {
        final ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.ALL);
        Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc");
        logger.addHandler(handler);
        logger.setLevel(Level.FINEST);
        logger.log(Level.FINE, "The Sql Server logger is correctly configured.");

        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setServerName("jefftesting.database.windows.net");
        ds.setDatabaseName("TestDb");
        ds.setAuthentication("ActiveDirectoryInteractive");

        ds.setUser("JWasty@simba.com");

        runNewWhileConnected(ds);
        //runOneAfterAnother(ds);
    }


    /**
     * Run one connection while another is connected to see if multiple auth windows pop up.
     * @param ds
     * @throws Exception
     */
    private static void runNewWhileConnected(SQLServerDataSource ds) throws Exception {

        // Run a new connection while connected
        try (Connection connection = ds.getConnection();
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT SUSER_SNAME()")) {
            DatabaseMetaData metaData = connection.getMetaData();
            System.out.println(metaData.getDriverVersion());
            if (rs.next()) {
                System.out.println("You have successfully logged on as: " + rs.getString(1));

                try (Connection conn = ds.getConnection();
                     Statement stmtt = conn.createStatement();
                     ResultSet rst = stmtt.executeQuery("SELECT SUSER_SNAME()")){
                    if (rst.next()) {
                        System.out.println("You have successfully logged on as: " + rst.getString(1));
                    }
                }
            }
        }
    }

    /**
     * Run one connection after another to see if multiple auth windows pop up.
     * @param ds
     * @throws Exception
     */
    private static void runOneAfterAnother(SQLServerDataSource ds) throws Exception {
        try (Connection connection = ds.getConnection();
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT SUSER_SNAME()")) {
            DatabaseMetaData metaData = connection.getMetaData();
            System.out.println(metaData.getDriverVersion());
            if (rs.next()) {
                System.out.println("You have successfully logged on as: " + rs.getString(1));
            }
        }
        try (Connection conn = ds.getConnection();
             Statement stmtt = conn.createStatement();
             ResultSet rst = stmtt.executeQuery("SELECT SUSER_SNAME()")){
            if (rst.next()) {
                System.out.println("You have successfully logged on as: " + rst.getString(1));
            }
        }
    }
}
