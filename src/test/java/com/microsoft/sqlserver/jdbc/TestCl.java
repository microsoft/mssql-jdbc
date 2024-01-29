package com.microsoft.sqlserver.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TestCl {
    public static void main(String[] args) throws SQLException {
        log(true, Level.FINER);

        String connectionString = "jdbc:sqlserver://nonsense;database=TestDb;user=sa;password=TestPassword123;" +
                "encrypt=true;trustServerCertificate=true;selectMethod=cursor;connectRetryCount=0;"
                + "transparentNetworkIPResolution=false";
        try(Connection conn = DriverManager.getConnection(connectionString);
            Statement s = conn.createStatement()) {

        }
    }

    private static void log(boolean on, Level lvl){
        if (on) {
            final ConsoleHandler handler = new ConsoleHandler();
            handler.setLevel(lvl);
            Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc");
            logger.addHandler(handler);
            logger.setLevel(lvl);
            logger.log(lvl, "The Sql Server logger is correctly configured.");
        }
    }
}
