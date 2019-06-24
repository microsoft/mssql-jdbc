package com.microsoft.sqlserver.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TestClass {
    public static void main(String[] args) throws InterruptedException, ClassNotFoundException, SQLException {
        
        int x=5;
        for(int i=0;i<20000000;i++) {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            final Connection connection = DriverManager
                    .getConnection("jdbc:sqlserver://localhost\\sqlexpress;", "test", "test");
            System.out.println(connection.getMetaData().getDriverVersion());
        }
        
    }
    
            
}
