package com.microsoft.sqlserver.jdbc;

import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.sql.DriverManager.getConnection;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestClass {
    public static void main(String[] args) throws Exception {
//        final ConsoleHandler handler = new ConsoleHandler();
//        handler.setLevel(Level.FINE);
//        Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc");
//        logger.addHandler(handler);
//        logger.setLevel(Level.FINEST);
//        logger.log(Level.FINE, "The Sql Server logger is correctly configured.");

        //sendStringParamsTest("i", false);
        //sendStringParamsTestJeff("2", false);
        //sendStringParamsTest2();
        //bigDecimalTest();
        //testConnectCountInLogin();
    }

    public static void sendStringParamsTest(String option, boolean sendStringParamsAsUnicode) throws Exception {
        String runningOption = "";
        String loggingOption = "";

        int count = 0;
        int ps_average = 0;
        int st_average = 0;

        if (option.equalsIgnoreCase("e")) {
            runningOption = "SELECT ID FROM [dbo].[Effective_Test_table]";
            loggingOption = "SELECT * FROM [dbo].[Effective_Test_table]";
        } else if (option.equalsIgnoreCase("i")) {
            runningOption = "SELECT ID,AGE FROM [dbo].[Invalid_Test_table]";
            loggingOption = "SELECT * FROM [dbo].[Invalid_Test_table]";
        } else {
            runningOption = "SELECT ID,NAME,AGE FROM [dbo].[Three_PK]";
            loggingOption = "SELECT * FROM [dbo].[Three_PK]";
        }

        String connectionString = "";
        if (sendStringParamsAsUnicode) {
            connectionString = "jdbc:sqlserver://localhost:1433;databaseName=TestDb;userName=sa;password=TestPassword123;encrypt=false;trustServerCertificate=true;";
        } else  {
            connectionString = "jdbc:sqlserver://localhost:1433;databaseName=TestDb;userName=sa;password=TestPassword123;encrypt=false;trustServerCertificate=true;sendStringParametersAsUnicode=false";
        }

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        // 获取连接
        try(Connection connection = getConnection(connectionString)){
            // Sql
            String effectiveTestTablePkSql = "SELECT ID FROM [dbo].[Effective_Test_table]";
            String invalidTestTablePKSql = "SELECT ID,AGE FROM [dbo].[Invalid_Test_table]";

            // 构建查询
            preparedStatement = connection.prepareStatement(runningOption);
            resultSet = preparedStatement.executeQuery();

            List<Map<String,Object>> pkDataList = new ArrayList<>();
            int iPutIndex = 0;

            while (resultSet.next()){
                Map<String,Object> pkDataMaps = new HashMap<>();
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();

                for (int i = 1; i <= columnCount; i++) {
                    // 存放主键的K,V键值对
                    pkDataMaps.put(metaData.getColumnLabel(i),resultSet.getObject(i));
                }

                pkDataList.add(pkDataMaps);
                ++iPutIndex;

                // 每N行写入一次
                if (iPutIndex >= 699) {
                    List<Object> paramValueList = new ArrayList<>();

                    PreparedStatement preparedStatement2 = null;
                    Statement statement = null;
                    ResultSet resultSetForPreparedStatement = null;
                    ResultSet resultSetForStatement = null;

                    // 利用ID主键查询全量数据
                    try(Connection connection2 = getConnection(connectionString)){
                        // Sql
                        String effectiveTestTableFullSql = "SELECT * FROM [dbo].[Effective_Test_table]";
                        String invalidTestTableFullSql = "SELECT * FROM [dbo].[Invalid_Test_table]";

                        // 用于拼装preparedStatement SQL
                        StringJoiner or = new StringJoiner(" OR ");
                        // 用于拼装statement SQL
                        StringJoiner or2 = new StringJoiner(" OR ");
                        pkDataList.forEach(c -> {
                            StringJoiner and = new StringJoiner(" AND ", "(", ")");
                            StringJoiner and2 = new StringJoiner(" AND ", "(", ")");
                            for (Map.Entry<String, Object> k : c.entrySet()) {
                                and.add(k.getKey() + "=?");
                                and2.add(k.getKey() + "='" + k.getValue()+"'");
                                paramValueList.add(k.getValue());
                            }
                            or.add(and.toString());
                            or2.add(and2.toString());
                        });

                        /*
                          prepareStatement测试
                         */
                        String preparedStatementTestSql = loggingOption + " WHERE " + or;
                        //System.out.println("preparedStatement test Sql:"+preparedStatementTestSql);

                        preparedStatement2 = connection2.prepareStatement(preparedStatementTestSql);
                        for (int i = 1; i <= paramValueList.size(); i++) {
                            preparedStatement2.setObject(i,paramValueList.get(i-1));
                        }

                        // 计算执行时间
                        long timerNow = System.currentTimeMillis();
                        //System.out.println("开始执行preparedStatement test Sql查询");
                        resultSetForPreparedStatement = preparedStatement2.executeQuery();

                        int countRow1 = 0;
                        while (resultSetForPreparedStatement.next()){
                            countRow1++;
                        }
                        //System.out.println("preparedStatement test Sql 查询" +countRow1+"条记录执行耗时:" +(System.currentTimeMillis() - timerNow) +"ms");
                        ps_average += (System.currentTimeMillis() - timerNow);

                        /*
                          Statement测试
                         */
                        int countRow2 = 0;
                        String statementTestSql = loggingOption + " WHERE " + or2;
                        //System.out.println("statement test Sql:"+statementTestSql);

                        timerNow = System.currentTimeMillis();
                        //System.out.println("开始执行statement test Sql查询");
                        statement = connection2.createStatement();
                        resultSetForStatement = statement.executeQuery(statementTestSql);
                        while (resultSetForStatement.next()){
                            countRow2++;
                        }
                        //System.out.println("statement test Sql 查询" +countRow2+"条记录执行耗时:" +(System.currentTimeMillis() - timerNow) +"ms");
                        st_average += (System.currentTimeMillis() - timerNow);
                        count++;
                    }finally {
                        resultSetForStatement.close();
                        resultSetForPreparedStatement.close();
                        statement.close();
                        preparedStatement2.close();
                    }
                    pkDataList.clear();
                    iPutIndex = 0;
                }
            }
        } finally {
            System.out.println("preparedStatement test Sql 查询" +(ps_average / count) +"ms");
            System.out.println("statement test Sql 查询" +(st_average / count) +"ms");
            if (resultSet != null)
                resultSet.close();
            if (preparedStatement != null)
                preparedStatement.close();
        }
    }

    public static void sendStringParamsTestJeff(String option, boolean sendStringParamsAsUnicode) throws Exception {
        String runningOption = "";
        String loggingOption = "";
        if (option.equalsIgnoreCase("1")) {
            runningOption = "SELECT ID FROM [dbo].[Jeff1]";
            loggingOption = "SELECT * FROM [dbo].[Jeff1]";
        } else if (option.equalsIgnoreCase("2")){
            runningOption = "SELECT ID,NAME FROM [dbo].[Jeff2]";
            loggingOption = "SELECT * FROM [dbo].[Jeff2]";
        } else {
            runningOption = "SELECT ID,NAME,AGE FROM [dbo].[Jeff3]";
            loggingOption = "SELECT * FROM [dbo].[Jeff3]";
        }

        String connectionString = "";
        if (sendStringParamsAsUnicode) {
            connectionString = "jdbc:sqlserver://localhost:1433;databaseName=TestDb;userName=sa;password=TestPassword123;encrypt=false;trustServerCertificate=true;";
        } else  {
            connectionString = "jdbc:sqlserver://localhost:1433;databaseName=TestDb;userName=sa;password=TestPassword123;encrypt=false;trustServerCertificate=true;sendStringParametersAsUnicode=false";
        }

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        // 获取连接
        try(Connection connection = getConnection(connectionString)){
            // Sql

            // 构建查询
            preparedStatement = connection.prepareStatement(runningOption);
            resultSet = preparedStatement.executeQuery();

            List<Map<String,Object>> pkDataList = new ArrayList<>();
            int iPutIndex = 0;

            while (resultSet.next()){
                Map<String,Object> pkDataMaps = new HashMap<>();
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();

                for (int i = 1; i <= columnCount; i++) {
                    // 存放主键的K,V键值对
                    pkDataMaps.put(metaData.getColumnLabel(i),resultSet.getObject(i));
                }

                pkDataList.add(pkDataMaps);
                ++iPutIndex;

                // 每N行写入一次
                if (iPutIndex >= 500) {
                    List<Object> paramValueList = new ArrayList<>();

                    PreparedStatement preparedStatement2 = null;
                    Statement statement = null;
                    ResultSet resultSetForPreparedStatement = null;
                    ResultSet resultSetForStatement = null;

                    // 利用ID主键查询全量数据
                    try(Connection connection2 = getConnection(connectionString)){

                        // 用于拼装preparedStatement SQL
                        StringJoiner or = new StringJoiner(" OR ");
                        // 用于拼装statement SQL
                        StringJoiner or2 = new StringJoiner(" OR ");
                        pkDataList.forEach(c -> {
                            StringJoiner and = new StringJoiner(" AND ", "(", ")");
                            StringJoiner and2 = new StringJoiner(" AND ", "(", ")");
                            for (Map.Entry<String, Object> k : c.entrySet()) {
                                and.add(k.getKey() + "=?");
                                and2.add(k.getKey() + "='" + k.getValue()+"'");
                                paramValueList.add(k.getValue());
                            }
                            or.add(and.toString());
                            or2.add(and2.toString());
                        });

                        /*
                          prepareStatement测试
                         */
                        String preparedStatementTestSql = loggingOption + " WHERE " + or;
                        System.out.println("preparedStatement test Sql:"+preparedStatementTestSql);

                        preparedStatement2 = connection2.prepareStatement(preparedStatementTestSql);
                        for (int i = 1; i <= paramValueList.size(); i++) {
                            preparedStatement2.setObject(i,paramValueList.get(i-1));
                        }

                        // 计算执行时间
                        long timerNow = System.currentTimeMillis();
                        System.out.println("开始执行preparedStatement test Sql查询");
                        resultSetForPreparedStatement = preparedStatement2.executeQuery();

                        int countRow1 = 0;
                        while (resultSetForPreparedStatement.next()){
                            countRow1++;
                        }
                        System.out.println("preparedStatement test Sql 查询" +countRow1+"条记录执行耗时:"
                                +(System.currentTimeMillis() - timerNow) +"ms");


                        /*
                          Statement测试
                         */
                        int countRow2 = 0;
                        String statementTestSql = loggingOption + " WHERE " + or2;
                        System.out.println("statement test Sql:"+statementTestSql);

                        timerNow = System.currentTimeMillis();
                        System.out.println("开始执行statement test Sql查询");
                        statement = connection2.createStatement();
                        resultSetForStatement = statement.executeQuery(statementTestSql);
                        while (resultSetForStatement.next()){
                            countRow2++;
                        }
                        System.out.println("statement test Sql 查询" +countRow2+"条记录执行耗时:"
                                +(System.currentTimeMillis() - timerNow) +"ms");
                    }finally {
                        resultSetForStatement.close();
                        resultSetForPreparedStatement.close();
                        statement.close();
                        preparedStatement2.close();
                    }

                    pkDataList.clear();
                    iPutIndex = 0;
                }
            }
        } finally {
            if (resultSet != null)
                resultSet.close();
            if (preparedStatement != null)
                preparedStatement.close();
        }
    }

    public static void sendStringParamsTest2() throws Exception {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        // 获取连接
        try(Connection connection = getConnection("jdbc:sqlserver://localhost:1433;databaseName=TestDb;userName=sa;password=TestPassword123;encrypt=false;trustServerCertificate=true;sendStringParametersAsUnicode=true")){
            // Sql
            String effectiveTestTablePkSql = "SELECT ID FROM [dbo].[Effective_Test_table]";
            String invalidTestTablePKSql = "SELECT ID,AGE FROM [dbo].[Invalid_Test_table]";

            // 构建查询
            preparedStatement = connection.prepareStatement(effectiveTestTablePkSql);
            resultSet = preparedStatement.executeQuery();

            List<Map<String,Object>> pkDataList = new ArrayList<>();
            int iPutIndex = 0;

            while (resultSet.next()){
                Map<String,Object> pkDataMaps = new HashMap<>();
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();

                for (int i = 1; i <= columnCount; i++) {
                    // 存放主键的K,V键值对
                    pkDataMaps.put(metaData.getColumnLabel(i),resultSet.getObject(i));
                }

                pkDataList.add(pkDataMaps);
                ++iPutIndex;

                // 每N行写入一次
                if (iPutIndex >= 1000) {
                    List<Object> paramValueList = new ArrayList<>();

                    PreparedStatement preparedStatement2 = null;
                    Statement statement = null;
                    ResultSet resultSetForPreparedStatement = null;
                    ResultSet resultSetForStatement = null;

                    // 利用ID主键查询全量数据
                    try(Connection connection2 = getConnection("jdbc:sqlserver://localhost:1433;databaseName=TestDb;userName=sa;password=TestPassword123;encrypt=false;trustServerCertificate=true;sendStringParametersAsUnicode=true")){
                        // Sql
                        String effectiveTestTableFullSql = "SELECT * FROM [dbo].[Effective_Test_table]";
                        String invalidTestTableFullSql = "SELECT * FROM [dbo].[Invalid_Test_table]";

                        // 用于拼装preparedStatement SQL
                        StringJoiner or = new StringJoiner(" OR ");
                        // 用于拼装statement SQL
                        StringJoiner or2 = new StringJoiner(" OR ");
                        pkDataList.forEach(c -> {
                            StringJoiner and = new StringJoiner(" AND ", "(", ")");
                            StringJoiner and2 = new StringJoiner(" AND ", "(", ")");
                            for (Map.Entry<String, Object> k : c.entrySet()) {
                                and.add(k.getKey() + "=?");
                                and2.add(k.getKey() + "='" + k.getValue()+"'");
                                paramValueList.add(k.getValue());
                            }
                            or.add(and.toString());
                            or2.add(and2.toString());
                        });

                        /*
                          prepareStatement测试
                         */
                        String preparedStatementTestSql = effectiveTestTableFullSql + " WHERE " + or;
                        System.out.println("preparedStatement test Sql:"+preparedStatementTestSql);

                        preparedStatement2 = connection2.prepareStatement(preparedStatementTestSql);
                        for (int i = 1; i <= paramValueList.size(); i++) {
                            preparedStatement2.setObject(i,paramValueList.get(i-1));
                        }

                        // 计算执行时间
                        //StopWatch jdbcScanTimeCost = StopWatch.createStarted();
                        long timerNow = System.currentTimeMillis();
                        System.out.println("开始执行preparedStatement test Sql查询");
                        resultSetForPreparedStatement = preparedStatement2.executeQuery();

                        int countRow1 = 0;
                        while (resultSetForPreparedStatement.next()){
                            countRow1++;
                        }
                        System.out.println("preparedStatement test Sql 查询" +countRow1+"条记录执行耗时:"+(System.currentTimeMillis() - timerNow)+"ms");


                        /*
                          Statement测试
                         */
                        int countRow2 = 0;
                        String statementTestSql = effectiveTestTableFullSql + " WHERE " + or2;
                        System.out.println("statement test Sql:"+statementTestSql);

                        //StopWatch jdbcScanTimeCost2 = StopWatch.createStarted();
                        timerNow = System.currentTimeMillis();
                        System.out.println("开始执行statement test Sql查询");
                        statement = connection2.createStatement();
                        resultSetForStatement = statement.executeQuery(statementTestSql);
                        while (resultSetForStatement.next()){
                            countRow2++;
                        }
                        System.out.println("statement test Sql 查询" +countRow2+"条记录执行耗时:"+(System.currentTimeMillis() - timerNow)+"ms");
                    }finally {
                        resultSetForStatement.close();
                        resultSetForPreparedStatement.close();
                        statement.close();
                        preparedStatement2.close();
                    }

                    pkDataList.clear();
                    iPutIndex = 0;
                }
            }
        }finally {
            resultSet.close();
            preparedStatement.close();
        }
    }

    public static void bigDecimalTest() throws Exception {
        String tableName = "BigDecimalTest";
        String cxnString2 = "jdbc:sqlserver://jdbc-aas-vbs.westeurope.cloudapp.azure.com;userName=employee;"
                + "password=Moonshine4me;";
        String cxnString = "jdbc:sqlserver://localhost:1433;databaseName=TestD;userName=sa;password=Test"
                + "Password123;encrypt=false;trustServerCertificate=true;connectRetryCount=1;connectRetryInterval=1";
        try (Connection con = getConnection(cxnString); Statement stmt = con.createStatement()) {
            //stmt.executeUpdate("CREATE TABLE " + tableName + " (test_column decimal(10,5))");
//            try (PreparedStatement pstmt2 = con.prepareStatement("CREATE TABLE " + tableName
//                  + " (test_column decimal(10,5))")) {
//                pstmt2.execute();
//            }
//            try (PreparedStatement pstmt2 = con.prepareStatement("INSERT INTO " + tableName + " VALUES(?)")) {
//                pstmt2.setDouble(1, 99999.12345);
//                pstmt2.execute();
//                pstmt2.execute();
//                pstmt2.execute();
//            }
//            try (PreparedStatement pstmt2 = con.prepareStatement("INSERT INTO " + tableName + " VALUES(?)")) {
//                pstmt2.setObject(1, 99999.12345);
//                pstmt2.execute();
//            }
            //stmt.executeUpdate();
//            try (PreparedStatement pstmt = con.prepareStatement("SELECT test_column "
//                    + " FROM " + tableName)) {
//                BigDecimal value1 = new BigDecimal("1.5");
//                pstmt.setObject(1, value1);
//
//                BigDecimal base = new BigDecimal("99999.12345");
//                BigDecimal expected1 = base.subtract(value1);
//
//                try (ResultSet rs = pstmt.executeQuery()) {
//                    rs.next();
//                    assertEquals(expected1, rs.getObject(1));
//                }
//            }
        }
    }

    /**
     * Tests whether connectRetryCount and connectRetryInterval are properly respected in the login loop.
     */
    public static void testConnectCountInLogin() {
        String cxnString = "jdbc:sqlserver://localhost:1433;databaseName=TestD;userName=sa;password=Test"
                + "Password123;encrypt=false;trustServerCertificate=true;";
        int connectRetryCount = 1;
        int connectRetryInterval = 5;

        long timerStart = 0;
        long timerEnd;
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(cxnString);
            ds.setLoginTimeout(30);
            ds.setConnectRetryCount(connectRetryCount);
            ds.setConnectRetryInterval(connectRetryInterval);
            ds.setDatabaseName(RandomUtil.getIdentifier("DataBase"));
            timerStart = System.currentTimeMillis();
            try (Connection con = ds.getConnection()) {
                // Should not have connected
                assertTrue(con == null, TestResource.getResource("R_shouldNotConnect"));
            }
        } catch (Exception e) {
            // We correctly should not have connected
            assertTrue(e.getMessage().contains(TestResource.getResource("R_cannotOpenDatabase")), e.getMessage());

            // Now measure the total time. It should be ~1s per attempt plus ~1s per retry plus the length of each
            // retry interval.
            timerEnd = System.currentTimeMillis();
            long totalTime = timerEnd - timerStart;
            int expectedTotalTimeInMillis = (1 + (connectRetryInterval * connectRetryCount)) * 1000;
            assertTrue(totalTime <= expectedTotalTimeInMillis, TestResource.getResource("R_executionTooLong"));
        }
    }
}