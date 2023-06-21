package com.microsoft.sqlserver.jdbc;

import java.sql.*;
import java.util.*;

public class GHIssueTest {
    public static void main(String[] args) throws Exception {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        // 获取连接
        try(Connection connection = DriverManager.getConnection("jdbc:sqlserver://XXX.XXX.XXX.XXX:XXXX;databaseName=TEST;sendStringParametersAsUnicode=false", "sa", "XXX")){
            // Sql
            String effectiveTestTablePkSql = "SELECT ID FROM [dbo].[Effective_Test_table]";
            String invalidTestTablePKSql = "SELECT ID,AGE FROM [dbo].[Invalid_Test_table]";

            // 构建查询
            preparedStatement = connection.prepareStatement(invalidTestTablePKSql);
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
                    try(Connection connection2 = DriverManager.getConnection("jdbc:sqlserver://XXX.XXX.XXX.XXX:XXXX;databaseName=TEST;sendStringParametersAsUnicode=false", "sa", "XXX")){
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
                        String preparedStatementTestSql = invalidTestTableFullSql + " WHERE " + or;
                        System.out.println("preparedStatement test Sql:"+preparedStatementTestSql);

                        preparedStatement2 = connection2.prepareStatement(preparedStatementTestSql);
                        for (int i = 1; i <= paramValueList.size(); i++) {
                            preparedStatement2.setObject(i,paramValueList.get(i-1));
                        }

                        // 计算执行时间
                        StopWatch jdbcScanTimeCost = StopWatch.createStarted();
                        System.out.println("开始执行preparedStatement test Sql查询");
                        resultSetForPreparedStatement = preparedStatement2.executeQuery();

                        int countRow1 = 0;
                        while (resultSetForPreparedStatement.next()){
                            countRow1++;
                        }
                        System.out.println("preparedStatement test Sql 查询" +countRow1+"条记录执行耗时:"+jdbcScanTimeCost.getTime()+"ms");


                        /*
                          Statement测试
                         */
                        int countRow2 = 0;
                        String statementTestSql = invalidTestTableFullSql + " WHERE " + or2;
                        System.out.println("statement test Sql:"+statementTestSql);

                        StopWatch jdbcScanTimeCost2 = StopWatch.createStarted();
                        System.out.println("开始执行statement test Sql查询");
                        statement = connection2.createStatement();
                        resultSetForStatement = statement.executeQuery(statementTestSql);
                        while (resultSetForStatement.next()){
                            countRow2++;
                        }
                        System.out.println("statement test Sql 查询" +countRow2+"条记录执行耗时:"+jdbcScanTimeCost2.getTime()+"ms");
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
}
