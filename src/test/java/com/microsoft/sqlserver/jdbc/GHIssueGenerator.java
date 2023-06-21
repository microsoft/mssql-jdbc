package com.microsoft.sqlserver.jdbc;

import java.util.Random;

public class GHIssueGenerator {
        public static class TestData{
            private String ID;
            private String NAME;
            private Integer AGE;
        }

        public static void main(String[] args) {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(128);

            DataStreamSource<TestData> source = env.addSource(new SourceFunction<TestData>() {
                private volatile boolean running = true;
                private final int maxRecords = 1000000;
                private int generatedRecords = 0;

                public void run(SourceContext<TestData> sourceContext) throws Exception {
                    Random random = new Random();

                    while (running && generatedRecords < maxRecords) {
                        String randomId = IdUtil.objectId();
                        String randomName = IdUtil.nanoId();
                        Integer randomAge = random.nextInt(100);

                        sourceContext.collect(new TestData(randomId, randomName, randomAge));

                        generatedRecords++;
                    }
                }

                public void cancel() {
                    running = false;
                }
            });

            SinkFunction<TestData> jdbcSink = JdbcSink.sink(
                    "INSERT INTO [dbo].[Invalid_Test_table] ([ID], [NAME], [AGE]) VALUES (?,?,?);",
                    (JdbcStatementBuilder<TestData>) (preparedStatement, value) -> {
                        preparedStatement.setObject(1,value.getID());
                        preparedStatement.setObject(2,value.getNAME());
                        preparedStatement.setObject(3,value.getAGE());
                    },
                    JdbcExecutionOptions.builder()
                            .withMaxRetries(3)
                            .withBatchSize(100000)
                            .build(),
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .withDriverName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
                            .withUrl("jdbc:sqlserver://XXX.XXX.XXX.XXX:XXXX;databaseName=TEST")
                            .withUsername("sa")
                            .withPassword("XXX")
                            .withConnectionCheckTimeoutSeconds(60)
                            .build()
            );
            source.addSink(jdbcSink);

            env.execute();
        }

}
