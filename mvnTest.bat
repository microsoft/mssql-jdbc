FOR /L %%A IN (1,1,50) DO (
  mvn -Dtest=BulkCopyISQLServerBulkRecordTest,bvtTest test
)

