module mssql.jdbc
{
	exports com.microsoft.sqlserver.jdbc;

	requires adal4j;
	requires azure.keyvault;
	requires azure.keyvault.webkey;

	requires java.logging;
	requires java.management;
	requires java.naming;
	requires java.security.jgss;
	requires java.sql;
	requires java.xml;
}