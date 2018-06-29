/**
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
module mssqljdbc {
	exports com.microsoft.sqlserver.jdbc to mssqljdbc;
	exports com.microsoft.sqlserver.jdbc.dataclassification to mssqljdbc;
	exports com.microsoft.sqlserver.jdbc.dns to mssqljdbc;
	exports microsoft.sql to datatypes;
	requires static adal4j;
	requires static azure.client.runtime;
	requires static azure.keyvault;
	requires static azure.keyvault.webkey;
	requires static okhttp;
	requires static retrofit;
	requires client.runtime;
	requires jackson.databind;
	requires java.logging;
	requires java.management;
	requires java.naming;
	requires java.security.jgss;
	requires java.sql;
	requires java.xml;
}