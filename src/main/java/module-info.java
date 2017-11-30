/**
 * 
 */
/**
 * @author rye
 *
 */
module mssql.jdbc {
	exports com.microsoft.sqlserver.jdbc;

/*	requires HikariCP;*/
	requires adal4j;
	requires azure.keyvault;
	requires azure.keyvault.webkey;
/*	requires client.runtime;
	requires commons.codec;
	requires commons.dbcp2;
	requires commons.lang3;
	requires hamcrest.core; */
	requires java.logging;
	requires java.management;
	requires java.naming;
	requires java.security.jgss;
	requires java.sql;
	requires java.xml;
/*	requires junit;
	requires junit.jupiter.api;
	requires junit.platform.runner;
	requires oauth2.oidc.sdk;
	requires okhttp;
	requires opentest4j;*/
}