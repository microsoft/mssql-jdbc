package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.PooledConnection;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.util.RandomUtil;

@RunWith(JUnitPlatform.class)
public class PoolingTest extends AbstractTest {

	@Test
	public void testPooling() throws SQLException {
		String randomTableName = RandomUtil.getIdentifier("table");

		//make the table a temporary table (will be created in tempdb database)
		String tempTableName = "#" + randomTableName;

		SQLServerXADataSource XADataSource1 = new SQLServerXADataSource();
		XADataSource1.setURL(connectionString);
		XADataSource1.setDatabaseName("tempdb");

		PooledConnection pc = XADataSource1.getPooledConnection();
		Connection conn = pc.getConnection();

		//create table in tempdb database 
		conn.createStatement().execute("create table [" + tempTableName + "] (myid int)");
		conn.createStatement().execute("insert into [" + tempTableName + "] values (1)");
		conn.close();

		conn = pc.getConnection();

		boolean tempTableFileRemoved = false;
		try {
			conn.createStatement().executeQuery("select * from [" + tempTableName + "]");
		} catch (SQLServerException e) {
			//make sure the temporary table is not found.
			if (e.getMessage().startsWith("Invalid object name")) {
				tempTableFileRemoved = true;
			}
		}
		assertTrue(tempTableFileRemoved, "Temporary table is not removed.");
	}

}
