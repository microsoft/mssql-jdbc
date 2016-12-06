/**
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) 2016 Microsoft Corporation
 * All rights reserved.
 * 
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;

/**
 * @author Microsoft
 *
 */
public class DBEngine {

	public static final Logger log = Logger.getLogger("DBEngine"); 
	
	/**
	 * We are utilizing connection which is initialized in setup.
	 * If one want to use extra connection then please create one from {@link PrepUtil#getConnection(String)}
	 * 
	 * @param dbTable {@link DBTable}
	 * @param connection {@link SQLServerConnection} Developer should not close this connection. 
	 */
	public void createTable(DBTable dbTable, SQLServerConnection connection) throws Exception{
		MSSQLGenerator sqlGenerator = new MSSQLGenerator();
		String sql = sqlGenerator.createTable(dbTable);
		System.out.println(sql);
		log.warning(sql);
		Statement stmt = connection.createStatement();
		stmt.execute(sql);
		stmt.close();
		stmt =null;
	}
	
	/**
	 * 
	 * @param tableName
	 * @param connection {@link SQLServerConnection}
	 * @return
	 */
	public boolean isTableExist(String tableName, SQLServerConnection connection) {
//		MSSQLGenerator sqlGenerator = new MSSQLGenerator();
//		sqlGenerator.isTableExist(tableName);
//		Temp. implemmentaion.
		
		boolean result = false;
		
		String sql = "SELECT count(1) FROM " + tableName;
		
		try {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			rs.next();
			String s = rs.getString(1);
//			System.out.println(s);
			result = true;
		} catch (SQLServerException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
			//Nothing todo.
		}catch(SQLException e) {
//			e.printStackTrace();
			//Nothing to do.
		}
		
		return result;
		
	}
	
	/**
	 * Drop table from Database
	 * @param tableName
	 * @param connection {@link SQLServerConnection}
	 * @return
	 * @throws Exception 
	 */
	public boolean dropTable(String tableName, SQLServerConnection connection) throws Exception {
		boolean result = false;

		SQLGeneratorIF sqlGenerator = new MSSQLGenerator();
		String sql = sqlGenerator.dropTable(tableName);
		System.out.println(sql);
		Statement stmt = connection.createStatement();
		int i = stmt.executeUpdate(sql);
		if (i >= 0) {
			result = true;
			if (log.isLoggable(Level.FINE)) {
				log.fine("Table Deleted " + tableName);
			}
		} else {
			log.warning("Table did not exist : " + tableName);
		}
		return result;
	}
	
	/**
	 * Insert data in table
	 * @param tableName table name
	 * @param values {@link DBValue}
	 * @param connection {@link SQLServerConnection}
	 * @return
	 * @throws Exception
	 */
	public void insertdata(String tableName, DBValue[] values, SQLServerConnection connection) throws Exception {
		SQLGeneratorIF sqlGenerator = new MSSQLGenerator();
		String sql = sqlGenerator.insertData(tableName, values);
		log.fine(sql);
		System.out.println(sql);
		Statement stmt = connection.createStatement();
		int rowUpdated = stmt.executeUpdate(sql);
		log.fine("Rows updated " + rowUpdated); 
		stmt.close();
		stmt = null;
	}
	
	/**
	 * Insert data in table
	 * @param tableName table name
	 * @param values {@link List}
	 * @param connection {@link SQLServerConnection}
	 * @return
	 * @throws Exception
	 */
	public void insertdata(String tableName, List<Object> values, SQLServerConnection connection) throws Exception {
		SQLGeneratorIF sqlGenerator = new MSSQLGenerator();
		String sql = sqlGenerator.insertData(tableName, values);
		log.fine(sql);
		System.out.println(sql);
		Statement stmt = connection.createStatement();
		int rowUpdated = stmt.executeUpdate(sql);
		log.fine("Rows updated " + rowUpdated); 
		stmt.close();
		stmt = null;
	}
}
