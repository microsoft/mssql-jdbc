/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) 2016 Microsoft Corporation
 * All rights reserved.
 * 
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
/**
 * 
 * This class tests {@link MSSQLGenerator}
 */
@RunWith(JUnitPlatform.class)
public class TestSQLGenerator {
	
	/**
	 * This tests {@link MSSQLGenerator#createTable(DBTable)}
	 */
	@Test
	public void testCreateTableQuery() {
		DBColumn column = new DBColumn("INT_Col", Types.INTEGER);
		//		DBColumn[] columns = {column,new DBColumn("Double_col", Types.DOUBLE, 10,2), new DBColumn("VARCHAR_COL", Types.VARCHAR, 50)};
		DBColumn column1 = new DBColumn("TestDefault", Types.NCHAR);
		column1.setDefaultValue("Test Default");
		DBColumn[] columns = { column, new DBColumn("Double_col", Types.DECIMAL, 10, 2), new DBColumn("VARCHAR_COL", Types.VARCHAR, 50), column1 };

		DBTable table = new DBTable("Test1", columns);

		MSSQLGenerator sqlGen = new MSSQLGenerator();
		String sql = sqlGen.createTable(table);

		assertNotNull(sql, "Query should build");
	}

	/**
	 * This tests {@link MSSQLGenerator#createTable(DBTable)}
	 * TODO: Use parameterized test feature of JUnit
	 */
	@Test
	public void testCreateTableQuery1() {
		DBColumn column = new DBColumn("INT_Col", Types.INTEGER);
		DBColumn[] columns = { column, new DBColumn("Datetime_col", microsoft.sql.Types.MONEY), new DBColumn("VARCHAR_COL", Types.VARCHAR, 50) };

		DBTable table = new DBTable("Test1", columns);

		MSSQLGenerator sqlGen = new MSSQLGenerator();
		String sql = sqlGen.createTable(table);

		assertNotNull(sql, "Query should build");
	}

	/**
	 * This tests {@link MSSQLGenerator#createTable(DBTable)}
	 * TODO: Use parameterized test feature of JUnit
	 */
	@Test
	public void testCreatetableQuery3() {
		DBColumn column1 = new DBColumn("CHAR_10", Types.CHAR,10);
		column1.setPrimaryKey(true);
		
		DBColumn column2 = new DBColumn("c2_date", microsoft.sql.Types.DATETIME);
		DBColumn column3 = new DBColumn("c3_decimal(28,4)", Types.DECIMAL,28,4);
		
		DBTable table = new DBTable("Test1",new DBColumn[] {column1, column2, column3, new DBColumn("C4_MONEY", microsoft.sql.Types.MONEY), new DBColumn("C5_TIME", Types.TIME)});
		
		MSSQLGenerator mssqlGenerator = new MSSQLGenerator();
		
		
		String sql = mssqlGenerator.createTable(table);
		
		assertNotNull(sql, "Query should build");
	}
	
	/**
	 * This tests {@link MSSQLGenerator#insertData(String, DBValue[])}
	 * TODO: Use parameterized test feature of JUnit
	 */
	@Test
	public void testInsertQuery() {
		DBValue[] complexOne = { new DBValue("CHAR_10", "One"), new DBValue("c2_date", Date.valueOf("2011-01-11")), new DBValue("c3_decimal(28,4)", new BigDecimal("1234.1234")) };

		MSSQLGenerator sqlGenerator = new MSSQLGenerator();
		String sql = sqlGenerator.insertData("test1", complexOne);

		assertNotNull(sql, "Query should build");
	}

	/**
	 * This tests {@link MSSQLGenerator#insertData(String, List)}
	 * TODO: 
	 * Use parameterized test feature of JUnit
	 */
	@Test
	public void testInsertQuery1() {
		List<Object> lstValues = new ArrayList<>();
		lstValues.add(1);
		lstValues.add(Date.valueOf("9999-12-31"));
		lstValues.add("Testing 1222");
		
		List<Object> lstComplex = new ArrayList<>();
		lstComplex.add("One");
		lstComplex.add(Date.valueOf("2011-01-11"));
		lstComplex.add(new BigDecimal("1234.1234"));
		

		MSSQLGenerator sqlGenerator = new MSSQLGenerator();
		String sql = sqlGenerator.insertData("test3", lstValues);
		
		assertNotNull(sql, "Query should build");
		
		sql = sqlGenerator.insertData("test4", lstComplex);
		assertNotNull(sql, "Query should build");
	}

}
