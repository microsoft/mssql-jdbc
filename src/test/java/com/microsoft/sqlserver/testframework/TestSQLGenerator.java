/**
 * File Name: TestSQLGenerator.java 
 * Created : Dec 3, 2016
 *
 * Microsoft JDBC Driver for SQL Server
 * The MIT License (MIT)
 * Copyright(c) 2016 Microsoft Corporation
 * All rights reserved.
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
 * and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *  
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR 
 * ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH 
 * THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE. 
 */
package com.microsoft.sqlserver.testframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumingThat;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
public class TestSQLGenerator {

	@Disabled
	@Test
	public void testCreateTableQuery() {
		DBColumn column = new DBColumn("INT_Col", Types.INTEGER);
		//		DBColumn[] columns = {column,new DBColumn("Double_col", Types.DOUBLE, 10,2), new DBColumn("VARCHAR_COL", Types.VARCHAR, 50)};
		DBColumn column1 = new DBColumn("TestDefault", Types.NCHAR);
		column1.setDefaultValue("Test Default");
		DBColumn[] columns = { column, new DBColumn("Double_col", Types.DECIMAL, 10, 2), new DBColumn("VARCHAR_COL", Types.VARCHAR, 50), column1 };

		DBTable table = new DBTable("Test1", columns);

		MSSQLGenerator sqlGen = new MSSQLGenerator();
		String s = sqlGen.createTable(table);

		System.out.println(s);
	}

	@Disabled
	@Test
	public void testCreateTableQuery1() {
		DBColumn column = new DBColumn("INT_Col", Types.INTEGER);
		DBColumn[] columns = { column, new DBColumn("Datetime_col", microsoft.sql.Types.MONEY), new DBColumn("VARCHAR_COL", Types.VARCHAR, 50) };

		DBTable table = new DBTable("Test1", columns);

		MSSQLGenerator sqlGen = new MSSQLGenerator();
		String s = sqlGen.createTable(table);

		System.out.println(s);
	}

	@Test
	public void testCreatetableQuery3() {
		DBColumn column1 = new DBColumn("CHAR_10", Types.CHAR,10);
		column1.setPrimaryKey(true);
		
		DBColumn column2 = new DBColumn("c2_date", microsoft.sql.Types.DATETIME);
		DBColumn column3 = new DBColumn("c3_decimal(28,4)", Types.DECIMAL,28,4);
		
		DBTable table = new DBTable("Test1",new DBColumn[] {column1, column2, column3, new DBColumn("C4_MONEY", microsoft.sql.Types.MONEY), new DBColumn("C5_TIME", Types.TIME)});
		
		MSSQLGenerator mssqlGenerator = new MSSQLGenerator();
		
		
		String sql = mssqlGenerator.createTable(table);
		
		System.out.println(sql);
	}
	
	@Disabled
	@Test
	public void testInsertQuery() {
		DBValue value = new DBValue("INT_Col", 1);
		DBValue[] values = { value, new DBValue("DateTimeCol", Date.valueOf("9999-12-31")), new DBValue("Description", "Testing 1222") };

		DBValue[] complexOne = { new DBValue("CHAR_10", "One"), new DBValue("c2_date", Date.valueOf("2011-01-11")), new DBValue("c3_decimal(28,4)", new BigDecimal("1234.1234")) };

		MSSQLGenerator sqlGenerator = new MSSQLGenerator();
		String sql = sqlGenerator.insertData("test1", complexOne);

		System.out.println(sql);
	}

	@Test
	public void testInsertQuery1() {
		DBValue value = new DBValue("INT_Col", 1);
		DBValue[] values = { value, new DBValue("DateTimeCol", Date.valueOf("9999-12-31")), new DBValue("Description", "Testing 1222") };
		List<Object> lstValues = new ArrayList<>();
		lstValues.add(1);
		lstValues.add(Date.valueOf("9999-12-31"));
		lstValues.add("Testing 1222");
		
		List<Object> lstComplex = new ArrayList<>();
		lstComplex.add("One");
		lstComplex.add(Date.valueOf("2011-01-11"));
		lstComplex.add(new BigDecimal("1234.1234"));
		
		DBValue[] complexOne = { new DBValue("CHAR_10", "One"), new DBValue("c2_date", Date.valueOf("2011-01-11")), new DBValue("c3_decimal(28,4)", new BigDecimal("1234.1234")) };

		MSSQLGenerator sqlGenerator = new MSSQLGenerator();
		String sql = sqlGenerator.insertData("test3", lstValues);
		
		System.out.println(sql);
		
		sql = sqlGenerator.insertData("test4", lstComplex);
		System.out.println(sql);
	}

	@Test
	public void testInAllEnvironments() {
		assumingThat("CI".equals(System.getenv("ENV")), () -> {
			// perform these assertions only on the CI server
			assertEquals(2, 3);
			System.out.println("CI");
		});

		// perform these assertions in all environments
		assertEquals("a string", "a string");
		System.out.println("NOT CI");
	}

}
