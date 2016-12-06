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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 * @author Microsoft
 *
 */
@RunWith(JUnitPlatform.class)
public class TestDBEngine extends AbstractTest {

	@Test
	public void testTableCreation() throws Exception {
		DBEngine dbEngine = new DBEngine();

		DBColumn column = new DBColumn("ID", Types.SMALLINT);
		column.setPrimaryKey(true);

		DBColumn[] columns = { column, new DBColumn("Name", Types.VARCHAR, 250), new DBColumn("Description", Types.NVARCHAR, 300) };
		DBTable table = new DBTable("TestNik1", columns);

		dbEngine.dropTable("testNik1", connection);

		dbEngine.createTable(table, connection);

		assertTrue(dbEngine.isTableExist("testNik1", connection), "Table Should Exist");

		dbEngine.dropTable("testNik1", connection);

		assertFalse(dbEngine.isTableExist("testNik1", connection), "Table Should Not Exist");
	}

	@Test
	public void testDropTable() throws Exception {
		DBEngine dbEngine = new DBEngine();
		dbEngine.dropTable("testNik1", connection);

		dbEngine.dropTable("testNik2", connection);

		dbEngine.dropTable("testNik3", connection);
	}

	@Test
	public void testInsertData() throws Exception {
		DBEngine dbEngine = new DBEngine();

		dbEngine.dropTable("Test1", connection);

		DBColumn column1 = new DBColumn("CHAR_10", Types.CHAR, 10);
		column1.setPrimaryKey(true);

		DBColumn column2 = new DBColumn("c2_date", microsoft.sql.Types.DATETIME);
		DBColumn column3 = new DBColumn("c3_decimal(28,4)", Types.DECIMAL, 28, 4);

		DBTable table = new DBTable("Test1", new DBColumn[] { column1, column2, column3 });

		dbEngine.createTable(table, connection);

		DBValue[] values = { new DBValue("CHAR_10", "One"), new DBValue("c2_date", Date.valueOf("2011-01-11")), new DBValue("c3_decimal(28,4)", new BigDecimal("1234.1234")) };

		dbEngine.insertdata("Test1", values, connection);
	}

	@Test
	public void testInsertData1() throws Exception {
		DBEngine dbEngine = new DBEngine();

		dbEngine.dropTable("Test2", connection);

		DBColumn column1 = new DBColumn("CHAR_10", Types.CHAR, 10);
		column1.setPrimaryKey(true);

		DBColumn column2 = new DBColumn("c2_date", microsoft.sql.Types.DATETIME);
		DBColumn column3 = new DBColumn("c3_decimal(28,4)", Types.DECIMAL, 28, 4);

		DBColumn[] column = { column1, column2, column3, new DBColumn("C4_MONEY", microsoft.sql.Types.MONEY), new DBColumn("C5_TIME", Types.TIME) };

		DBTable table = new DBTable("Test2", column);

		dbEngine.createTable(table, connection);

		DBValue value4 = new DBValue("C4_MONEY", new BigDecimal(78787878.58656));
		DBValue value5 = new DBValue("C5_TIME", Time.valueOf("23:59:59"));

		DBValue[] values = { new DBValue("CHAR_10", "One"), new DBValue("c2_date", Date.valueOf("2011-01-11")), new DBValue("c3_decimal(28,4)", new BigDecimal("1234.1234")),
				value4, value5 };

		dbEngine.insertdata("Test2", values, connection);
	}
	
	@Test
	public void testInsertDataWithList() throws Exception {
		DBEngine dbEngine = new DBEngine();

		dbEngine.dropTable("Test4", connection);

		DBColumn column1 = new DBColumn("CHAR_10", Types.CHAR, 10);
		column1.setPrimaryKey(true);

		DBColumn column2 = new DBColumn("c2_date", microsoft.sql.Types.DATETIME);
		DBColumn column3 = new DBColumn("c3_decimal(28,4)", Types.DECIMAL, 28, 4);

		DBColumn[] column = { column1, column2, column3, new DBColumn("C4_MONEY", microsoft.sql.Types.MONEY), new DBColumn("C5_TIME", Types.TIME) };

		DBTable table = new DBTable("Test4", column);

		dbEngine.createTable(table, connection);
/*
		DBValue value4 = new DBValue("C4_MONEY", new BigDecimal(78787878.58656));
		DBValue value5 = new DBValue("C5_TIME", Time.valueOf("23:59:59"));

		DBValue[] values = { new DBValue("CHAR_10", "One"), new DBValue("c2_date", Date.valueOf("2011-01-11")), new DBValue("c3_decimal(28,4)", new BigDecimal("1234.1234")),
				value4, value5 };
*/
		List<Object> lstValues = new ArrayList<>();
		lstValues.add("One");
		lstValues.add(Date.valueOf("2011-01-11"));
		lstValues.add(new BigDecimal("1234.1234"));
		lstValues.add(new BigDecimal(78787878.58656));
		lstValues.add( Time.valueOf("23:59:59"));
		
		dbEngine.insertdata("Test4", lstValues, connection);
	}
}
