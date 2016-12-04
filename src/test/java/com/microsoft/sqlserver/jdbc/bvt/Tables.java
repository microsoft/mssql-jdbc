/*
File: Tables.java
Contents: Creates and populates the tables.

Microsoft JDBC Driver for SQL Server
Copyright(c) Microsoft Corporation
All rights reserved.
MIT License
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), 
to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
IN THE SOFTWARE.
*/
package com.microsoft.sqlserver.jdbc.bvt;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class Tables {
	static String query = "";
	static String name = "";
	private static String create_Table_sql = "";
	private static String drop_Table_Query = "";
	private static final String primary_key = "[c26_int]";

	public static String createTable(String tableName) {
		create_Table_sql = "CREATE TABLE " + tableName + "([c1_char(512)] char(512)," + " [c2_date] date, "
				+ "[c3_decimal(28,4)] decimal(28,4)," + " [c4_money] money," + " [c5_time] time, "
				+ "[c6_uniqueidentifier] uniqueidentifier," + " [c7_varchar(max)] varchar(max), "
				+ "[c8_nvarchar(max)] nvarchar(max)," + " [c9_smallint] smallint, "
				+ "[c10_numeric(28,4)] numeric(28,4), " + "[c11_ntext] ntext, "
				+ "[c12_varbinary(max)] varbinary(max), " + "[c13_nchar(512)] nchar(512), "
				+ "[c14_datetimeoffset(7)] datetimeoffset(7), " + "[c15_xml] xml,"
				+ " [c16_smalldatetime] smalldatetime," + " [c17_varbinary(512)] varbinary(512), " + "[c18_bit] bit, "
				+ "[c19_varchar(512)] varchar(512), " + "[c20_real] real, " + "[c21_tinyint] tinyint,"
				+ " [c22_float] float, " + "[c23_text] text, " + "[c24_binary(512)] binary(512),"
				+ " [c25_datetime2] datetime2," + " [c26_int] int NOT NULL PRIMARY KEY, " + "[c27_datetime] datetime,"
				+ " [c28_bigint] bigint," + " [c29_nvarchar(512)] nvarchar(512), " + "[c30_smallmoney] smallmoney,"
				+ "[c31_int] int" + ")";
		return create_Table_sql;
	}

	public static String dropTable(String tableName) {
		drop_Table_Query = "if object_id('" + tableName + "','U') is not null" + " drop table " + tableName;
		return drop_Table_Query;
	}

	public static String select(String tableName) {
		return query = "SELECt * FROM " + tableName;
	}

	public static String select_Orderby(String tableName, String column) {
		return "SELECt * FROM " + tableName + " order by " + column;
	}

	public static String orderby(String column) {

		if (query.contains("order"))
			return query = query + ", " + column;
		else
			return query = query + " order by " + column;

	}

	public static String primaryKey() {
		return primary_key;
	}

	public static void populate(String tableName, Statement stmt) {
		ArrayList<Object> valuesPerRow = null;
		for (int i = 0; i < Values.getRowNumbers(); i++) {
			valuesPerRow = Values.getTableValues(i);
			String populate_query = createInsertSQL(tableName, valuesPerRow);
			try {
				stmt.executeUpdate(populate_query);
			} catch (SQLException e) {
				fail(e.toString());
			}
		}
	}

	private static String createInsertSQL(String tableName, ArrayList<Object> valuesPerRow) {
		String sql = "INSERT into " + tableName + " values (" + "'" + valuesPerRow.get(0) + "', '" + valuesPerRow.get(1)
				+ "', " + valuesPerRow.get(2) + "," + valuesPerRow.get(3) + ", '" + valuesPerRow.get(4) + "','"
				+ valuesPerRow.get(5) + "','" + valuesPerRow.get(6) + "','" + valuesPerRow.get(7) + "', "
				+ valuesPerRow.get(8) + ", " + valuesPerRow.get(9) + ",N'" + valuesPerRow.get(10) + "', "
				+ valuesPerRow.get(11) + ", '" + valuesPerRow.get(12) + "',' " + valuesPerRow.get(13) + "',' "
				+ valuesPerRow.get(14) + "','" + valuesPerRow.get(15) + "'," + valuesPerRow.get(16) + ",'"
				+ valuesPerRow.get(17) + "','" + valuesPerRow.get(18) + "'," + valuesPerRow.get(19) + ", "
				+ valuesPerRow.get(20) + ", " + valuesPerRow.get(21) + ",'" + valuesPerRow.get(22) + "', "
				+ valuesPerRow.get(23) + ",' " + valuesPerRow.get(24) + "', " + valuesPerRow.get(25) + ",' "
				+ valuesPerRow.get(26) + "', " + valuesPerRow.get(27) + ", '" + valuesPerRow.get(28) + "', "
				+ valuesPerRow.get(29) + ", " + valuesPerRow.get(30) + " )";

		return sql;
	}
}
