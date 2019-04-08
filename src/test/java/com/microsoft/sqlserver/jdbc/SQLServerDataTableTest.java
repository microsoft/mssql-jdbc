package com.microsoft.sqlserver.jdbc;

import com.microsoft.sqlserver.testframework.AbstractTest;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.sql.Types;

@RunWith(JUnitPlatform.class)
public class SQLServerDataTableTest extends AbstractTest
{
	@Test
	void testClear() throws SQLServerException
	{
		SQLServerDataTable table = new SQLServerDataTable();
		SQLServerDataColumn a = new SQLServerDataColumn("foo", Types.VARCHAR);
		SQLServerDataColumn b = new SQLServerDataColumn("bar", Types.INTEGER);
		table.addColumnMetadata(a);
		table.addColumnMetadata(b);
		Assert.assertEquals(2, table.getColumnMetadata().size());

		table.clear();
		Assert.assertEquals(0, table.getColumnMetadata().size());

		table.addColumnMetadata(a);
		table.addColumnMetadata(b);
		Assert.assertEquals(2, table.getColumnMetadata().size());
	}
}