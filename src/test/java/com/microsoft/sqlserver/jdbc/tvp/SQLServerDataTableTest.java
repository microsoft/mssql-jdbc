/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.tvp;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDataColumn;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerException;


@RunWith(JUnitPlatform.class)
public class SQLServerDataTableTest {

    @Test
    public void testClear() throws SQLServerException {
        SQLServerDataTable table = new SQLServerDataTable();
        SQLServerDataColumn a = new SQLServerDataColumn("foo", Types.VARCHAR);
        SQLServerDataColumn b = new SQLServerDataColumn("bar", Types.INTEGER);
        SQLServerDataColumn c = new SQLServerDataColumn("baz", Types.TIMESTAMP_WITH_TIMEZONE);

        table.addColumnMetadata(a);
        table.addColumnMetadata(b);
        table.addColumnMetadata(c);
        assertEquals(3, table.getColumnMetadata().size());

        table.clear();
        assertEquals(0, table.getColumnMetadata().size());

        table.addColumnMetadata(a);
        table.addColumnMetadata(b);
        table.addColumnMetadata(c);
        assertEquals(3, table.getColumnMetadata().size());
    }

    /*
     * * Test for hashCode and equals methods of SQLServerDataColumn and SQLServerDataTable.
     */
    @Test
    public void testHashCodes() throws SQLServerException {
        // Test Null field values for SQLServerDataColumn
        SQLServerDataColumn nullDataColumn1 = new SQLServerDataColumn(null, 0);
        SQLServerDataColumn nullDataColumn2 = new SQLServerDataColumn(null, 0);
        assert (nullDataColumn1.hashCode() == nullDataColumn2.hashCode());
        assert (nullDataColumn1.equals(nullDataColumn2));

        // Test Null field values for SQLServerDataTable
        SQLServerDataTable nullDataTable1 = new SQLServerDataTable();
        SQLServerDataTable nullDataTable2 = new SQLServerDataTable();
        assert (nullDataTable1.hashCode() == nullDataTable2.hashCode());
        assert (nullDataTable1.equals(nullDataTable2));

        SQLServerDataColumn a = new SQLServerDataColumn("foo", Types.VARCHAR);

        // Test consistent generation of hashCode
        assert (a.hashCode() == a.hashCode());
        assert (a.equals(a));

        SQLServerDataColumn aClone = new SQLServerDataColumn("foo", Types.VARCHAR);

        // Test for different instances generating same hashCode for same data
        assert (a.hashCode() == aClone.hashCode());
        assert (a.equals(aClone));

        SQLServerDataColumn b = new SQLServerDataColumn("bar", Types.DECIMAL);
        SQLServerDataColumn c = new SQLServerDataColumn("baz", Types.TIMESTAMP_WITH_TIMEZONE);
        SQLServerDataTable table = createTable(a, b, c);

        // Test consistent generation of hashCode
        assert (table.hashCode() == table.hashCode());
        assert (table.equals(table));

        SQLServerDataTable tableClone = createTable(aClone, b, c);

        // Test for different instances generating same hashCode for same data
        assert (table.hashCode() == tableClone.hashCode());
        assert (table.equals(tableClone));

        // Test for non equal hashCodes
        assert (a.hashCode() != b.hashCode());
        assert (!a.equals(b));

        SQLServerDataColumn bb = new SQLServerDataColumn("bar", Types.FLOAT);
        table.clear();
        table = createTable(a, bb, c);

        // Test for non equal hashCodes
        assert (table.hashCode() != tableClone.hashCode());
        assert (!table.equals(tableClone));
    }

    @Test()
    public void testEqualsTvp() throws SQLServerException {
        SQLServerDataTable a = new SQLServerDataTable();
        SQLServerDataTable b = new SQLServerDataTable();

        assert (a.equals(b));

        a.setTvpName("test");
        b.setTvpName(new String("test"));

        assert (a.equals(b));

        a.setTvpName("test");
        b.setTvpName(new String("test2"));
        assert (!a.equals(b));
    }

    private SQLServerDataTable createTable(SQLServerDataColumn a, SQLServerDataColumn b, SQLServerDataColumn c) throws SQLServerException {
        SQLServerDataTable table = new SQLServerDataTable();
        table.addColumnMetadata(a);
        table.addColumnMetadata(b);
        table.addColumnMetadata(c);
        table.addRow("Hello", new BigDecimal(1.5), OffsetDateTime.parse("1977-07-24T12:34+02:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        table.addRow("World", new BigDecimal(5.5), OffsetDateTime.parse("1977-07-24T12:34:56+02:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        table.setTvpName("TVP_HashCode");
        return table;
    }
}
