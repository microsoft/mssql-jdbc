package com.microsoft.sqlserver.jdbc.tvp;

import static org.junit.Assert.assertEquals;

import java.sql.Types;

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

        table.addColumnMetadata(a);
        table.addColumnMetadata(b);
        assertEquals(2, table.getColumnMetadata().size());

        table.clear();
        assertEquals(0, table.getColumnMetadata().size());

        table.addColumnMetadata(a);
        table.addColumnMetadata(b);
        assertEquals(2, table.getColumnMetadata().size());
    }

    @Test
    public void testHashCodes() throws SQLServerException {
        SQLServerDataColumn a = new SQLServerDataColumn("foo", Types.VARCHAR);

        // Test consistent generation of hashCode
        assert (a.hashCode() == a.hashCode());
        assert (a.equals(a));

        SQLServerDataColumn aClone = new SQLServerDataColumn("foo", Types.VARCHAR);

        // Test for different instances generating same hashCode for same data
        assert (a.hashCode() == aClone.hashCode());
        assert (a.equals(aClone));

        SQLServerDataTable table = new SQLServerDataTable();
        table.addColumnMetadata(a);

        // Test consistent generation of hashCode
        assert (table.hashCode() == table.hashCode());
        assert (table.equals(table));

        SQLServerDataTable tableClone = new SQLServerDataTable();
        tableClone.addColumnMetadata(aClone);

        // Test for different instances generating same hashCode for same data
        assert (table.hashCode() == tableClone.hashCode());
        assert (table.equals(tableClone));

        SQLServerDataColumn b = new SQLServerDataColumn("bar", Types.BOOLEAN);

        // Test for non equal hashCodes
        assert (a.hashCode() != b.hashCode());
        assert (!a.equals(b));

        table.addColumnMetadata(b);

        // Test for non equal hashCodes
        assert (table.hashCode() != tableClone.hashCode());
        assert (!table.equals(tableClone));
    }
}
