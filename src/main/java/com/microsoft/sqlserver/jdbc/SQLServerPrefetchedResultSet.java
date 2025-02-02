package com.microsoft.sqlserver.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;

public class SQLServerPrefetchedResultSet extends SQLServerResultSet {

    private static final BigInteger MIN_VALUE_LONG_BI = new BigInteger(String.valueOf(Long.MIN_VALUE));
    private static final BigInteger MAX_VALUE_LONG_BI = new BigInteger(String.valueOf(Long.MAX_VALUE));
    private static final BigDecimal MIN_VALUE_LONG_BD = new BigDecimal(String.valueOf(Long.MIN_VALUE));
    private static final BigDecimal MAX_VALUE_LONG_BD = new BigDecimal(String.valueOf(Long.MAX_VALUE));

    private Object[] row = null;

    SQLServerPrefetchedResultSet(SQLServerStatement stmtIn) throws SQLServerException {
        super(stmtIn);
        this.row = new Object[this.columns.length];
    }

    private void loadRow() throws SQLServerException {
        initializeNullCompressedColumns();
        // Iterate on columns and get values and keep them in row
        for (int index = 1; index <= this.columns.length; ++index) {
            Column c = getColumn(index);
            // Column c = columns[index - 1];
            Object o = c.getValue(c.getTypeInfo().getSSType().getJDBCType(), null, null, tdsReader, stmt);
            row[index - 1] = o;
        }
    }

    @Override
    public boolean next() throws SQLServerException {
        super.checkClosed();
        boolean r = super.next();
        if (r) {
            loadRow();
        }
        return r;
    }

    @Override
    protected void checkClosed() throws SQLServerException {
        // Do nothing
    }

    @Override
    protected Object getValue(int columnIndex, JDBCType jdbcType, InputStreamGetterArgs getterArgs, Calendar cal)
            throws SQLServerException {
        Object o = row[columnIndex - 1];
        lastValueWasNull = (null == o);
        return o;
    }

    //TODO
    //implement methods like below for all data types to convert data appropriately and return.
    
    @Override
    public long getLong(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getLong", columnIndex);
        checkClosed();
        Object o = getValue(columnIndex, JDBCType.BIGINT);
        if (o == null) {
            return 0;
        }
        if (o instanceof BigDecimal) {
            BigDecimal val = (BigDecimal) o;
            if (val.compareTo(MIN_VALUE_LONG_BD) < 0 || val.compareTo(MAX_VALUE_LONG_BD) > 0) {
                // TODO: change this to standard exception
                throw new SQLServerException("Numeric Overflow", null);
            } else {
                return new Long(val.longValue());
            }
        } else if (o instanceof Long) {
            return (Long) o;
        } else if (o instanceof Boolean) {
            return ((Boolean) o).booleanValue() ? 1L : 0L;
        } else if (o instanceof Byte) {
            return new Long(((Byte) o).byteValue() & 0xFF);
        } else if (o instanceof BigInteger) {
            BigInteger val = (BigInteger) o;
            if (val.compareTo(MIN_VALUE_LONG_BI) < 0 || val.compareTo(MAX_VALUE_LONG_BI) > 0) {
                // TODO: change this to standard exception
                throw new SQLServerException("Numeric Overflow", null);
            } else {
                return val.longValue();
            }
        } else if (o instanceof Number) {
            return new Long(((Number) o).longValue());
        } else if (o instanceof String) {
            return new Long(((String) o).trim());
        }
        loggerExternal.exiting(getClassNameLogging(), "getLong", o);
        // return null != o ? Long.valueOf(o.toString()) : 0;
        return 0;
    }

/*
    @Override 
    protected void skipColumns(int columnsToSkip, boolean discardValues) throws SQLServerException { 
        // Do nothing
        //super.skipColumns(columnsToSkip, discardValues); 
    }     
*/
}
