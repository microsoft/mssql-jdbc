/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents an JSON object
 */
final class SQLServerSQLJSON {

    // Connection that created this JSON only set when created for setting data
    private final SQLServerConnection con;
    // Contents null means this is a setter.
    private final PLPInputStream contents;
    private final InputStreamGetterArgs getterArgs;
    private final TypeInfo typeInfo;

    private boolean isUsed = false;
    private boolean isFreed = false;
    private String strValue;

    static private final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerSQLJSON");

    // Setter values are held in different forms depending on which setter is used only one setter can
    // be used at one time.
    private ByteArrayOutputStreamToInputStream outputStreamValue;
    
    static private final AtomicInteger baseID = new AtomicInteger(0); // Unique id generator for each instance (used for
                                                                      // logging).
    final private String traceID;

    final public String toString() {
        return traceID;
    }
    
    
    /**
     * Returns unique id for each instance.
     * 
     * @return
     */
    private static int nextInstanceID() {
        return baseID.incrementAndGet();
    }

    public void setString(String value) throws SQLException {
        checkClosed();
        checkWriteJSON();
        isUsed = true;
        if (null == value)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_cantSetNull"), null,
                    true);
        strValue = value;
    }

    InputStream getValue() throws SQLServerException {
        checkClosed();
            if (!isUsed)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_noDataXML"), null,
                    true);
        assert null == contents;
        ByteArrayInputStream o = null;
        if (null != outputStreamValue) {
            o = outputStreamValue.getInputStream();
            assert null == strValue;
        } else {
            assert null == outputStreamValue;
            assert null != strValue;
            o = new ByteArrayInputStream(strValue.getBytes(Encoding.UTF8.charset()));
        }
        assert null != o;
        isFreed = true; // we have consumed the data
        return o;
    }

    SQLServerSQLJSON(SQLServerConnection connection) {
         contents = null;
        traceID = " SQLServerSQLJSON:" + nextInstanceID();
        con = connection;

        if (logger.isLoggable(java.util.logging.Level.FINE))
            logger.fine(toString() + " created by (" + connection.toString() + ")");
        getterArgs = null; // make the compiler happy
        typeInfo = null;        
    }

    SQLServerSQLJSON(InputStream stream, InputStreamGetterArgs getterArgs, TypeInfo typeInfo) {
        traceID = " SQLServerSQLJSON:" + nextInstanceID();
        contents = (PLPInputStream) stream;
        this.con = null;
        this.getterArgs = getterArgs;
        this.typeInfo = typeInfo;
        if (logger.isLoggable(java.util.logging.Level.FINE))
            logger.fine(toString() + " created by (null connection)");  
   }

    private void checkReadJSON() throws SQLException {
        if (null == contents)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_writeOnlyJSON"), null,
                    true);
        if (isUsed)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_dataHasBeenReadJSON"),
                    null, true);
        try {
            contents.checkClosed();
        } catch (IOException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_isFreed"));
            SQLServerException.makeFromDriverError(con, null, form.format(new Object[] {"JSON"}), null, true);
        }
    }

    InputStream getStream() {
        return contents;
    }

    public void free() throws SQLException {
        if (!isFreed) {
            isFreed = true;
            if (null != contents) {
                try {
                    contents.close();
                } catch (IOException e) {
                    SQLServerException.makeFromDriverError(null, null, e.getMessage(), null, true);
                }
            }
        }
    }

    private void checkClosed() throws SQLServerException {
        if (isFreed || (null != con && con.isClosed())) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_isFreed"));
            SQLServerException.makeFromDriverError(con, null, form.format(new Object[] {"SQLXML"}), null, true);
        }
    }

    void checkWriteJSON() throws SQLException {
        if (null != contents)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_readOnlyJSON"), null,
                    true);
        if (isUsed)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_dataHasBeenSetJSON"),
                    null, true);

    }

    public InputStream getBinaryStream() throws SQLException {
        checkClosed();
        checkReadJSON();
        isUsed = true;
        return contents;
    }

    public java.io.OutputStream setBinaryStream() throws SQLException {
        checkClosed();
        checkWriteJSON();
        isUsed = true;
        outputStreamValue = new ByteArrayOutputStreamToInputStream();
        return outputStreamValue;
    }

    public java.io.Writer setCharacterStream() throws SQLException {
        checkClosed();
        checkWriteJSON();
        isUsed = true;
        outputStreamValue = new ByteArrayOutputStreamToInputStream();
        return new OutputStreamWriter(outputStreamValue, Encoding.UTF8.charset());
    }

    public Reader getCharacterStream() throws SQLException {
        checkClosed();
        checkReadJSON();
        isUsed = true;
        StreamType type = StreamType.CHARACTER;
        InputStreamGetterArgs newArgs = new InputStreamGetterArgs(type, getterArgs.isAdaptive, getterArgs.isStreaming,
                getterArgs.logContext);
        // Skip the BOM bytes from the plpinputstream we do not need it for the conversion
        assert null != contents;
        /* No need to skip BOM bytes as we are not using the BOM bytes for conversion
        // Read two bytes to eat BOM
        try {
            contents.read();
            contents.read();
        } catch (IOException e) {
            SQLServerException.makeFromDriverError(null, null, e.getMessage(), null, true);
        }*/

        return (Reader) DDC.convertStreamToObject(contents, typeInfo, type.getJDBCType(), newArgs);
    }

    public String getString() throws SQLException {
        checkClosed();
        checkReadJSON();
        isUsed = true;
        assert null != contents;
        /*// Read two bytes to eat BOM
        try {
            contents.read();
            contents.read();
        } catch (IOException e) {
            SQLServerException.makeFromDriverError(null, null, e.getMessage(), null, true);
        } */

        byte[] byteContents = contents.getBytes();
        return new String(byteContents, 0, byteContents.length, Encoding.UTF8.charset());
    }

}

