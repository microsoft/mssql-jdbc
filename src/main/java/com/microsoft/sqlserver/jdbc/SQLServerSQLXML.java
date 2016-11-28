//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerSQLXML.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the ""Software""), 
//  to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
//  and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
//  IN THE SOFTWARE.
//---------------------------------------------------------------------------------------------------------------------------------


package com.microsoft.sqlserver.jdbc;

import java.sql.*;
import java.io.*;

import javax.xml.XMLConstants;
import javax.xml.parsers.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.sax.*;
import javax.xml.transform.*;

import org.w3c.dom.*;

import java.text.*;

import org.xml.sax.*;

import javax.xml.transform.stax.*;
import javax.xml.stream.*;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.xml.sax.helpers.XMLReaderFactory;


/**
 * SQLServerSQLXML represents an XML object and implements a java.sql.SQLXML.
 */

final class SQLServerSQLXML implements java.sql.SQLXML {
    // Connection that created this SQLXML only set when created for setting data
    private final SQLServerConnection con;
    // Contents null means this is a setter. 
    private final PLPXMLInputStream contents;
    private final InputStreamGetterArgs getterArgs;
    private final TypeInfo typeInfo;

    private boolean isUsed = false;
    private boolean isFreed = false;

    static private final java.util.logging.Logger logger =
            java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerSQLXML");

    // Setter values are held in different forms depending on which setter is used only one setter can 
    // be used at one time.
    private ByteArrayOutputStreamToInputStream outputStreamValue;
    private Document docValue;
    private String strValue;
    // End of setter values

    static private int baseID = 0;    // Unique id generator for each  instance (used for logging).
    final private String traceID;

    final public String toString() {
        return traceID;
    }

    // Returns unique id for each instance.
    private synchronized static int nextInstanceID() {
        baseID++;
        return baseID;
    }

    // This method is used to get the value the user has set 
    // This is used at the execution time when sending the data to the server
    // possible optimization transform DOM straight to tds
    InputStream getValue() throws SQLServerException {
        checkClosed();
        // the user should have called one of the setter methods, the setter methods "use" the object and write some value.
        // Note just calling one of the setters is enough.
        if (!isUsed)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_noDataXML"), null, true);
        assert null == contents;
        ByteArrayInputStream o = null;
        if (null != outputStreamValue) {
            o = outputStreamValue.getInputStream();
            assert null == docValue;
            assert null == strValue;
        } else if (null != docValue) {
            assert null == outputStreamValue;
            assert null == strValue;
            ByteArrayOutputStreamToInputStream strm = new ByteArrayOutputStreamToInputStream();
            // Need to beat a stream out of docValue
            TransformerFactory factory;
            Writer wr = null;
            try {
                factory = TransformerFactory.newInstance();
                factory.newTransformer().transform(new DOMSource(docValue), new StreamResult(strm));
            } catch (javax.xml.transform.TransformerException e) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_noParserSupport"));
                Object[] msgArgs = {e.toString()};
                SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
            }
            o = strm.getInputStream();

            try {
                strm.close();
            } catch (IOException e) {
                throw new SQLServerException(null, e.getMessage(), null, 0, true);
            }
        } else {
            assert null == outputStreamValue;
            assert null == docValue;
            assert null != strValue;
            try {
                o = new ByteArrayInputStream(strValue.getBytes(Encoding.UNICODE.charsetName()));
            } catch (UnsupportedEncodingException ex) {
                throw new SQLServerException(null, ex.getMessage(), null, 0, true);
            }
        }
        assert null != o;
        isFreed = true; // we have consumed the data 
        return o;
    }

    // To be used in the createSQLXML call
    SQLServerSQLXML(SQLServerConnection connection) {
        contents = null;
        traceID = " SQLServerSQLXML:" + nextInstanceID();
        con = connection;

        if (logger.isLoggable(java.util.logging.Level.FINE))
            logger.fine(toString() + " created by (" + connection.toString() + ")");
        getterArgs = null; // make the compiler happy
        typeInfo = null;
    }

    SQLServerSQLXML(InputStream stream, InputStreamGetterArgs getterArgs, TypeInfo typeInfo) throws SQLServerException {
        traceID = " SQLServerSQLXML:" + nextInstanceID();
        contents = (PLPXMLInputStream) stream;
        this.con = null;
        this.getterArgs = getterArgs;
        this.typeInfo = typeInfo;
        if (logger.isLoggable(java.util.logging.Level.FINE))
            logger.fine(toString() + " created by (null connection)");
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
            SQLServerException.makeFromDriverError(con, null, form.format(new Object[]{"SQLXML"}), null, true);
        }
    }

    private void checkReadXML() throws SQLException {
        if (null == contents)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_writeOnlyXML"), null, true);
        if (isUsed)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_dataHasBeenReadXML"), null, true);
        try {
            contents.checkClosed();
        } catch (IOException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_isFreed"));
            SQLServerException.makeFromDriverError(con, null, form.format(new Object[]{"SQLXML"}), null, true);
        }
    }

    void checkWriteXML() throws SQLException {
        if (null != contents)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_readOnlyXML"), null, true);
        if (isUsed)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_dataHasBeenSetXML"), null, true);

    }

    /**
     * Return an input stream to read data from this SQLXML
     *
     * @return the input stream to that contains the SQLXML data
     * @throws SQLServerException
     */
    public InputStream getBinaryStream() throws SQLException {
        checkClosed();
        checkReadXML();
        isUsed = true;
        return contents;
    }

    /**
     * Retrieves a stream that can be used to write to the SQLXML value that this SQLXML object represents
     * The user has to write the BOM for binary streams.
     *
     * @param pos the position in the SQLXML value at which to start writing
     * @return OutputStream
     * @throws SQLException
     */
    public java.io.OutputStream setBinaryStream() throws SQLException {
        checkClosed();
        checkWriteXML();
        isUsed = true;
        outputStreamValue = new ByteArrayOutputStreamToInputStream();
        return outputStreamValue;
    }

    public java.io.Writer setCharacterStream() throws SQLException {
        checkClosed();
        checkWriteXML();
        isUsed = true;
        outputStreamValue = new ByteArrayOutputStreamToInputStream();
        java.io.Writer wrt = null;
        try {
            wrt = new OutputStreamWriter(outputStreamValue, Encoding.UNICODE.charsetName());
        } catch (UnsupportedEncodingException ex) {
            throw new SQLServerException(null, ex.getMessage(), null, 0, true);
        }
        return wrt;
    }

    public Reader getCharacterStream() throws SQLException {
        checkClosed();
        checkReadXML();
        isUsed = true;
        StreamType type = StreamType.CHARACTER;
        InputStreamGetterArgs newArgs = new InputStreamGetterArgs(
                type,
                getterArgs.isAdaptive,
                getterArgs.isStreaming,
                getterArgs.logContext);
        // Skip the BOM bytes from the plpinputstream we do not need it  for the conversion
        assert null != contents;
        // Read two bytes to eat BOM
        try {
            contents.read();
            contents.read();
        } catch (IOException e) {
            SQLServerException.makeFromDriverError(null, null, e.getMessage(), null, true);
        }

        Reader rd = (Reader) DDC.convertStreamToObject(contents, typeInfo, type.getJDBCType(), newArgs);
        return rd;
    }

    public String getString() throws SQLException {
        checkClosed();
        checkReadXML();
        isUsed = true;
        assert null != contents;
        // Read two bytes to eat BOM
        try {
            contents.read();
            contents.read();
        } catch (IOException e) {
            SQLServerException.makeFromDriverError(null, null, e.getMessage(), null, true);
        }

        byte byteContents[] = contents.getBytes();
        String ret = null;
        try {
            ret = new String(byteContents, 0, byteContents.length, Encoding.UNICODE.charsetName());
        } catch (UnsupportedEncodingException ex) {
            throw new SQLServerException(null, ex.getMessage(), null, 0, true);
        }
        return ret;
    }

    public void setString(String value) throws SQLException {
        checkClosed();
        checkWriteXML();
        isUsed = true;
        if (null == value)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_cantSetNull"), null, true);
        strValue = value;
    }
    // Support the following DOMSource, SAXSource, StAX and Stream. Also, null means default which is stream source

    public <T extends Source> T getSource(Class<T> iface) throws SQLException {
        checkClosed();
        checkReadXML();
        if (null == iface) {
            // The compiler does not allow setting the null iface to StreamSource.class hence
            // this cast is needed, the cast is safe.
            @SuppressWarnings("unchecked") T src = (T) getSourceInternal(StreamSource.class);
            return src;
        } else
            return getSourceInternal(iface);
    }

    <T extends Source> T getSourceInternal(Class<T> iface) throws SQLException {
        isUsed = true;
        T src = null;
        if (DOMSource.class == iface) {
            src = iface.cast(getDOMSource());
        } else if (SAXSource.class == iface) {
            src = iface.cast(getSAXSource());
        } else if (StAXSource.class == iface) {
            src = iface.cast(getStAXSource());
        } else if (StreamSource.class == iface) {
            src = iface.cast(new StreamSource(contents));
        } else
            // Do not support this type
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_notSupported"), null, true);
        return src;
    }

    public <T extends Result> T setResult(Class<T> resultClass) throws SQLException {
        checkClosed();
        checkWriteXML();
        if (null == resultClass) {
            // The compiler does not allow setting the null resultClass to StreamResult.class hence
            // this cast is needed, the cast is safe.
            @SuppressWarnings("unchecked") T result = (T) setResultInternal(StreamResult.class);
            return result;
        } else
            return setResultInternal(resultClass);

    }

    <T extends Result> T setResultInternal(Class<T> resultClass) throws SQLException {
        isUsed = true;
        T result = null;
        if (DOMResult.class == resultClass) {
            result = resultClass.cast(getDOMResult());
        } else if (SAXResult.class == resultClass) {
            result = resultClass.cast(getSAXResult());
        } else if (StAXResult.class == resultClass) {
            result = resultClass.cast(getStAXResult());
        } else if (StreamResult.class == resultClass) {
            outputStreamValue = new ByteArrayOutputStreamToInputStream();
            result = resultClass.cast(new StreamResult(outputStreamValue));
        } else
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_notSupported"), null, true);
        return result;
    }

    // Supporting functions
    private final DOMSource getDOMSource() throws SQLException {
        Document document = null;
        final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        try {
            //Secure feature processing is false by default for IBM parser
            //For all others, it is true by default.
            //So, setting it explicitly for all, irrespective of the JVM.
            //This limits the expansion of entities.
            //The limit is implementation specific. For IBM it's 100,000
            //whereas for oracle it is 64,000.
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            builder = factory.newDocumentBuilder();

            //set an entity resolver to disable parsing of external entities
            builder.setEntityResolver(new SQLServerEntityResolver());
            try {
                document = builder.parse(contents);
            } catch (IOException e) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
                Object[] msgArgs = {e.toString()};
                SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), "", true);
            }
            DOMSource inputSource = new DOMSource(document);
            return inputSource;

        } catch (ParserConfigurationException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_noParserSupport"));
            Object[] msgArgs = {e.toString()};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        } catch (SAXException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_failedToParseXML"));
            Object[] msgArgs = {e.toString()};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }
        return null;
    }

    private final SAXSource getSAXSource() throws SQLException {
        try {
            InputSource src = new InputSource(contents);
            XMLReader reader = XMLReaderFactory.createXMLReader();
            SAXSource saxSource = new SAXSource(reader, src);
            return saxSource;

        } catch (SAXException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_failedToParseXML"));
            Object[] msgArgs = {e.toString()};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }
        return null;
    }

    private final StAXSource getStAXSource() throws SQLException {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        try {
            XMLStreamReader r = factory.createXMLStreamReader(contents);
            StAXSource result = new StAXSource(r);
            return result;

        } catch (XMLStreamException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_noParserSupport"));
            Object[] msgArgs = {e.toString()};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }
        return null;
    }

    private final StAXResult getStAXResult() throws SQLException {
        XMLOutputFactory factory = XMLOutputFactory.newInstance();
        outputStreamValue = new ByteArrayOutputStreamToInputStream();
        try {
            XMLStreamWriter r = factory.createXMLStreamWriter(outputStreamValue);
            StAXResult result = new StAXResult(r);
            return result;

        } catch (XMLStreamException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_noParserSupport"));
            Object[] msgArgs = {e.toString()};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }
        return null;
    }

    private final SAXResult getSAXResult() throws SQLException {
        TransformerHandler handler = null;
        try {
            SAXTransformerFactory stf =
                    (SAXTransformerFactory) TransformerFactory.newInstance();
            handler = stf.newTransformerHandler();
        } catch (TransformerConfigurationException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_noParserSupport"));
            Object[] msgArgs = {e.toString()};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        } catch (ClassCastException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_noParserSupport"));
            Object[] msgArgs = {e.toString()};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }
        outputStreamValue = new ByteArrayOutputStreamToInputStream();
        handler.setResult(new StreamResult(outputStreamValue));
        SAXResult result = new SAXResult(handler);
        return result;
    }

    private final DOMResult getDOMResult() throws SQLException {
        final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        assert null == outputStreamValue;
        try {
            builder = factory.newDocumentBuilder();
            docValue = builder.newDocument();
            DOMResult result = new DOMResult(docValue);
            return result;

        } catch (ParserConfigurationException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_noParserSupport"));
            Object[] msgArgs = {e.toString()};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }
        return null;
    }

}

// We use this class to convert the byte information we have in the string to 
// an inputstream which is used when sending to the info to the server
final class ByteArrayOutputStreamToInputStream extends ByteArrayOutputStream {
    ByteArrayInputStream getInputStream() throws SQLServerException {
        ByteArrayInputStream is = new ByteArrayInputStream(buf, 0, count);
        return is;
    }
}

//Resolves External entities in an XML with inline DTDs to empty string
final class SQLServerEntityResolver implements EntityResolver {
    public InputSource resolveEntity(String publicId, String systemId) {
        return new InputSource(new StringReader(""));
    }
}
