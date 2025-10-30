/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.SQLException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stax.StAXResult;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.w3c.dom.Document;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;

@DisplayName("Test SQLServerSQLXML")
@Tag(Constants.JSONTest)
public class SQLServerSQLXMLTest extends AbstractTest {

    @Mock
    private SQLServerConnection mockConnection;
    
    private SQLServerSQLXML sqlXmlSetter;
    private static final String TEST_XML = "<?xml version=\"1.0\"?><test><item>value</item></test>";
    private static final byte[] XML_BYTES = TEST_XML.getBytes();

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        when(mockConnection.isClosed()).thenReturn(false);
        sqlXmlSetter = new SQLServerSQLXML(mockConnection);
    }

    @Test
    @Tag("CodeCov")
    @DisplayName("Test complete SQLXML lifecycle and basic operations")
    void testCompleteLifecycle() throws Exception {
        // Test constructor and toString
        assertNotNull(sqlXmlSetter);
        assertTrue(sqlXmlSetter.toString().contains("SQLServerSQLXML"));
        
        // Test all setter methods work and mark object as used
        testAllSetterMethods();
        
        // Test getter methods throw SQLException on setter instance
        testGetterMethodsFailOnSetter();
        
        // Test free method and operations after free
        sqlXmlSetter.free();
        assertThrows(SQLException.class, () -> sqlXmlSetter.setString(TEST_XML));
        
        // Test static methods and ID generation
        testStaticMethods();
    }
    
    private void testAllSetterMethods() throws Exception {
        // Test setString
        SQLServerSQLXML xml1 = new SQLServerSQLXML(mockConnection);
        xml1.setString(TEST_XML);
        assertTrue(isUsed(xml1));
        
        // Test setBinaryStream
        SQLServerSQLXML xml2 = new SQLServerSQLXML(mockConnection);
        OutputStream os = xml2.setBinaryStream();
        assertNotNull(os);
        os.write(XML_BYTES);
        os.close();
        assertTrue(isUsed(xml2));
        
        // Test setCharacterStream
        SQLServerSQLXML xml3 = new SQLServerSQLXML(mockConnection);
        Writer writer = xml3.setCharacterStream();
        assertNotNull(writer);
        writer.write(TEST_XML);
        writer.close();
        assertTrue(isUsed(xml3));
        
        // Test setString with null throws exception
        SQLServerSQLXML xml4 = new SQLServerSQLXML(mockConnection);
        assertThrows(SQLException.class, () -> xml4.setString(null));
    }
    
    private void testGetterMethodsFailOnSetter() {
        assertThrows(SQLException.class, () -> sqlXmlSetter.getBinaryStream());
        assertThrows(SQLException.class, () -> sqlXmlSetter.getCharacterStream());
        assertThrows(SQLException.class, () -> sqlXmlSetter.getString());
    }
    
    private void testStaticMethods() throws Exception {
        Method nextInstanceIDMethod = SQLServerSQLXML.class.getDeclaredMethod("nextInstanceID");
        nextInstanceIDMethod.setAccessible(true);
        int id1 = (Integer) nextInstanceIDMethod.invoke(null);
        int id2 = (Integer) nextInstanceIDMethod.invoke(null);
        assertTrue(id2 > id1);
        
        Field loggerField = SQLServerSQLXML.class.getDeclaredField("logger");
        assertNotNull(loggerField);
    }

    @Test
    @Tag("CodeCov")
    @DisplayName("Test XML transformation support (Results and Sources)")
    void testXMLTransformationSupport() throws SQLException {
        // Test all Result types work on setter
        Class<?>[] resultTypes = {StreamResult.class, DOMResult.class, SAXResult.class, StAXResult.class};
        for (Class<?> resultType : resultTypes) {
            SQLServerSQLXML xml = new SQLServerSQLXML(mockConnection);
            @SuppressWarnings("unchecked")
            Class<Result> resultClass = (Class<Result>) resultType;
            Result result = xml.setResult(resultClass);
            assertNotNull(result);
            assertTrue(resultType.isInstance(result));
        }
        
        // Test null defaults to StreamResult and unsupported types fail
        Result result = sqlXmlSetter.setResult(null);
        assertTrue(result instanceof StreamResult);
        assertThrows(SQLException.class, () -> {
            @SuppressWarnings("unchecked")
            Class<Result> invalidClass = (Class<Result>) (Class<?>) String.class;
            sqlXmlSetter.setResult(invalidClass);
        });
        
        // Test all Source types fail on setter (getter-only functionality)
        Class<?>[] sourceTypes = {StreamSource.class, DOMSource.class, SAXSource.class, StAXSource.class};
        for (Class<?> sourceType : sourceTypes) {
            assertThrows(SQLException.class, () -> {
                @SuppressWarnings("unchecked")
                Class<Source> sourceClass = (Class<Source>) sourceType;
                sqlXmlSetter.getSource(sourceClass);
            });
        }
        
        // Test null and unsupported Source types also fail
        assertThrows(SQLException.class, () -> sqlXmlSetter.getSource(null));
        assertThrows(SQLException.class, () -> {
            @SuppressWarnings("unchecked")
            Class<Source> invalidClass = (Class<Source>) (Class<?>) String.class;
            sqlXmlSetter.getSource(invalidClass);
        });
    }

    @Test
    @Tag("CodeCov")
    @DisplayName("Test error conditions and internal state management")
    void testErrorConditionsAndInternalState() throws Exception {
        // Test operations on closed connection
        when(mockConnection.isClosed()).thenReturn(true);
        assertThrows(SQLException.class, () -> sqlXmlSetter.setString(TEST_XML));
        
        // Reset connection and test multiple setter calls
        when(mockConnection.isClosed()).thenReturn(false);
        SQLServerSQLXML xml = new SQLServerSQLXML(mockConnection);
        xml.setString(TEST_XML);
        assertThrows(SQLException.class, () -> xml.setString(TEST_XML));
        
        // Test getValue without setting data
        SQLServerSQLXML emptyXml = new SQLServerSQLXML(mockConnection);
        assertThrows(Exception.class, () -> getValueFromSetter(emptyXml));
        
        // Test internal validation methods via reflection
        testInternalValidationMethods();
        
        // Test DOM value transformation
        testDOMTransformation();
    }
    
    private void testInternalValidationMethods() throws Exception {
        SQLServerSQLXML xml = new SQLServerSQLXML(mockConnection);
        
        // Test checkClosed method
        Method checkClosedMethod = SQLServerSQLXML.class.getDeclaredMethod("checkClosed");
        checkClosedMethod.setAccessible(true);
        checkClosedMethod.invoke(xml); // Should not throw
        
        xml.free();
        assertThrows(Exception.class, () -> {
            try {
                checkClosedMethod.invoke(xml);
            } catch (Exception e) {
                throw e.getCause();
            }
        });
        
        // Test checkWriteXML method
        SQLServerSQLXML xml2 = new SQLServerSQLXML(mockConnection);
        Method checkWriteXMLMethod = SQLServerSQLXML.class.getDeclaredMethod("checkWriteXML");
        checkWriteXMLMethod.setAccessible(true);
        checkWriteXMLMethod.invoke(xml2); // Should not throw
        
        xml2.setString(TEST_XML);
        assertThrows(Exception.class, () -> {
            try {
                checkWriteXMLMethod.invoke(xml2);
            } catch (Exception e) {
                throw e.getCause();
            }
        });
        
        // Test checkReadXML method (should always throw on setter)
        Method checkReadXMLMethod = SQLServerSQLXML.class.getDeclaredMethod("checkReadXML");
        checkReadXMLMethod.setAccessible(true);
        assertThrows(Exception.class, () -> {
            try {
                checkReadXMLMethod.invoke(sqlXmlSetter);
            } catch (Exception e) {
                throw e.getCause();
            }
        });
    }
    
    private void testDOMTransformation() throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(new ByteArrayInputStream("<test/>".getBytes()));
        
        SQLServerSQLXML xml = new SQLServerSQLXML(mockConnection);
        setDOMValue(xml, doc);
        InputStream is = getValueFromSetter(xml);
        assertNotNull(is);
    }

    @Test
    @Tag("CodeCov")
    @DisplayName("Test utility classes and complete coverage")
    void testUtilityClassesAndCompleteCoverage() throws Exception {
        // Test ByteArrayOutputStreamToInputStream utility class
        Class<?> innerClass = Class.forName("com.microsoft.sqlserver.jdbc.ByteArrayOutputStreamToInputStream");
        Constructor<?> constructor = innerClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        Object instance = constructor.newInstance();
        
        Method writeMethod = innerClass.getMethod("write", int.class);
        Method getInputStreamMethod = innerClass.getDeclaredMethod("getInputStream");
        getInputStreamMethod.setAccessible(true);
        
        writeMethod.invoke(instance, 65); // Write 'A'
        InputStream is = (InputStream) getInputStreamMethod.invoke(instance);
        assertEquals(65, is.read());
        
        // Test SQLServerEntityResolver utility class
        Class<?> resolverClass = Class.forName("com.microsoft.sqlserver.jdbc.SQLServerEntityResolver");
        Constructor<?> resolverConstructor = resolverClass.getDeclaredConstructor();
        resolverConstructor.setAccessible(true);
        Object resolver = resolverConstructor.newInstance();
        
        Method resolveMethod = resolverClass.getMethod("resolveEntity", String.class, String.class);
        Object result = resolveMethod.invoke(resolver, "publicId", "systemId");
        assertNotNull(result);
        
        // Test getter instance creation (for complete coverage)
        testGetterInstanceCreation();
    }
    
    private void testGetterInstanceCreation() throws Exception {
        PLPXMLInputStream mockPLP = mock(PLPXMLInputStream.class);
        when(mockPLP.read()).thenReturn(-1);
        when(mockPLP.getBytes()).thenReturn(XML_BYTES);
        doNothing().when(mockPLP).checkClosed();
        
        InputStreamGetterArgs getterArgs = new InputStreamGetterArgs(StreamType.NONE, false, false, "test");
        
        Constructor<TypeInfo> typeInfoConstructor = TypeInfo.class.getDeclaredConstructor();
        typeInfoConstructor.setAccessible(true);
        TypeInfo typeInfo = typeInfoConstructor.newInstance();
        
        Constructor<SQLServerSQLXML> constructor = SQLServerSQLXML.class.getDeclaredConstructor(
            InputStream.class, InputStreamGetterArgs.class, TypeInfo.class);
        constructor.setAccessible(true);
        
        SQLServerSQLXML getter = constructor.newInstance(mockPLP, getterArgs, typeInfo);
        assertNotNull(getter);
        assertTrue(getter.toString().contains("SQLServerSQLXML"));
    }

    // Helper methods
    private InputStream getValueFromSetter(SQLServerSQLXML sqlxml) throws Exception {
        Method getValueMethod = SQLServerSQLXML.class.getDeclaredMethod("getValue");
        getValueMethod.setAccessible(true);
        return (InputStream) getValueMethod.invoke(sqlxml);
    }

    private void setDOMValue(SQLServerSQLXML sqlxml, Document doc) throws Exception {
        Field docField = SQLServerSQLXML.class.getDeclaredField("docValue");
        docField.setAccessible(true);
        docField.set(sqlxml, doc);
        
        Field isUsedField = SQLServerSQLXML.class.getDeclaredField("isUsed");
        isUsedField.setAccessible(true);
        isUsedField.set(sqlxml, true);
    }

    private boolean isUsed(SQLServerSQLXML sqlxml) throws Exception {
        Field field = SQLServerSQLXML.class.getDeclaredField("isUsed");
        field.setAccessible(true);
        return (Boolean) field.get(sqlxml);
    }
}
