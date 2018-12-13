package com.microsoft.sqlserver.jdbc.unit.serial;


import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;

import microsoft.sql.DateTimeOffset;


@RunWith(JUnitPlatform.class)
public class DTOSerialTest extends AbstractTest {
    private static final String dateString = "2007-05-08 12:35:29.1234567 +12:15";

    // public static void testDSerial(String connString) throws Exception
    @Test
    public void testDSerial() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {

            // create a DTO
            ResultSet rs = stmt.executeQuery(
                    "SELECT CAST('" + dateString + "' AS datetimeoffset(7)) AS" + "   'datetimeoffset IS08601' ");
            rs.next();
            verifyCorrectSerialization(((SQLServerResultSet) rs).getDateTimeOffset(1));
            verifyMessedSerialization();
        }
    }

    public void testESerial() throws Exception {
        String connectionString = TestUtils.getConfiguredProperty("mssql_jdbc_test_connection_properties");

        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {

            // raise an error.
            SQLServerException currException = null;

            try {
                stmt.executeUpdate("RAISERROR ('foo', 13,1) WITH LOG");
            } catch (SQLServerException x) {
                currException = x;
            }
            // store the info
            String errInfo = currException.toString();
            String sqlState = currException.getSQLState();
            int errCode = currException.getErrorCode();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            // serialize the exception;
            out.writeObject(currException);
            ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
            SQLServerException ex = (SQLServerException) in.readObject();
            String newErrInfo = ex.toString();

            if (!errInfo.equals(newErrInfo)) {
                fail("Errors are different.");
            }
            if (sqlState != ex.getSQLState()) {
                fail("Sql states are different.");
            }
            if (errCode != ex.getErrorCode()) {
                fail("Error codes are different.");
            }
        }
    }

    // Positive test case, this should succeed
    private static void verifyCorrectSerialization(DateTimeOffset dto) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        // serialize the DateTimeOffset;
        out.writeObject(dto);
        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
        DateTimeOffset dtn = (DateTimeOffset) in.readObject();
        verifyDTOEqual(dto, dtn);
        // Make sure that you can send rehydrated to server
        verifyCorrectSend(dtn);
    }

    // this is to make sure that the rehydrated date can be sent to server correctly
    private static void verifyCorrectSend(DateTimeOffset dtN) throws Exception {
        String connectionString = TestUtils.getConfiguredProperty("mssql_jdbc_test_connection_properties");

        // create a DTO
        try (Connection conn = DriverManager.getConnection(connectionString);
                SQLServerPreparedStatement ps = (SQLServerPreparedStatement) conn
                        .prepareStatement("SELECT CAST(? AS datetimeoffset(7)) AS" + "   'datetimeoffset IS08601' ")) {
            ps.setDateTimeOffset(1, dtN);
            ResultSet rs = ps.executeQuery();
            rs.next();
            verifyDTOEqual(dtN, ((SQLServerResultSet) rs).getDateTimeOffset(1));
        }
    }

    // Negative test cases.
    private static void verifyMessedSerialization() throws Exception {
        // serialized DTO class with wrong nano values (-1)
        byte wrongNanos[] = {-84, -19, 0, 5, 115, 114, 0, 47, 109, 105, 99, 114, 111, 115, 111, 102, 116, 46, 115, 113,
                108, 46, 68, 97, 116, 101, 84, 105, 109, 101, 79, 102, 102, 115, 101, 116, 36, 83, 101, 114, 105, 97,
                108, 105, 122, 97, 116, 105, 111, 110, 80, 114, 111, 120, 121, 9, 57, 90, 0, -49, -42, -72, 50, 2, 0, 3,
                73, 0, 13, 109, 105, 110, 117, 116, 101, 115, 79, 102, 102, 115, 101, 116, 73, 0, 5, 110, 97, 110, 111,
                115, 74, 0, 9, 117, 116, 99, 77, 105, 108, 108, 105, 115, 120, 112, 0, 0, 3, 12, -1, -1, -1, -1, 0, 0,
                0, 0, 0, 0, 0, 1};

        // serialized DTO class with wrong offset (15*60)
        byte wrongOffset[] = {-84, -19, 0, 5, 115, 114, 0, 47, 109, 105, 99, 114, 111, 115, 111, 102, 116, 46, 115, 113,
                108, 46, 68, 97, 116, 101, 84, 105, 109, 101, 79, 102, 102, 115, 101, 116, 36, 83, 101, 114, 105, 97,
                108, 105, 122, 97, 116, 105, 111, 110, 80, 114, 111, 120, 121, 9, 57, 90, 0, -49, -42, -72, 50, 2, 0, 3,
                73, 0, 13, 109, 105, 110, 117, 116, 101, 115, 79, 102, 102, 115, 101, 116, 73, 0, 5, 110, 97, 110, 111,
                115, 74, 0, 9, 117, 116, 99, 77, 105, 108, 108, 105, 115, 120, 112, 0, 0, 3, -124, 0, 0, 1, 44, 0, 0, 0,
                0, 0, 0, 0, 1};

        // These two serialized forms throw the exception IllegalArgumentException
        boolean exThrown = false;
        try {
            verifyMessedSerializationHelper(wrongNanos);
        } catch (IllegalArgumentException e) {
            exThrown = true;
        }

        if (!exThrown) {
            fail("wrongnanos serialized form succeeded.");
        }

        exThrown = false;
        try {
            verifyMessedSerializationHelper(wrongOffset);
        } catch (IllegalArgumentException e) {
            exThrown = true;
        }

        if (!exThrown) {
            fail("wrongnanos serialized form succeeded.");
        }
    }

    private static void verifyMessedSerializationHelper(byte[] svalue) throws Exception {
        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(svalue));
        DateTimeOffset dtn = (DateTimeOffset) in.readObject();
    }

    // This function is used to make sure the hydrated is equal to original string and the initial DTO
    private static void verifyDTOEqual(DateTimeOffset initial, DateTimeOffset hydrated) throws Exception {
        // check string
        String info = initial.toString();
        String newInfo = hydrated.toString();
        // check timestamp
        java.sql.Timestamp originalTS = initial.getTimestamp();
        java.sql.Timestamp hydratedTS = hydrated.getTimestamp();
        // and offset
        int originalOffset = initial.getMinutesOffset();
        int hydratedOffset = hydrated.getMinutesOffset();

        if (!info.equals(newInfo)) {
            fail("Strings are different.");
        }
        if (!info.equals(dateString)) {
            fail("Strings are different from original.");
        }
        if (!initial.equals(hydrated)) {
            fail("Equality test fails.");
        }
        if (!originalTS.equals(hydratedTS)) {
            fail("Equality test fails from original.");
        }
    }
}
