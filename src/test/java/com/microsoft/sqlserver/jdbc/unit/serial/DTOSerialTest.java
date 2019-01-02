package com.microsoft.sqlserver.jdbc.unit.serial;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;

import microsoft.sql.DateTimeOffset;


@RunWith(JUnitPlatform.class)
public class DTOSerialTest extends AbstractTest {
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss XXX");
    private static String dateString;

    @Test
    public void testDSerial() throws Exception {
        sdf.setTimeZone(TimeZone.getTimeZone("Z"));
        dateString = sdf.format(new Date());

        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {

            // create a DTO
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT CAST('" + dateString + "' AS datetimeoffset(7)) AS" + "   'datetimeoffset IS08601' ")) {
                rs.next();
                verifyCorrectSerialization(((SQLServerResultSet) rs).getDateTimeOffset(1));
                verifyMessedSerialization();
            }
        }
    }

    @Test
    public void testESerial() throws Exception {
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
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutputStream out = new ObjectOutputStream(bos)) {

                // serialize the exception;
                out.writeObject(currException);
                try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
                    SQLServerException ex = (SQLServerException) in.readObject();

                    assertEquals(ex.toString(), currException.toString());
                    assertEquals(ex.getSQLState(), currException.getSQLState());
                    assertEquals(ex.getErrorCode(), currException.getErrorCode());
                }
            }
        }
    }

    // Positive test case, this should succeed
    @Test
    private static void verifyCorrectSerialization(DateTimeOffset dto) throws Exception {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bos)) {

            // serialize the DateTimeOffset;
            out.writeObject(dto);
            try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
                DateTimeOffset dtn = (DateTimeOffset) in.readObject();
                verifyDTOEqual(dto, dtn);

                // Make sure that you can send rehydrated to server
                verifyCorrectSend(dtn);
            }
        }
    }

    // this is to make sure that the rehydrated date can be sent to server correctly
    @Test
    private static void verifyCorrectSend(DateTimeOffset dtn) throws Exception {
        // create a DTO
        try (Connection conn = DriverManager.getConnection(connectionString);
                SQLServerPreparedStatement ps = (SQLServerPreparedStatement) conn
                        .prepareStatement("SELECT CAST(? AS datetimeoffset(7)) AS" + "   'datetimeoffset IS08601' ")) {
            ps.setDateTimeOffset(1, dtn);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                verifyDTOEqual(dtn, ((SQLServerResultSet) rs).getDateTimeOffset(1));
            }
        }
    }

    // Negative test cases.
    @Test
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
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        }

        exThrown = false;
        try {
            verifyMessedSerializationHelper(wrongOffset);
        } catch (IllegalArgumentException e) {
            exThrown = true;
        }

        if (!exThrown) {
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        }
    }

    private static void verifyMessedSerializationHelper(byte[] svalue) throws Exception {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(svalue))) {
            // this will throw error for negative tests
            DateTimeOffset dtn = (DateTimeOffset) in.readObject();
        }
    }

    // This is used to make sure the hydrated is equal to original string and the initial DTO
    private static void verifyDTOEqual(DateTimeOffset initial, DateTimeOffset hydrated) throws Exception {
        String initialStr = initial.toString();
        String hydratedStr = hydrated.toString();

        java.sql.Timestamp originalTS = initial.getTimestamp();
        java.sql.Timestamp hydratedTS = hydrated.getTimestamp();

        // and offset
        int initiallOffset = initial.getMinutesOffset();
        int hydratedOffset = hydrated.getMinutesOffset();

        // check hydrated string
        assertEquals(initialStr, hydratedStr);

        // check formatted date string
        String formattedDate = sdf.format(sdf.parse(initialStr));

        assertEquals(dateString, formattedDate);

        // check hydrated datetimeoffset
        assertEquals(initial, hydrated);

        // check hydrated timestamp
        assertEquals(originalTS, hydratedTS);

        // check hydrated offset
        assertEquals(initiallOffset, hydratedOffset);
    }
}
