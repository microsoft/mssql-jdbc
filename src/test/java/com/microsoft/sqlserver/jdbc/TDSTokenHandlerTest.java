package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit test for TDSTokenHandler onDone method, specifically testing
 * severity 25 (fatal) error handling and SQLServerError generation.
 */
public class TDSTokenHandlerTest {

    private static final String INVALID_TDS = "simulated invalid TDS";
    private SQLServerConnection con;
    private TDSCommand command;

    @BeforeEach
    void setUpPlpStreams() throws Exception {
        con = mock(SQLServerConnection.class);
        doThrow(new SQLServerException(INVALID_TDS, null)).when(con).throwInvalidTDS();
        command = mock(TDSCommand.class);
        when(command.getTDSWriter()).thenReturn(mock(TDSWriter.class));
        when(command.readingResponse()).thenReturn(false);
    }

    @ParameterizedTest
    @ValueSource(longs = {0x80000000L, 0xFFFFFFFFL, 0x100000000L, Long.MAX_VALUE, Long.MIN_VALUE, -3L})
    void testOutOfRangeDeclaredLengthIsRejected(long length) throws Exception {
        SQLServerException e = assertThrows(SQLServerException.class,
                () -> PLPInputStream.makeStream(reader(header(length)), InputStreamGetterArgs.getDefaultArgs(), null));
        assertEquals(INVALID_TDS, e.getMessage());
        e = assertThrows(SQLServerException.class, () -> PLPXMLInputStream.makeXMLStream(reader(header(length)),
                InputStreamGetterArgs.getDefaultArgs(), null));
        assertEquals(INVALID_TDS, e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(longs = {Integer.MAX_VALUE, PLPInputStream.UNKNOWN_PLP_LEN})
    void testAccumulatedSizeIsLimitedWhenSkipping(long declaredLength) throws Exception {
        TDSReader reader = mockReader();
        when(reader.readUnsignedInt()).thenReturn((long) Integer.MAX_VALUE, 1L);
        try (PLPInputStream stream = new PLPInputStream(reader, declaredLength, false, false, null)) {
            IOException e = assertThrows(IOException.class, () -> stream.skip(Integer.MAX_VALUE));
            assertEquals(INVALID_TDS, e.getCause().getMessage());
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {Integer.MAX_VALUE, PLPInputStream.UNKNOWN_PLP_LEN})
    void testMaximumTotalSizeIsAcceptedWithoutAllocatingPayload(long declaredLength) throws Exception {
        TDSReader reader = mockReader();
        when(reader.readUnsignedInt()).thenReturn((long) Integer.MAX_VALUE - 1, 1L, 0L);
        try (PLPInputStream stream = new PLPInputStream(reader, declaredLength, false, false, null)) {
            assertEquals(Integer.MAX_VALUE, stream.skip(Integer.MAX_VALUE));
            assertEquals(-1, stream.read());
        }
    }

    @Test
    void testChunksExceedingKnownLengthAreRejected() throws Exception {
        PLPInputStream stream = PLPInputStream.makeStream(reader(plp(2, new byte[] {1, 2, 3})),
                InputStreamGetterArgs.getDefaultArgs(), null);
        IOException e = assertThrows(IOException.class, stream::available);
        assertEquals(INVALID_TDS, e.getCause().getMessage());
    }

    @ParameterizedTest
    @ValueSource(longs = {5, PLPInputStream.UNKNOWN_PLP_LEN})
    void testNormalMultiChunkValue(long declaredLength) throws Exception {
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5},
                readPlp(plp(declaredLength, new byte[] {1, 2}, new byte[] {3, 4, 5})));
    }

    @Test
    void testEmptyValues() throws Exception {
        assertArrayEquals(new byte[0], readPlp(plp(0)));
        assertArrayEquals(new byte[0], readPlp(plp(PLPInputStream.UNKNOWN_PLP_LEN)));
    }

    @Test
    void testXmlValue() throws Exception {
        PLPXMLInputStream stream = PLPXMLInputStream.makeXMLStream(reader(plp(4, new byte[] {1, 2, 3, 4})),
                InputStreamGetterArgs.getDefaultArgs(), null);
        assertArrayEquals(new byte[] {(byte) 0xFF, (byte) 0xFE, 1, 2, 3, 4}, stream.getBytes());
    }

    @Test
    void testMarkResetRestoresSizeAccounting() throws Exception {
        TDSReader reader = mockReader();
        when(reader.mark()).thenReturn(mock(TDSReaderMark.class));
        when(reader.readUnsignedInt()).thenReturn((long) Integer.MAX_VALUE - 1, 1L, 0L, 1L, 0L);
        try (PLPInputStream stream = new PLPInputStream(reader, PLPInputStream.UNKNOWN_PLP_LEN, false, false, null)) {
            assertEquals(1, stream.skip(1));
            stream.mark(Integer.MAX_VALUE);
            assertEquals((long) Integer.MAX_VALUE - 1, stream.skip(Integer.MAX_VALUE));
            stream.reset();
            assertEquals((long) Integer.MAX_VALUE - 1, stream.skip(Integer.MAX_VALUE));
        }
    }

    @Test
    void testMarkResetAcrossRealChunkBoundaries() throws Exception {
        PLPInputStream stream = PLPInputStream.makeStream(reader(plp(5, new byte[] {1, 2}, new byte[] {3, 4, 5})),
                InputStreamGetterArgs.getDefaultArgs(), null);
        assertEquals(1, stream.read());
        stream.mark(10);
        byte[] remaining = new byte[4];
        assertEquals(4, stream.read(remaining));
        assertArrayEquals(new byte[] {2, 3, 4, 5}, remaining);
        stream.reset();
        assertEquals(4, stream.read(remaining));
        assertArrayEquals(new byte[] {2, 3, 4, 5}, remaining);
    }

    /**
     * Reproduces the reported MSRC triggers: a large declared total length followed by a one-byte first-chunk header
     * and no chunk body. The driver must fail with an ordinary SQLServerException when the data runs out instead of
     * allocating the declared length up front.
     */
    @ParameterizedTest
    @ValueSource(longs = {268_435_455L, Integer.MAX_VALUE})
    void testDeclaredLengthWithoutChunkBodyDoesNotAllocateDeclaredLength(long declaredLength) throws Exception {
        byte[] wire = concat(header(declaredLength), le32(1));
        SQLServerException e = assertThrows(SQLServerException.class, () -> readPlp(wire));
        assertEquals(INVALID_TDS, e.getMessage());

        e = assertThrows(SQLServerException.class, () -> PLPXMLInputStream
                .makeXMLStream(reader(wire), InputStreamGetterArgs.getDefaultArgs(), null).getBytes());
        assertEquals(INVALID_TDS, e.getMessage());
    }

    /**
     * Verifies that memory allocated while materializing a value follows the data received rather than the declared
     * total or chunk lengths. Uses HotSpot's per-thread allocation counter when available.
     */
    @ParameterizedTest
    @ValueSource(longs = {268_435_455L, Integer.MAX_VALUE, PLPInputStream.UNKNOWN_PLP_LEN})
    void testAllocationBoundedByReceivedData(long declaredLength) throws Exception {
        java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        assumeTrue(bean instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean threadBean = (com.sun.management.ThreadMXBean) bean;
        assumeTrue(threadBean.isThreadAllocatedMemorySupported() && threadBean.isThreadAllocatedMemoryEnabled());

        // Warm up class loading and mocks so they don't count against the budget.
        assertThrows(SQLServerException.class, () -> readPlp(concat(header(16), le32(1))));

        byte[] wire = concat(header(declaredLength), le32(Integer.MAX_VALUE));
        long threadId = Thread.currentThread().getId();
        long before = threadBean.getThreadAllocatedBytes(threadId);
        assertThrows(SQLServerException.class, () -> readPlp(wire));
        long allocated = threadBean.getThreadAllocatedBytes(threadId) - before;

        assertTrue(allocated < 4L * PLPInputStream.MAX_UNVERIFIED_ALLOCATION,
                "Allocated " + allocated + " bytes for a value whose body was never sent");
    }

    @ParameterizedTest
    @ValueSource(longs = {3 * 1024 * 1024 + 123, PLPInputStream.UNKNOWN_PLP_LEN})
    void testLargeValueAboveInitialAllocation(long declaredLength) throws Exception {
        byte[] data = new byte[3 * 1024 * 1024 + 123];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i * 31 + 7);
        }
        byte[][] chunks = new byte[(data.length + 7999) / 8000][];
        for (int i = 0; i < chunks.length; i++) {
            chunks[i] = Arrays.copyOfRange(data, i * 8000, Math.min(data.length, (i + 1) * 8000));
        }
        assertArrayEquals(data, readPlp(plp(declaredLength, chunks)));
    }

    @Test
    void testValueEndingBeforeDeclaredLengthIsRejected() throws Exception {
        SQLServerException e = assertThrows(SQLServerException.class,
                () -> readPlp(plp(6, new byte[] {1, 2, 3, 4, 5})));
        assertEquals(INVALID_TDS, e.getMessage());

        PLPInputStream stream = PLPInputStream.makeStream(reader(plp(6, new byte[] {1, 2, 3, 4, 5})),
                InputStreamGetterArgs.getDefaultArgs(), null);
        IOException readError = assertThrows(IOException.class, () -> stream.read(new byte[10]));
        assertEquals(INVALID_TDS, readError.getCause().getMessage());
    }

    private static byte[] le32(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] result = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    private TDSReader mockReader() throws SQLServerException {
        TDSReader reader = mock(TDSReader.class);
        doThrow(new SQLServerException(INVALID_TDS, null)).when(reader).throwInvalidTDS();
        // A zero mock packet size lets close release the stream without draining a fabricated large payload.
        when(reader.getConnection()).thenReturn(con);
        return reader;
    }

    private byte[] readPlp(byte[] wire) throws Exception {
        PLPInputStream stream = PLPInputStream.makeStream(reader(wire), InputStreamGetterArgs.getDefaultArgs(), null);
        assertNotNull(stream);
        return stream.getBytes();
    }

    private TDSReader reader(byte[] wire) {
        TDSReader reader = new TDSReader(null, con, command);
        TDSPacket previous = reader.mark().packet;
        for (int offset = 0; offset < wire.length; offset += 512) {
            byte[] payload = Arrays.copyOfRange(wire, offset, Math.min(wire.length, offset + 512));
            TDSPacket packet = new TDSPacket(payload.length);
            System.arraycopy(payload, 0, packet.payload, 0, payload.length);
            packet.payloadLength = payload.length;
            previous.next = packet;
            previous = packet;
        }
        return reader;
    }

    private byte[] header(long length) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(length).array();
    }

    private byte[] plp(long declaredLength, byte[]... chunks) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(header(declaredLength), 0, 8);
        for (byte[] chunk : chunks) {
            byte[] length = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(chunk.length).array();
            out.write(length, 0, 4);
            out.write(chunk, 0, chunk.length);
        }
        out.write(new byte[4], 0, 4);
        return out.toByteArray();
    }

    @Mock
    private TDSReader mockTdsReader;
    
    @Mock
    private SQLServerConnection mockConnection;
    
    @Mock
    private IdleConnectionResiliency mockSessionRecovery;
    
    @Mock
    private TDSCommand mockCommand;
    
    @BeforeEach
    void setUp() throws SQLServerException {
        MockitoAnnotations.openMocks(this);
        
        // Setup basic mocks
        when(mockTdsReader.getConnection()).thenReturn(mockConnection);
        when(mockConnection.getSessionRecovery()).thenReturn(mockSessionRecovery);
        when(mockTdsReader.getCommand()).thenReturn(mockCommand);
        
        // Mock the TDS data reading methods that StreamDone.setFromTDS() needs
        when(mockTdsReader.readUnsignedByte()).thenReturn(TDS.TDS_DONE); // token type
        when(mockTdsReader.readShort()).thenReturn((short) 0); // status and curCmd
        when(mockTdsReader.readLong()).thenReturn(0L); // rowCount
    }
    
    /**
     * Simplified test case specifically for Severity 20 fatal error simulation.
     * This test mocks the TDS status flags to simulate a severity 20 error
     * and verifies that SQLServerError is properly generated.
     */
    @Test
    void testSeverity20FatalErrorSimulation() throws SQLServerException {
        // Arrange - Simulate severity 20 fatal error with DONE_SRVERROR status
        short severity20ErrorStatus = (short) (TDS.DONE_SRVERROR);
        
        // Mock TDSReader to return the error status that would be set for severity 20 errors
        when(mockTdsReader.peekStatusFlag()).thenReturn(severity20ErrorStatus);
        
        // Override the readShort to return the error status for the StreamDone.setFromTDS() call
        // The first readShort call is for the status field in StreamDone
        when(mockTdsReader.readShort())
            .thenReturn(severity20ErrorStatus);

        // Create token handler to test
        TDSTokenHandler handler = new TDSTokenHandler("Severity20Test");
        
        // Act - Call onDone method which should detect the error status and create SQLServerError
        handler.onDone(mockTdsReader);
        
        // Verify that SQLServerError was generated due to the error status
        SQLServerError generatedError = handler.getDatabaseError();
        assertNotNull(generatedError, "SQLServerError must be generated for severity 20 error condition");

        // Verify the error message is the expected server error message
        String expectedErrorMessage = SQLServerException.getErrString("R_severeError");
        assertEquals(expectedErrorMessage, generatedError.getErrorMessage(), 
                "Generated error should have R_severeError message for fatal errors");
       
    }
}
