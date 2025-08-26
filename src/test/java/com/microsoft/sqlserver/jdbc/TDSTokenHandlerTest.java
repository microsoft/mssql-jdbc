package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit test for TDSTokenHandler onDone method, specifically testing
 * severity 25 (fatal) error handling and SQLServerError generation.
 */
public class TDSTokenHandlerTest {

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
     * Simplified test case specifically for Severity 25 fatal error simulation.
     * This test mocks the TDS status flags to simulate a severity 25 error
     * and verifies that SQLServerError is properly generated.
     */
    @Test
    void testSeverity25FatalErrorSimulation() throws SQLServerException {
        // Arrange - Simulate severity 25 fatal error with DONE_ERROR status
        short severity25ErrorStatus = (short) (TDS.DONE_ERROR); // 0x0002 - indicates error condition
        
        // Mock TDSReader to return the error status that would be set for severity 25 errors
        when(mockTdsReader.peekStatusFlag()).thenReturn(severity25ErrorStatus);
        
        // Override the readShort to return the error status for the StreamDone.setFromTDS() call
        // The first readShort call is for the status field in StreamDone
        when(mockTdsReader.readShort())
            .thenReturn(severity25ErrorStatus);

        // Create token handler to test
        TDSTokenHandler handler = new TDSTokenHandler("Severity25Test");
        
        // Act - Call onDone method which should detect the error status and create SQLServerError
        handler.onDone(mockTdsReader);
        
        // Verify that SQLServerError was generated due to the error status
        SQLServerError generatedError = handler.getDatabaseError();
        assertNotNull(generatedError, "SQLServerError must be generated for severity 25 error condition");
        
        // Verify the error message is the expected server error message
        String expectedErrorMessage = SQLServerException.getErrString("R_severeError");
        assertEquals(expectedErrorMessage, generatedError.getErrorMessage(), 
                "Generated error should have R_severeError message for fatal errors");
       
    }
}
