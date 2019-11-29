package com.microsoft.sqlserver.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.security.cert.CertificateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.LogManager;

import org.junit.jupiter.api.Tag;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * A class for testing basic NTLMv2 functionality.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xSQLv14)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.xAzureSQLDB)
@Tag(Constants.reqExternalSetup)
public class EnclavePackageTest extends AbstractTest {

    private static SQLServerDataSource dsLocal = null;
    private static SQLServerDataSource dsXA = null;
    private static SQLServerDataSource dsPool = null;

    private static String connectionStringEnclave;

    private static byte[] healthReportCertificate = {61, 11, 0, 0, 27, 2, 0, 0, -66, 3, 0, 0, 88, 5, 0, 0, 82, 83, 65,
            49, 0, 16, 0, 0, 3, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, -57, 59, -80, 116, -86, -109, -4,
            -96, -106, -85, 107, 22, 88, 29, 91, -50, 29, -44, -36, -109, -123, 57, 70, -41, 110, -85, 90, 49, -17, 100,
            -49, 96, -34, 89, -76, -59, -82, 100, 118, -9, 44, 86, -112, 44, 13, 36, -43, 3, -69, -46, -6, -126, 26,
            -74, 75, -89, 82, -111, 72, -1, 127, 78, -80, 102, -126, 51, -89, 12, 70, 107, 50, 126, -40, -6, 51, 13, 87,
            5, 79, -53, -66, 26, 126, -128, -82, 97, 103, 7, 62, 49, -66, -97, -115, 113, -88, -73, 127, 102, -21, -51,
            79, 8, 101, -76, 15, -29, 0, -83, -42, -12, 52, 106, -60, 62, -67, 5, -7, -91, -21, -60, 4, -52, -39, 59,
            -100, 127, 5, -55, 93, 9, 89, 83, 14, 85, -9, -33, -5, 75, 42, -81, 123, -117, -73, -50, 15, 59, -35, -35,
            -74, -113, 41, -74, -41, 51, -3, 88, 110, 44, 3, -31, -36, 94, 29, 82, 12, -119, -34, 72, -51, 59, -109,
            -92, 44, 24, 75, 78, 84, 64, -86, -43, 11, 23, 70, 124, 33, -90, 60, -85, -63, -23, -70, 79, 62, 13, -121,
            -45, 53, 107, -49, 104, -85, -80, 7, -84, 61, -93, 44, -87, -68, -101, -7, -43, -65, 90, -12, -12, 16, 104,
            26, 5, -103, -32, -84, -80, -26, 17, -29, 30, -46, 127, 44, -70, -87, 61, -35, 47, -62, 86, -23, 114, 0,
            -19, 38, -25, 90, -6, -1, -121, -69, 51, 74, -95, 64, -67, 6, 49, 35, -125, -5, -40, -115, 1, -31, -51, 64,
            -102, -51, 106, 111, 38, 124, -111, 25, -75, 7, -116, -111, -120, 82, 103, 112, 14, 32, -49, -87, 23, 31,
            -14, -107, -64, -76, 9, -40, -37, 127, -99, 67, -114, 114, -3, -62, 7, -88, 103, -89, 124, 125, -14, -21,
            59, 91, 112, 13, 35, -80, -103, -79, 6, 41, -110, 8, -15, -58, 95, -101, 43, -8, 106, -50, -87, -32, 93,
            -30, -31, -105, 69, -73, 57, -38, 75, 42, 120, -75, -49, 77, 76, -120, -14, -42, -44, -34, 8, -4, -98, -35,
            46, -23, 16, -83, -6, 115, -127, 117, 54, -25, 58, -106, 10, 85, 54, -105, 48, 62, -55, 70, 56, 115, -121,
            31, -4, 113, 21, -98, 53, 118, 31, -82, 122, -29, 41, 59, -112, -123, -114, -26, 54, -45, 106, 77, 79, 2,
            -63, -37, 53, 42, -26, -24, -60, 88, -80, 22, 55, -37, -114, 99, -58, 113, 87, 26, -70, 105, -70, -78, -48,
            26, 51, 10, -37, -105, 66, 3, -69, 38, -56, 34, 77, -98, 56, 15, 0, 85, -63, -67, -102, -116, -49, -85, 22,
            -70, 68, -31, 99, 23, -100, -33, -45, -49, -90, 104, -26, -5, -32, 100, 81, -117, -60, -7, -59, 84, -8,
            -101, 86, 88, -100, 107, -65, -11, -5, 41, 96, -76, 24, 68, -105, -18, -99, -10, -15, 26, -43, 91, -86, 33,
            -17, -19, -6, -89, 78, -123, 48, 47, -107, 36, -2, -33, 123, -124, 91, 123, -20, -88, 73, 48, -126, 3, -70,
            48, -126, 2, -94, -96, 3, 2, 1, 2, 2, 16, 66, 85, -66, 31, 39, 20, 120, -95, 75, 114, 74, -14, 59, 52, 99,
            -29, 48, 13, 6, 9, 42, -122, 72, -122, -9, 13, 1, 1, 11, 5, 0, 48, 47, 49, 45, 48, 43, 6, 3, 85, 4, 3, 12,
            36, 77, 105, 99, 114, 111, 115, 111, 102, 116, 32, 82, 101, 109, 111, 116, 101, 32, 65, 116, 116, 101, 115,
            116, 97, 116, 105, 111, 110, 32, 83, 101, 114, 118, 105, 99, 101, 48, 30, 23, 13, 49, 57, 49, 48, 51, 48,
            49, 56, 50, 50, 52, 55, 90, 23, 13, 49, 57, 49, 48, 51, 49, 48, 50, 50, 50, 52, 56, 90, 48, 75, 49, 73, 48,
            71, 6, 3, 85, 4, 3, 12, 64, 65, 49, 57, 67, 49, 56, 70, 51, 50, 53, 49, 56, 66, 56, 65, 57, 51, 70, 49, 57,
            52, 57, 69, 66, 57, 69, 56, 53, 48, 68, 56, 53, 66, 66, 53, 53, 65, 67, 69, 65, 49, 56, 67, 53, 54, 55, 49,
            70, 55, 55, 65, 69, 49, 54, 51, 54, 56, 52, 51, 55, 68, 65, 53, 56, 48, -126, 1, 34, 48, 13, 6, 9, 42, -122,
            72, -122, -9, 13, 1, 1, 1, 5, 0, 3, -126, 1, 15, 0, 48, -126, 1, 10, 2, -126, 1, 1, 0, -50, -20, -124, 102,
            67, -52, 79, -43, 5, -104, 31, 99, -89, 53, -77, 61, 82, 64, -125, -30, 32, 102, 101, -87, 63, -75, -10,
            -100, -31, -125, 15, -9, -40, 81, 8, 54, -43, 112, 91, -19, 126, 49, -57, 33, -51, -33, -82, 21, 22, 4, -68,
            112, 83, -55, -50, 60, -70, 1, -37, -60, 3, -75, 63, 124, -3, -112, 107, -120, -122, -97, 26, -28, 62, -101,
            -21, -66, 28, -84, 23, 21, -126, 27, -9, 45, 43, 50, 1, -117, 28, -114, -99, -38, 38, -104, 7, -45, 85,
            -100, -44, -102, 68, -6, -22, -45, -75, 84, -33, -11, -76, -9, 42, -1, 82, -21, -12, 18, -15, 118, -121,
            124, 103, -85, 43, 54, -106, 77, 102, 0, -107, -12, 103, 10, 118, -85, 23, 40, -61, -39, 76, 39, -124, 75,
            -121, -114, -65, -119, -42, -75, 42, -110, -71, 94, 61, -125, 104, 105, -119, 74, -83, 57, -11, -38, -120,
            -94, -100, -58, -85, 106, 124, 50, 30, 118, -102, 1, -123, -119, -81, 111, 6, 18, -114, 61, -35, 64, 99,
            -53, 16, -56, -8, -18, -110, 96, -85, 125, 95, 60, 0, -58, -81, -30, 54, 39, 73, 100, -27, 14, 5, -45, 122,
            120, -33, -48, -62, -43, 98, 9, -5, -12, 4, -6, -57, -113, -28, 117, -26, 50, 76, 65, -107, 109, 49, -9,
            -22, 86, 103, 68, -128, -87, -116, 25, -117, 114, 28, -9, 28, 70, -28, -36, 102, -27, 40, 120, 60, -113,
            -11, -73, 2, 3, 1, 0, 1, -93, -127, -75, 48, -127, -78, 48, 14, 6, 3, 85, 29, 15, 1, 1, -1, 4, 4, 3, 2, 7,
            -128, 48, 29, 6, 9, 43, 6, 1, 4, 1, -126, 55, 21, 10, 4, 16, 48, 14, 48, 12, 6, 10, 43, 6, 1, 4, 1, -126,
            55, 48, 1, 2, 48, 25, 6, 3, 85, 29, 14, 4, 18, 4, 16, 49, 110, 105, 97, 22, -114, 85, -99, 21, -32, -60,
            -109, 42, -59, 66, 11, 48, 102, 6, 3, 85, 29, 35, 4, 95, 48, 93, -128, 20, 37, -118, -59, 87, -8, 105, 88,
            79, -36, 20, -127, -8, -56, -9, 97, -10, 105, -80, -109, -93, -95, 51, -92, 49, 48, 47, 49, 45, 48, 43, 6,
            3, 85, 4, 3, 12, 36, 77, 105, 99, 114, 111, 115, 111, 102, 116, 32, 82, 101, 109, 111, 116, 101, 32, 65,
            116, 116, 101, 115, 116, 97, 116, 105, 111, 110, 32, 83, 101, 114, 118, 105, 99, 101, -126, 16, 37, 113,
            -89, -86, -71, 42, -44, -83, 67, -30, 8, -31, 97, 14, -40, -12, 48, 13, 6, 9, 42, -122, 72, -122, -9, 13, 1,
            1, 11, 5, 0, 3, -126, 1, 1, 0, -85, -56, -125, -91, 83, -96, -32, -69, -69, -40, -127, -93, 69, 24, -7,
            -121, 54, 30, 76, 20, 28, -95, 93, -76, -109, -4, -97, -102, 126, 76, -68, -39, 6, -66, 70, -100, 67, -112,
            25, -109, -2, -83, 40, 86, 69, -70, -105, 40, 88, 109, -101, 64, 95, -60, -49, -25, -42, 61, -37, 18, -3,
            -64, -68, -123, 111, 7, 119, 107, 81, 40, -103, 66, 42, 110, 81, 95, -38, 104, 109, -92, -90, 6, -120, -113,
            53, -8, -116, -44, -90, -3, 103, 100, 78, 90, 96, -121, 80, -105, 109, 28, -28, -63, 75, 14, -98, 83, -122,
            78, -70, 83, 4, 11, -107, 115, 50, 78, -37, -88, 62, -72, 4, 26, 6, 102, -65, -39, -48, 58, -72, 36, 125,
            29, -27, -6, -42, 111, 57, -97, 102, 3, -5, 112, 62, -13, -65, -4, 40, -103, 19, -75, 82, -83, -26, -12, 62,
            -41, -38, -70, -85, 22, 100, 34, 0, 90, 68, -33, -107, 40, -63, -9, 38, 99, -89, 15, 118, 14, -33, -2, -18,
            -98, -105, 23, 31, 92, -46, -68, -61, 103, 47, -121, 73, -8, -82, -28, 43, 0, 50, 78, 78, 68, 88, -91, 121,
            -29, 30, -25, 23, 88, 43, -72, -49, -77, 92, -62, -8, 85, -86, -111, 99, -37, -46, -121, 34, 122, 118, 9,
            -27, 57, -56, 59, 44, 62, -115, -41, -49, -73, -59, 122, 39, -69, 76, 122, 127, -21, 113, 43, -21, -109, 95,
            98, 38, 33, 111, -34, -38, 71, 88, 5, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 64, 4, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 64,
            4, 0, 0, 1, 0, 0, 0, 14, 60, -81, -73, 81, 79, 109, 37, -78, -54, -47, -115, 69, -20, -28, -70, -75, 43, 5,
            -75, -42, 70, -27, 53, -124, -35, 61, 45, -90, -38, 41, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 32, 48, 64, 65, 49, 33, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -108, -76, 101, 116, -38, -69, -79, 113, -122, -19, -59, 38,
            60, -76, -34, 123, -10, -39, -30, -21, -122, -122, 34, 32, 90, -108, -45, -109, -44, -52, 101, -127, 4, 55,
            -54, -30, 83, 125, -117, -101, 7, 118, -74, 27, 17, -26, -50, -45, -46, 50, -23, 48, -113, 96, -30, 26, -38,
            -78, -3, -111, -29, -38, -107, -104, -2, -2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 23, 18, 0, 1, 5,
            32, 19, 0, 5, 20, 3, 18, 1, 34, 5, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 0, 0, 0, -112, 0, 0, 0, -108, -76, 101, 116, -38, -69, -79, 113, -122, -19, -59, 38, 60, -76, -34, 123,
            -10, -39, -30, -21, -122, -122, 34, 32, 90, -108, -45, -109, -44, -52, 101, -127, 4, 55, -54, -30, 83, 125,
            -117, -101, 7, 118, -74, 27, 17, -26, -50, -45, -46, 50, -23, 48, -113, 96, -30, 26, -38, -78, -3, -111,
            -29, -38, -107, -104, -2, -2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 23, 18, 0, 1, 5, 32, 19, 0, 5,
            20, 3, 18, 1, 34, 5, 0, 0, 0, 0, 97, 0, 101, 0, 116, 0, 109, 0, 45, 0, 101, 0, 110, 0, 99, 0, 108, 0, 97, 0,
            118, 0, 101, 0, 46, 0, 100, 0, 108, 0, 108, 0, 0, 0, 0, 0, 1, 0, 0, 0, -120, 0, 0, 0, 90, -58, -49, 7, 59,
            -65, 81, -44, -97, -107, 97, -106, 71, -70, 33, -5, 99, -7, -4, -117, -31, -91, -35, -56, -12, -105, -29,
            80, 5, 72, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 39, -67, 104, 117, 89, 73, -73, -66, 6, 52, 80,
            -30, 22, -41, -19, 0, 0, 0, 0, 98, 0, 99, 0, 114, 0, 121, 0, 112, 0, 116, 0, 46, 0, 100, 0, 108, 0, 108, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, -104, 0, 0, 0, -76, 15, 80, 111, -51, 57, 8, -54, -53, -109, -113, -106,
            35, -72, -114, -12, -128, -17, 51, -64, -124, -38, -55, 27, 1, 80, 69, 48, -96, -77, -128, 58, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, -16, 60, -51, -89, -24, 123, 70, -21, -86, -25, 31, 19, -43, -51, -34, 93, 0, 0, 0,
            0, 117, 0, 99, 0, 114, 0, 116, 0, 98, 0, 97, 0, 115, 0, 101, 0, 95, 0, 101, 0, 110, 0, 99, 0, 108, 0, 97, 0,
            118, 0, 101, 0, 46, 0, 100, 0, 108, 0, 108, 0, 0, 0, 0, 0, 1, 0, 0, 0, -120, 0, 0, 0, -41, 110, -101, -107,
            45, -49, 11, 25, -47, 17, 90, 38, -28, 105, -88, 110, 39, -35, -15, -94, -106, 14, -37, -70, -105, -14, 59,
            -88, 80, 30, -112, -91, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 114, -124, 65, 114, 103, -88, 78, -115, -65, 1,
            40, 75, 7, 67, 43, 30, 0, 0, 0, 0, 118, 0, 101, 0, 114, 0, 116, 0, 100, 0, 108, 0, 108, 0, 46, 0, 100, 0,
            108, 0, 108, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, -112, 0, 0, 0, 20, -54, 74, 74, 43, -64, -12, 60, -101, -67,
            -58, -102, 71, -124, 113, -46, 92, -63, 11, -104, 100, -6, -126, 9, 3, 16, -36, 57, 24, -22, 8, 122, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 65, 0, 122, 0,
            117, 0, 114, 0, 101, 0, 65, 0, 116, 0, 116, 0, 101, 0, 115, 0, 116, 0, 46, 0, 100, 0, 108, 0, 108, 0, 0, 0,
            0, 0, 0, 0, 1, 0, 0, 0, -104, 0, 0, 0, -54, 68, -119, -29, 65, -104, -2, 53, -48, -27, -20, 25, 8, -72, -43,
            124, -63, 9, 126, -58, -100, 14, -41, -75, 61, -66, -28, 80, 67, 17, -109, 38, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 77, 19, -65, 62, 83, 100, 65, 48, -86, -102, -64, -14, -31, -12, -58, 36, 0, 0, 0, 0, 98, 0, 99, 0,
            114, 0, 121, 0, 112, 0, 116, 0, 80, 0, 114, 0, 105, 0, 109, 0, 105, 0, 116, 0, 105, 0, 118, 0, 101, 0, 115,
            0, 46, 0, 100, 0, 108, 0, 108, 0, 0, 0, 0, 0, -107, 11, 37, -57, 57, -18, -111, 70, 53, -83, -117, 111, -63,
            65, -80, -4, -3, -36, 27, 35, 111, -91, 54, -34, 103, 41, -5, -79, 8, -36, -124, 101, 103, 90, -115, -21,
            40, 49, 33, 61, 85, -104, 67, 5, -26, -78, -27, -78, -92, -70, 115, -77, -98, 39, -101, 58, 35, -21, -51,
            83, -4, 80, 125, 39, -117, -75, 50, -71, 125, -44, 28, 41, 25, 67, -4, 3, -120, -89, -124, -19, 94, -81, 31,
            114, -91, 117, -9, 6, 85, 32, 16, -16, -28, 59, 108, 67, -77, -126, 42, 25, 7, -46, 40, -32, 52, 51, -60,
            120, 28, 1, 89, -1, 51, -96, 105, 93, -111, -8, -25, 9, 55, -120, 37, -63, -6, -66, 11, -16, -34, -71, -18,
            67, -74, 86, -98, -15, -56, -76, 86, 10, -6, 13, -9, 24, -9, -94, -93, 127, -63, -69, 86, -73, -57, 91,
            -108, -11, 70, 77, -80, -39, 70, -14, -61, 39, -45, -83, 46, 17, 126, -104, 10, -117, 46, -60, -122, 12, 20,
            -117, 30, 105, -80, -67, -48, -4, -57, -98, 95, 102, -85, 36, 29, -61, -93, -91, 67, -61, 76, -54, -60, 77,
            95, -79, -126, -66, 110, -32, -8, -45, -108, 42, -101, 102, -99, 104, -33, 74, -43, -124, -47, 81, -58, 118,
            112, -80, -9, 2, -40, 77, 40, 122, 16, 48, 111, -98, 78, 80, -122, 99, -29, -125, -78, -104, -36, -94, -95,
            -115, -110, 31, -108, -46, 88, 79, -103, -65, 90, -13, 120, 2, 0, 0, 19, 0, 0, 0, 75, 1, 0, 0, 104, 0, 0, 0,
            0, 2, 0, 0, 69, 67, 75, 51, 48, 0, 0, 0, 38, -81, -37, -121, 34, 74, 31, -103, -95, 62, 72, -29, 19, -105,
            -124, -112, 71, 6, 64, 98, -73, -85, -49, -21, -63, -67, -69, 2, 56, 36, 116, -49, -24, -123, 24, 43, 69,
            -62, -126, -94, 80, 113, 114, 60, -44, -48, -44, -83, -13, -104, -69, 57, -26, 41, 65, -15, 97, -65, 26,
            -41, 32, -125, -50, 52, -18, 15, -91, 98, -4, -52, 88, -34, -61, -32, 126, -40, 63, 2, 113, -11, 92, 69, 3,
            -118, -52, 50, -30, 52, -105, -36, -95, -18, 17, 99, 107, -105, 17, -104, -125, 34, 11, -44, 7, 79, 35, 24,
            -2, -27, -43, -21, 115, 52, 44, -45, -68, -122, 100, -55, -79, 14, -79, 81, 49, 81, -3, -43, -89, -118, 116,
            76, 13, -117, 16, -1, 46, -62, 40, 21, 97, 63, 43, -65, 45, 101, 30, 2, 66, 15, -20, -53, -112, 23, 120,
            -12, -31, -79, -33, 89, -83, 95, -110, -67, -58, -86, 118, 28, 84, 22, 79, -69, -41, -78, 88, 31, 73, 97,
            71, -109, -127, -22, 14, 113, -65, 51, 11, 34, -23, -61, 107, 114, -109, 58, 9, 104, 120, -36, -93, 70, 104,
            -79, -46, 13, -15, -62, -61, -62, -16, 19, -79, 4, 121, -63, 61, -100, -66, 25, -23, 117, -119, -97, -84,
            15, 99, -101, 59, -18, -81, -67, -99, -20, -82, -41, 74, -19, -54, -86, -58, 121, -7, -8, -8, 11, 113, -52,
            -50, 14, -102, -77, -105, 126, -110, 85, -97, 75, -24, -33, -63, 48, 79, 67, 84, 73, 54, 49, -104, 38, 13,
            -100, 76, 66, 86, 72, -120, 100, 33, -5, -26, 107, 79, -48, -1, 71, 104, 22, 115, 66, -94, 73, 88, -48, 71,
            44, 118, 15, -76, -98, -74, -78, 43, 33, 120, -74, -87, 78, 31, 36, 96, -104, -98, -28, 121, -128, -8, -77,
            -16, -74, -94, -101, -57, 99, -78, 70, 108, -26, 115, 21, -13, -41, 46, -93, -68, 29, 76, 68, 109, -63, 108,
            27, -112, -20, -53, 92, 109, 121, 33, 64, 69, 42, -72, -45, 35, 50, 60, -124, -99, -40, -7, -96, -57, -14,
            -71, -103, -44, 13, 69, -89, -17, -46, -128, -5, -83, 80, -125, 13, -48, -126, 49, 62, 37, 113, -92, -30,
            15, 47, 113, 124, 108, -89, -48, -4, -84, -61, -11, -55, -97, -28, 36, 20, -106, 57, -16, 107, 51, 67, -95,
            -73, -76, -8, -118, 97, 127, -20, 67, 103, 25, 3, -67, 41, 73, 122, 92, -9, 22, 69, -91, -107, 52, -23,
            -119, -56, 71, -76, 77, -51, 12, -72, 117, -48, 108, -100, -77, -103, 114, 45, 100, 100, -11, 55, 124, -121,
            -102, -105, 22, 4, -33, 76, 68, 116, -96, -80, 43, 5, -119, -113, 44, 66, -128, -104, 60, -78, -123, 56,
            -36, -99, -63, 71, 25, -15, 17, -8, -71, -8, -37, 92, 70, 127, 33, 69, -34, 11, -9, 38, 127, -46, -10, 112,
            126, -7, -77, 16, -32, -42, 118, -110, 98, -112, 78, -95, 56, -45, -46, -108, -54, 116, 99, 107, 78, -92,
            -60, 72, -45, -40, 33, -93, -21, -90, -108, -19, 89, -42, 29, -123, 35, -114, -95, -37, 2, -61, 12, 84,
            -100, -100, 112, -96, 80, 12, 90, 78, 36, 40, -56, -90, -60, -77, -77, -56, 53, -93, -10, -22, -47, 15, 5,
            -90, -67, 111, -82, -94, 96, -68, -79, -3, -51, -108, 41, 112, -89, 22, -92, 88, 37, 66, 20, 93, 111, 102,
            -69, -20, -47, -43, -24, 82, -41, 12, -58, 53, 68, -76, -94, -116, 75, 14, 24, -43, 73, -78, -87, 21};

    /**
     * Setup environment for test.
     * 
     * @throws Exception
     *         when an error occurs
     */
    public static void setupEnclave() throws Exception {
        connectionStringEnclave = TestUtils.addOrOverrideProperty(connectionString, "columnEncryptionSetting",
                ColumnEncryptionSetting.Enabled.toString());

        String enclaveAttestationUrl = System.getProperty("enclaveAttestationUrl");
        connectionStringEnclave = TestUtils.addOrOverrideProperty(connectionStringEnclave, "enclaveAttestationUrl",
                (null != enclaveAttestationUrl) ? enclaveAttestationUrl : "http://blah");

        String enclaveAttestationProtocol = System.getProperty("enclaveAttestationProtocol");
        connectionStringEnclave = TestUtils.addOrOverrideProperty(connectionStringEnclave, "enclaveAttestationProtocol",
                (null != enclaveAttestationProtocol) ? enclaveAttestationProtocol : AttestationProtocol.HGS.toString());

        // reset logging to avoid severe logs due to negative testing
        LogManager.getLogManager().reset();

        dsLocal = new SQLServerDataSource();
        AbstractTest.updateDataSource(connectionStringEnclave, dsLocal);

        dsXA = new SQLServerXADataSource();
        AbstractTest.updateDataSource(connectionStringEnclave, dsXA);

        dsPool = new SQLServerConnectionPoolDataSource();
        AbstractTest.updateDataSource(connectionStringEnclave, dsPool);
    }

    /**
     * Tests basic connection.
     * 
     * @throws SQLException
     *         when an error occurs
     */
    public static void testBasicConnection() throws SQLException {
        try (Connection con1 = dsLocal.getConnection(); Connection con2 = dsXA.getConnection();
                Connection con3 = dsPool.getConnection();
                Connection con4 = PrepUtil.getConnection(connectionStringEnclave)) {
            if (TestUtils.isAEv2(con1)) {
                verifyEnclaveEnabled(con1);
            }
            if (TestUtils.isAEv2(con2)) {
                verifyEnclaveEnabled(con2);
            }
            if (TestUtils.isAEv2(con3)) {
                verifyEnclaveEnabled(con3);
            }
            if (TestUtils.isAEv2(con4)) {
                verifyEnclaveEnabled(con4);
            }
        }
    }

    /**
     * Tests invalid connection property combinations.
     */
    public static void testInvalidProperties() {
        // enclaveAttestationUrl and enclaveAttestationProtocol without "columnEncryptionSetting"
        testInvalidProperties(TestUtils.addOrOverrideProperty(connectionStringEnclave, "columnEncryptionSetting",
                ColumnEncryptionSetting.Disabled.toString()), "R_enclavePropertiesError");

        // enclaveAttestationUrl without enclaveAttestationProtocol
        testInvalidProperties(TestUtils.removeProperty(connectionStringEnclave, "enclaveAttestationProtocol"),
                "R_enclavePropertiesError");

        // enclaveAttestationProtocol without enclaveAttestationUrl
        testInvalidProperties(TestUtils.addOrOverrideProperty(connectionStringEnclave, "enclaveAttestationUrl", ""),
                "R_enclavePropertiesError");

        // bad enclaveAttestationProtocol
        testInvalidProperties(
                TestUtils.addOrOverrideProperty(connectionStringEnclave, "enclaveAttestationProtocol", ""),
                "R_enclaveInvalidAttestationProtocol");
    }

    /*
     * Test bad Java Key Store
     */
    @SuppressWarnings("unused")
    public static void testBadJks() {
        try {
            SQLServerColumnEncryptionJavaKeyStoreProvider jksp = new SQLServerColumnEncryptionJavaKeyStoreProvider(null,
                    null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidConnectionSetting")));
        }
    }

    /*
     * Test bad Azure Key Vault
     */
    @SuppressWarnings("unused")
    public static void testBadAkv() {
        try {
            SQLServerColumnEncryptionAzureKeyVaultProvider akv = new SQLServerColumnEncryptionAzureKeyVaultProvider(
                    null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_NullValue")));
        }
    }

    /*
     * Test calling verifyColumnMasterKeyMetadata for non enclave computation
     */
    public static void testVerifyCMKNoEnclave() {
        try {
            SQLServerColumnEncryptionJavaKeyStoreProvider jksp = new SQLServerColumnEncryptionJavaKeyStoreProvider(
                    "keystore", null);
            assertFalse(jksp.verifyColumnMasterKeyMetadata(null, false, null));
        } catch (SQLServerException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }

        try {
            SQLServerColumnEncryptionAzureKeyVaultProvider aksp = new SQLServerColumnEncryptionAzureKeyVaultProvider("",
                    "");
            assertFalse(aksp.verifyColumnMasterKeyMetadata(null, false, null));
        } catch (SQLServerException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /*
     * Test calling verifyColumnMasterKeyMetadata with untrusted key path
     */
    public static void testVerifyCMKUntrusted() {
        Map<String, List<String>> trustedKeyPaths = null;
        try {
            trustedKeyPaths = SQLServerConnection.getColumnEncryptionTrustedMasterKeyPaths();
            List<String> paths = new ArrayList<String>();
            paths.add(javaKeyPath);
            String serverName = connection.activeConnectionProperties
                    .getProperty(SQLServerDriverStringProperty.SERVER_NAME.toString()).toUpperCase();

            trustedKeyPaths.put(serverName, paths);
            SQLServerConnection.setColumnEncryptionTrustedMasterKeyPaths(trustedKeyPaths);

            SQLServerSecurityUtility.verifyColumnMasterKeyMetadata(connection, "My_KEYSTORE", "UnTrustedKeyPath",
                    serverName, true, null);
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_UntrustedKeyPath")));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedException"));
        } finally {
            if (null != trustedKeyPaths) {
                trustedKeyPaths.clear();
                SQLServerConnection.setColumnEncryptionTrustedMasterKeyPaths(trustedKeyPaths);
            }
        }
    }

    /*
     * Test getEnclavePackage with null enclaveSession
     */
    public static void testGetEnclavePackage() {
        SQLServerVSMEnclaveProvider provider = new SQLServerVSMEnclaveProvider();
        try {
            byte[] b = provider.getEnclavePackage(null, null);
            assertTrue(b == null);
        } catch (SQLServerException e) {
            fail(TestResource.getResource("R_unexpectedException"));
        }
    }

    /*
     * Test invalidEnclaveSession
     */
    public static void testInvalidEnclaveSession() {
        SQLServerVSMEnclaveProvider provider = new SQLServerVSMEnclaveProvider();
        provider.invalidateEnclaveSession();
        if (null != provider.getEnclaveSession()) {
            fail(TestResource.getResource("R_invalidEnclaveSessionFailed"));
        }
    }

    /*
     * Test VSM createSessionSecret with bad server response
     */
    public static void testNullSessionSecret() throws SQLServerException {
        VSMAttestationParameters param = new VSMAttestationParameters();

        try {
            param.createSessionSecret(null);
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_MalformedECDHPublicKey")));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /*
     * Test bad session secret
     */
    public static void testBadSessionSecret() throws SQLServerException {
        VSMAttestationParameters param = new VSMAttestationParameters();

        try {
            byte[] serverResponse = new byte[104]; // ENCLAVE_LENGTH is private
            param.createSessionSecret(serverResponse);
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_MalformedECDHHeader")));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /*
     * Test null Attestation response
     */
    @SuppressWarnings("unused")
    public static void testNullAttestationResponse() throws SQLServerException {
        try {
            AttestationResponse resp = new AttestationResponse(null);
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_EnclaveResponseLengthError")));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /*
     * Test bad Attestation response
     */
    @SuppressWarnings("unused")
    public static void testBadAttestationResponse() throws SQLServerException {
        try {
            byte[] responseBytes = new byte[36];
            AttestationResponse resp = new AttestationResponse(responseBytes);
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_HealthCertError")));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /*
     * Test bad certificate signature
     */
    public static void testBadCertSignature() throws SQLServerException, CertificateException {
        try {
            AttestationResponse resp = new AttestationResponse(healthReportCertificate);
            resp.validateCert(null);
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidHealthCert")));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /*
     * Negative Test - AEv2 not supported
     */
    public static void testAEv2NotSupported() {
        try (SQLServerConnection con = PrepUtil.getConnection(connectionStringEnclave)) {
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_enclaveNotSupported")));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    /*
     * Test invalid properties
     */
    private static void testInvalidProperties(String connStr, String resourceKeyword) {
        try (Connection con = PrepUtil.getConnection(connStr)) {
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (SQLException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg(resourceKeyword)), e.getMessage());
        }
    }

    /*
     * Verify if Enclave is enabled
     */
    private static void verifyEnclaveEnabled(Connection con) throws SQLException {
        try (Statement stmt = con.createStatement(); ResultSet rs = stmt.executeQuery(
                "SELECT [name], [value], [value_in_use] FROM sys.configurations WHERE [name] = 'column encryption enclave type';")) {
            while (rs.next()) {
                assertEquals("1", rs.getString(2));
            }
        }
    }
}
