/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.datatypes.vector.bulkcopy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.vectorJsonTest;

import microsoft.sql.Vector.VectorDimensionType;

/**
 * Test class for FLOAT16 vector bulk copy operations.
 * 
 * <p>This class extends {@link AbstractVectorBulkCopyTest} and provides FLOAT16-specific
 * configuration. All test methods are inherited from the abstract base class.</p>
 * 
 * <p>SQL Syntax: VECTOR(n, float16) where n is the dimension count</p>
 * <p>Scale: 2 (FLOAT16)</p>
 * <p>Max Dimensions: 3996</p>
 * 
 * <p><b>Note:</b> This test class is tagged with {@link Constants#vectorFloat16Test}
 * for pipeline exclusion since the server does not yet support FLOAT16 vectors.</p>
 */
@RunWith(JUnitPlatform.class)
@DisplayName("Test Vector Bulk Copy - FLOAT16")
@vectorJsonTest
@Tag(Constants.vectorFloat16Test)
public class VectorFloat16BulkCopyTest extends AbstractVectorBulkCopyTest {

    @Override
    protected VectorDimensionType getVectorDimensionType() {
        return VectorDimensionType.FLOAT16;
    }

    @Override
    protected String getColumnDefinition(int dimensionCount) {
        // FLOAT16 requires explicit type specification
        return "VECTOR(" + dimensionCount + ", float16)";
    }

    @Override
    protected int getScale() {
        return 2; // FLOAT16 scale
    }

    @Override
    protected String getTypeName() {
        return "FLOAT16";
    }

    /**
     * The server implementation restricts vectors to a total of 8000 bytes.
     * Subtracting the 8 byte header leaves 7992 bytes for data.
     * For float16, each dimension requires 2 bytes, so the maximum number of dimensions is 3996.
     * Calculation: (3996 * 2) + 8 == 8000
     */
    @Override
    protected int getMaxDimensionCount() {
        return 3996;
    }

    @Override
    protected String getRequiredVectorTypeSupport() {
        return "v2"; // FLOAT16 requires v2
    }
}
