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
 * Test class for FLOAT32 vector bulk copy operations.
 * 
 * <p>This class extends {@link AbstractVectorBulkCopyTest} and provides FLOAT32-specific
 * configuration. All test methods are inherited from the abstract base class.</p>
 * 
 * <p>SQL Syntax: VECTOR(n) where n is the dimension count</p>
 * <p>Scale: 4 (FLOAT32)</p>
 * <p>Max Dimensions: 1998</p>
 */
@RunWith(JUnitPlatform.class)
@DisplayName("Test Vector Bulk Copy - FLOAT32")
@vectorJsonTest
@Tag(Constants.vectorTest)
public class VectorFloat32BulkCopyTest extends AbstractVectorBulkCopyTest {

    @Override
    protected VectorDimensionType getVectorDimensionType() {
        return VectorDimensionType.FLOAT32;
    }

    @Override
    protected String getColumnDefinition(int dimensionCount) {
        return "VECTOR(" + dimensionCount + ")";
    }

    @Override
    protected int getScale() {
        return 4; // FLOAT32 scale
    }

    @Override
    protected String getTypeName() {
        return "FLOAT32";
    }

    /**
     * The server implementation restricts vectors to a total of 8000 bytes.
     * Subtracting the 8 byte header leaves 7992 bytes for data.
     * For float32, each dimension requires 4 bytes, so the maximum number of dimensions is 1998.
     * Calculation: (1998 * 4) + 8 == 8000
     */
    @Override
    protected int getMaxDimensionCount() {
        return 1998;
    }

    @Override
    protected String getRequiredVectorTypeSupport() {
        return "v1"; // FLOAT32 works with v1
    }
}
