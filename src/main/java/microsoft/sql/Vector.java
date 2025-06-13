/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package microsoft.sql;

import java.util.Arrays;

import com.microsoft.sqlserver.jdbc.VectorUtils;

public final class Vector implements java.io.Serializable {

    public enum VectorDimensionType {
        // float16, 16-bit (half precision) float
        float32 // 32-bit (single precision) float
    }

    private VectorDimensionType vectorType;
    private int dimensionCount;

    private Object[] data;

    /**
     * Constructor for Vector with dimension count and vector type.
     * 
     * @param dimensionCount The number of dimensions in the vector.
     * @param vectorType     The type of the vector.
     * @param data           The object array representing the vector data.
     */
    public Vector(int dimensionCount, VectorDimensionType vectorType, Object[] data) {
        VectorUtils.validateVectorParameters(dimensionCount, vectorType, data);
        this.dimensionCount = dimensionCount; // checks non zero
        this.vectorType = vectorType;
        this.data = data; // float and null
    }

    /**
     * Constructor for Vector with precision and scale value.
     * 
     * @param precision The number of dimensions in the vector.
     * @param scale     The scale value of the vector (4 for float32).
     * @param data      The object array representing the vector data.
     */
    public Vector(int precision, int scale, Object[] data) {
        this(precision, VectorUtils.getVectorDimensionTypeFromScaleValue(scale), data);
    }

    // Getter methods for vector properties
    public Object[] getData() {
        return data;
    }

    public int getDimensionCount() {
        return dimensionCount;
    }

    public VectorDimensionType getVectorDimensionType() {
        return vectorType;
    }

    /**
     * Converts the vector to a string representation.
     * 
     * @return A string representation of the vector.
     */
    @Override
    public String toString() {
        return "VECTOR(" + vectorType + ", " + dimensionCount + ") : " +
                (data != null ? Arrays.toString(data) : "null");
    }

}