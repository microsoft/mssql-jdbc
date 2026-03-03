/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package microsoft.sql;

import java.text.MessageFormat;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.Arrays;

/**
 * Represents a SQL Server {@code VECTOR} data type value.
 *
 * A vector stores a fixed-length array of floating-point values, with support for
 * {@link VectorDimensionType#FLOAT32 FLOAT32} (32-bit) and
 * {@link VectorDimensionType#FLOAT16 FLOAT16} (16-bit half-precision) element types.
 *
 * Example usage:
 * {@code
 * Float[] data = new Float[] { 0.1f, 0.2f, 0.3f };
 * Vector vector = new Vector(3, VectorDimensionType.FLOAT32, data);
 * pstmt.setObject(1, vector, microsoft.sql.Types.VECTOR);
 * }
 *
 * @since 13.3.2
 */
public final class Vector implements java.io.Serializable {

    public enum VectorDimensionType {
        FLOAT16, // 16-bit (half precision) float
        FLOAT32 // 32-bit (single precision) float
    }

    private VectorDimensionType vectorType;
    private int dimensionCount;

    private Object[] data;

    /**
     * Constructor for Vector with dimension count and vector type.
     * 
     * @param dimensionCount The number of dimensions in the vector.
     * @param vectorType     The type of the vector.
     * @param data           The vector data. Must be {@code Float[]} or {@code null}.
     */
    public Vector(int dimensionCount, VectorDimensionType vectorType, Object[] data) {
        validateVectorParameters(dimensionCount, vectorType, data);

        this.dimensionCount = dimensionCount;
        this.vectorType = vectorType;
        this.data = data;
    }

    /**
     * Constructor for Vector with precision and scale value.
     * 
     * @param precision The number of dimensions in the vector.
     * @param scale     The scale value of the vector (4 for FLOAT32, 2 for FLOAT16).
     * @param data      The vector data. Must be {@code Float[]} or {@code null}.
     */
    public Vector(int precision, int scale, Object[] data) {
        this(precision, getVectorDimensionTypeFromScaleValue(scale), data);
    }

    /**
     * Returns the data of the vector.
     * 
     * @return The vector data as {@code Float[]}, or {@code null}.
     */
    public Object[] getData() {
        return data;
    }

    /**
     * Returns the number of dimensions in the vector.
     * 
     * @return The dimension count of the vector.
     */
    public int getDimensionCount() {
        return dimensionCount;
    }

    /**
     * Returns the type of the vector.
     * 
     * @return The vector dimension type.
     */
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
        return "VECTOR(" + vectorType + ", " + dimensionCount + ") : " + Arrays.toString(data);
    }

    private static void validateVectorParameters(int dimensionCount, VectorDimensionType vectorType, Object[] data) {
        if (dimensionCount <= 0) {
            throw vectorException("R_InvalidVectorDimensionCount");
        }
        if (vectorType == null) {
            throw vectorException("R_VectorDimensionTypeCannotBeNull");
        }
        if (data != null) {
            if (data.length != dimensionCount) {
                throw vectorException("R_VectorDimensionCountMismatch");
            }
            if (!(data instanceof Float[])) {
                throw vectorException("R_VectorDataTypeMismatch");
            }
        }
    }

    private static IllegalArgumentException vectorException(String resourceKey, Object... args) {
        try {
            MessageFormat form = new MessageFormat(
                    ResourceBundle.getBundle("com.microsoft.sqlserver.jdbc.SQLServerResource").getString(resourceKey));
            return new IllegalArgumentException(form.format(args));
        } catch (MissingResourceException e) {
            return new IllegalArgumentException("Missing resource: " + resourceKey);
        }
    }

    /**
     * Returns the vector dimension type based on the scale value.
     * 4 for FLOAT32, 2 for FLOAT16
     */
    private static VectorDimensionType getVectorDimensionTypeFromScaleValue(int scale) {
        switch (scale) {
            case 4:
                return VectorDimensionType.FLOAT32;
            case 2:
                return VectorDimensionType.FLOAT16;
            default:
                return VectorDimensionType.FLOAT32;
        }
    }

}