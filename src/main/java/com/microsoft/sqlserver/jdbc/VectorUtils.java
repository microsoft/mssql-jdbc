/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.MessageFormat;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import microsoft.sql.Vector;
import microsoft.sql.Vector.VectorDimensionType;

class VectorUtils {

    private static final int VECTOR_HEADER_LENGTH = 8;
    private static final int BYTES_PER_FLOAT = 4;
    private static final byte SCALE_BYTE_FLOAT32 = 0x00;
    // private static final int BYTES_PER_SHORT = 2;
    // private static final byte SCALE_BYTE_FLOAT16 = 0x01;

    /**
     * Converts a byte array to a Vector object. The byte array must contain the following:
     * 8 bytes for header and 4 bytes per float value.
     */
    static Vector fromBytes(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        // Read the vector type from the header (5th byte in the header)
        byte vectorTypeByte = bytes[4];
        VectorDimensionType vectorType = getVectorDimensionType(vectorTypeByte);
        int bytesPerDimension = getBytesPerDimensionFromScale(vectorType);

        if (bytes.length < getHeaderLength()) {
            throw vectorException("R_vectorByteArrayLength");
        }
        if ((bytes.length - getHeaderLength()) % bytesPerDimension != 0) {
            throw vectorException("R_vectorByteArrayMultipleOfBytesPerDimension", bytesPerDimension, vectorType);
        }

        int objectCount = (bytes.length - getHeaderLength()) / bytesPerDimension; // 8 bytes for header
        Object[] objectArray;
        switch (vectorType) {
            // case FLOAT16:
            // objectArray = new Short[objectCount];
            // break;
            case FLOAT32:
            default:
                objectArray = new Float[objectCount];
                break;
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        buffer.position(getHeaderLength()); // Skip the first 8 bytes (header)

        for (int i = 0; i < objectCount; i++) {
            objectArray[i] = buffer.getFloat();
        }

        return new Vector(objectCount, vectorType, objectArray);
    }

    /**
     * Converts the vector to a byte array. The byte array will contain the following:
     * 1. Layout Format (VECTOR marker) - 1 byte
     * 2. Layout Version (always 1) - 1 byte
     * 3. Number of Dimensions (2 bytes, little-endian) - 2 bytes
     * 4. Dimension Type (0x00 for FLOAT32) - 1 byte
     * 5. Reserved (3 bytes of padding) - 3 bytes
     * 6. Encode float values (Little-Endian) - 4 bytes per float value
     */
    static byte[] toBytes(Vector vector) {
        if (vector.getData() == null) {
            return null;
        }

        int payloadSize = getVectorLength(vector); // 8-byte header + float payload

        ByteBuffer buffer = ByteBuffer.allocate(payloadSize).order(ByteOrder.LITTLE_ENDIAN);

        buffer.put((byte) 0xA9);
        buffer.put((byte) 0x01);
        buffer.putShort((short) (vector.getDimensionCount()));
        buffer.put(getScaleByte(vector.getVectorDimensionType())); // 0x00 for FLOAT32, 0x01 for FLOAT16
        buffer.put(new byte[3]);

        Object[] data = vector.getData();
        switch (vector.getVectorDimensionType()) {
            // case FLOAT16:
            //     for (Object value : data) {
            //         buffer.putShort((short) ((Number) value).intValue());
            //     }
            //     break;
            case FLOAT32:
            default:
                for (Object value : data) {
                    buffer.putFloat(((Number) value).floatValue());
                }
                break;
        }

        return buffer.array();
    }

    static int getDefaultPrecision() {
        return BYTES_PER_FLOAT; // FLOAT32
    }

    static int getHeaderLength() {
        return VECTOR_HEADER_LENGTH; // 8 bytes for header
    }

    /**
     * Returns the vector dimension type based on the scale.
     * FLOAT32 for 0, FLOAT16 for 1
     */
    static VectorDimensionType getVectorDimensionType(int scaleByte) {
        switch (scaleByte) {
            case 0:
                return VectorDimensionType.FLOAT32;
            // case 1:
            // return VectorDimensionType.FLOAT16;
            default:
                return VectorDimensionType.FLOAT32;
        }
    }

    /**
     * Returns the precision based on the vector length and scale.
     * (vectorLength - 8) / scale
     */
    static int getPrecision(int vectorLength, int scale) {
        return (vectorLength - getHeaderLength()) / scale;
    }

    /**
     * Returns the bytesPerDimension based on the scale.
     * 4 bytes per dimension for FLOAT32(0), 2 bytes per dimension for FLOAT16(1)
     */
    static int getBytesPerDimensionFromScale(int scaleByte) {
        switch (scaleByte) {
            case 0:
                return BYTES_PER_FLOAT;
            // case 1:
            // return BYTES_PER_SHORT;
            default:
                return BYTES_PER_FLOAT;
        }
    }

    /**
     * Returns the bytesPerDimension based on the vectorType.
     * 4 for FLOAT32, 2 for FLOAT16
     */
    static int getBytesPerDimensionFromScale(VectorDimensionType vectorType) {
        switch (vectorType) {
            // case FLOAT16:
            // return BYTES_PER_SHORT;
            case FLOAT32:
            default:
                return BYTES_PER_FLOAT;
        }
    }

    /**
     * Returns the scale for the vector type.
     * 0x00 for FLOAT32, 0x01 for FLOAT16.
     */
    static byte getScaleByte(VectorDimensionType vectorType) {
        switch (vectorType) {
            // case FLOAT16:
            // return SCALE_BYTE_FLOAT16;
            case FLOAT32:
            default:
                return SCALE_BYTE_FLOAT32;
        }
    }

    /**
     * Returns the scale byte for the vector type.
     * 0x00 for FLOAT32(4 bytes), 0x01 for FLOAT16(2 bytes).
     */
    static byte getScaleByte(int scale) {
        switch (scale) {
            // case BYTES_PER_SHORT:
            // return SCALE_BYTE_FLOAT16;
            case BYTES_PER_FLOAT:
            default:
                return SCALE_BYTE_FLOAT32;
        }
    }

    /**
     * Returns the actual length of the vector in bytes.
     * 8 bytes for header + 4 bytes per float value.
     */
    static int getVectorLength(Vector vector) {
        int bytesPerDimension;
        switch (vector.getVectorDimensionType()) {
            // case FLOAT16:
            // bytesPerDimension = BYTES_PER_SHORT;
            // break;
            case FLOAT32:
            default:
                bytesPerDimension = BYTES_PER_FLOAT;
                break;
        }
        return getHeaderLength() + bytesPerDimension * vector.getDimensionCount(); // 8-byte header + dimension payload
    }

    /**
     * Returns the actual length of the vector in bytes.
     * 8 bytes for header + scale * precision.
     */
    static int getVectorLength(int scale, int precision) {
        return getHeaderLength() + scale * precision; // 8-byte header + dimension payload
    }

    /**
     * Returns the SQL type definition string for a vector parameter.
     * 
     * @param vector      The vector instance (may be null for output-only
     *                    parameters)
     * @param scale       Number of bytes per dimension (e.g., 4 for FLOAT32, 2 for
     *                    FLOAT16)
     * @param isOutput    True if the parameter is an output parameter
     * @param outScale    Output parameter's bytes per dimension (if applicable)
     * @param valueLength The value length for output parameters
     * @return SQL type definition string, e.g., VECTOR(128)
     */
    static String getTypeDefinition(Vector vector, int scale, boolean isOutput, int outScale, int valueLength) {
        int precision = 0;
        if (isOutput && scale < outScale) {
            precision = valueLength;
        } else if (vector != null) {
            precision = vector.getDimensionCount();
        }
        return "VECTOR(" + precision + ")";
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

}