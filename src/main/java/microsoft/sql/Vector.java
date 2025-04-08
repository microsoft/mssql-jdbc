/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package microsoft.sql;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class Vector implements java.io.Serializable {
    public enum VectorDimensionType {
        F16, // 16-bit (half precision) float
        F32 // 32-bit (single precision) float
    }

    public VectorDimensionType vectorType;
    public int dimensionCount;

    public float[] data;

    public Vector(int dimensionCount, VectorDimensionType vectorType, float[] data) {
        this.dimensionCount = dimensionCount;
        this.vectorType = vectorType;
        this.data = data;
    }

    /**
     * Converts a byte array to a Vector object. The byte array must contain the following:
     * 8 bytes for header and 4 bytes per float value.
     */
    public static microsoft.sql.Vector fromBytes(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        if (bytes.length < 8) {
            throw new IllegalArgumentException("Byte array length must be at least 8 bytes.");
        }
        if (bytes.length % 4 != 0) {
            throw new IllegalArgumentException("Byte array length must be a multiple of 4.");
        }

        int floatCount = (bytes.length - 8) / 4; // 8 bytes for header
        float[] floatArray = new float[floatCount];

        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        buffer.position(8); // Skip the first 8 bytes (header)

        for (int i = 0; i < floatCount; i++) {
            floatArray[i] = buffer.getFloat();
        }

        return new Vector(floatCount, VectorDimensionType.F32, floatArray);
    }

    /**
     * Converts the vector to a byte array. The byte array will contain the following:
     * 1. Layout Format (VECTOR marker) - 1 byte
     * 2. Layout Version (always 1) - 1 byte
     * 3. Number of Dimensions (2 bytes, little-endian) - 2 bytes
     * 4. Dimension Type (0x01 for F32) - 1 byte
     * 5. Reserved (3 bytes of padding) - 3 bytes
     * 6. Encode float values (Little-Endian) - 4 bytes per float value
     */
    public byte[] toBytes() {
        if (data == null) {
            return null;
        }
        
        int payloadSize = getActualLength(); // 8-byte header + float payload

        ByteBuffer buffer = ByteBuffer.allocate(payloadSize).order(ByteOrder.LITTLE_ENDIAN);

        // 1. Layout Format (VECTOR marker)
        buffer.put((byte) 0xA9);

        // 2. Layout Version (always 1)
        buffer.put((byte) 0x01);

        // 3. Number of Dimensions (2 bytes, little-endian)
        buffer.putShort((short) (dimensionCount));

        // 4. Dimension Type (0x01 for float) 
        buffer.put(getScale());

        // 5. Reserved (3 bytes of padding)
        buffer.put(new byte[3]);

        // 6. Encode float values (Little-Endian)
        for (float value : data) {
            buffer.putFloat(value);
        }

        return buffer.array();
    }

    @Override
    public String toString() {
        return "VECTOR" + "(" + dimensionCount + ")";
    }

    public static microsoft.sql.Vector valueOf(Object obj) {
        if (obj instanceof byte[]) {
            return fromBytes((byte[]) obj);
        } else if (obj instanceof microsoft.sql.Vector) {
            return (microsoft.sql.Vector) obj;
        } else if (obj instanceof float[]) {
            float[] objArray = (float[]) obj;
            return new Vector(objArray.length, VectorDimensionType.F32, objArray);
        } else {
            return null;
        }
    }

    public float[] getData() {
        return data;
    }

    public VectorDimensionType getVectorDimensionType() {
        return vectorType;
    }

    public int getDimensionCount() {
        return dimensionCount;
    }

    /**
     * Returns the scale for the vector type.
     * 0x00 for F32, 0x01 for F16.
     */
    public byte getScale() {
        switch (vectorType) {
            case F32:
                return 0x00; // Scale(dimension type) for F32
            case F16:
                return 0x01; // Scale(dimension type) for F16
            default:
                return 0x00; // Default case
        }
    }

    /**
     * Returns the actual length of the vector in bytes.
     * 8 bytes for header + 4 bytes per float value.
     */
    public int getActualLength() {
        int bytesPerDimension;
        switch (vectorType) {
            case F32:
                bytesPerDimension = 4; // 4 bytes per dimension for F32
                break;
            case F16:
                bytesPerDimension = 2; 
                break;
            default:
                bytesPerDimension = 4; 
                break;
        }
        return 8 + bytesPerDimension * dimensionCount; // 8-byte header + dimension payload
    }
}
