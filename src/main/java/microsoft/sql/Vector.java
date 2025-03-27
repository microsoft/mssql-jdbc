/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package microsoft.sql;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class Vector implements java.io.Serializable {
    public enum VECTORTYPE {
        F16, // 16-bit (half precision) float
        F32 // 32-bit (single precision) float
    }

    public VECTORTYPE vectorType;
    public int dimensionCount;
    public float[] data;

    public Vector(float[] data, VECTORTYPE vectorType, int dimensionCount) {
        this.data = data;
        this.vectorType = vectorType;
        this.dimensionCount = dimensionCount;
    }

    public static microsoft.sql.Vector fromBytes(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        if (bytes.length % 4 != 0) {
            throw new IllegalArgumentException("Byte array length must be a multiple of 4.");
        }

        int floatCount = bytes.length / 4;
        float[] floatArray = new float[floatCount];

        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        for (int i = 0; i < floatCount; i++) {
            floatArray[i] = buffer.getFloat();
        }

        return new Vector(floatArray, VECTORTYPE.F32, floatCount);
    }

    public byte[] toBytes() {
        int payloadSize = 8 + (dimensionCount * 4); // 8-byte header + float payload

        ByteBuffer buffer = ByteBuffer.allocate(payloadSize).order(ByteOrder.LITTLE_ENDIAN);

        // 1. Layout Format (VECTOR marker)
        buffer.put((byte) 0xA9);

        // 2. Layout Version (always 1)
        buffer.put((byte) 0x01);

        // 3. Number of Dimensions (2 bytes, little-endian)
        buffer.putShort((short) (dimensionCount));

        // 4. Dimension Type (0x04 for float)
        buffer.put((byte) 0x00);

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
        } else {
            return null;
        }
    }
}
