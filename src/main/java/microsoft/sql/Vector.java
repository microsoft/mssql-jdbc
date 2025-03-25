/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

 package microsoft.sql;

 import java.nio.ByteBuffer;
 import java.nio.ByteOrder;
 import java.util.Arrays;
 
 public class Vector {
     private final float[] values;
 
     public Vector(byte[] data) {
         this.values = decodeVector(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN));
     }
 
     private float[] decodeVector(ByteBuffer buffer) {
         int numElements = buffer.remaining() / 4; 
         float[] vector = new float[numElements];
 
         for (int i = 0; i < numElements; i++) {
             vector[i] = buffer.getFloat();
         }
 
         return vector;
     }
 
     public float[] getValues() {
         return values;
     }
 
     @Override
     public String toString() {
         return Arrays.toString(values);
     }
 }
 
