/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.spatialdatatypes;

/**
 * Represents the internal makings of a Segment.
 *
 */
public class Segment {
    private byte segmentType;

    /**
     * Creates a Segment object
     * 
     * @param segmentType
     *        segment type
     */
    public Segment(byte segmentType) {
        this.segmentType = segmentType;
    }

    /**
     * Returns the segmentType value.
     * 
     * @return byte segmentType value.
     */
    public byte getSegmentType() {
        return segmentType;
    }
}
