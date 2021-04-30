/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.spatialdatatypes;

/**
 * Represents the internal makings of a WKB Linear Ring.
 *
 */
public class WKBLinearRing {
    private final int numPoints;
    private final WKBPoint[] wkbPoints;

    /**
     * Creates a WKB Linear Ring object
     * 
     * @param numPoints
     *        num points
     * @param wkbPoints
     *        wkb points
     */
    public WKBLinearRing(int numPoints, WKBPoint[] wkbPoints) {
        this.numPoints = numPoints;
        this.wkbPoints = wkbPoints;
    }

    /**
     * Returns the number of points.
     * 
     * @return int number of points.
     */
    public int getNumPoints() {
        return numPoints;
    }

    /**
     * Returns the WKB points.
     * 
     * @return WKBPoint[] the WKB points.
     */
    public WKBPoint[] getWkbPoints() {
        return wkbPoints;
    }
}
