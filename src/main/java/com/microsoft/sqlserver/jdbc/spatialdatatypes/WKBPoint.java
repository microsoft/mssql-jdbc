/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.spatialdatatypes;

/**
 * Represents the internal makings of a WKB Point.
 *
 */
public class WKBPoint {
    private final double x;
    private final double y;

    /**
     * Creates a WKB Point object
     * 
     * @param x
     *        x value
     * @param y
     *        y value
     */
    public WKBPoint(double x, double y) {
        this.x = x;
        this.y = y;
    }

    /**
     * Returns the x coordinate value.
     * 
     * @return double x coordinate value.
     */
    public double getX() {
        return x;
    }

    /**
     * Returns the y coordinate value.
     * 
     * @return double y coordinate value.
     */
    public double getY() {
        return y;
    }
}
