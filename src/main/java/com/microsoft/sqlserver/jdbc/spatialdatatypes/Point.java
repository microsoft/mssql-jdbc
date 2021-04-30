/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.spatialdatatypes;

/**
 * Represents the internal makings of a Point.
 *
 */
public class Point {
    private final double x;
    private final double y;
    private final double z;
    private final double m;

    /**
     * Creates a Point object
     * 
     * @param x
     *        x value
     * @param y
     *        y value
     * @param z
     *        z value
     * @param m
     *        m value
     */
    public Point(double x, double y, double z, double m) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.m = m;
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

    /**
     * Returns the z (elevation) value.
     * 
     * @return double z value.
     */
    public double getZ() {
        return z;
    }

    /**
     * Returns the m (measure) value.
     * 
     * @return double m value.
     */
    public double getM() {
        return m;
    }
}
