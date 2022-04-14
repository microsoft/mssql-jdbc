/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.spatialdatatypes;

/**
 * Represents the internal makings of a Figure.
 *
 */
public class Figure {
    private byte figuresAttribute;
    private int pointOffset;

    /**
     * Creates a Figure object
     * 
     * @param figuresAttribute
     *        figures attribute
     * @param pointOffset
     *        points offset
     */
    public Figure(byte figuresAttribute, int pointOffset) {
        this.figuresAttribute = figuresAttribute;
        this.pointOffset = pointOffset;
    }

    /**
     * Returns the figuresAttribute value.
     * 
     * @return byte figuresAttribute value.
     */
    public byte getFiguresAttribute() {
        return figuresAttribute;
    }

    /**
     * Returns the pointOffset value.
     * 
     * @return int pointOffset value.
     */
    public int getPointOffset() {
        return pointOffset;
    }

    /**
     * Sets the figuresAttribute value.
     * 
     * @param fa
     *        figuresAttribute value.
     */
    public void setFiguresAttribute(byte fa) {
        figuresAttribute = fa;
    }
}
