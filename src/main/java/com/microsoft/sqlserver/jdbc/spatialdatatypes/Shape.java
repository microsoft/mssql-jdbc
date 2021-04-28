/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.spatialdatatypes;

/**
 * Represents the internal makings of a Shape.
 *
 */
public class Shape {
    private int parentOffset;
    private int figureOffset;
    private byte openGISType;

    /**
     * Creates a Shape object
     * 
     * @param parentOffset
     *        parent offset
     * @param figureOffset
     *        figure offset
     * @param openGISType
     *        open GIS type
     */
    public Shape(int parentOffset, int figureOffset, byte openGISType) {
        this.parentOffset = parentOffset;
        this.figureOffset = figureOffset;
        this.openGISType = openGISType;
    }

    /**
     * Returns the parentOffset value.
     * 
     * @return int parentOffset value.
     */
    public int getParentOffset() {
        return parentOffset;
    }

    /**
     * Returns the figureOffset value.
     * 
     * @return int figureOffset value.
     */
    public int getFigureOffset() {
        return figureOffset;
    }

    /**
     * Returns the openGISType value.
     * 
     * @return byte openGISType value.
     */
    public byte getOpenGISType() {
        return openGISType;
    }

    /**
     * Sets the figureOffset value.
     * 
     * @param fo
     *        figureOffset value.
     */
    public void setFigureOffset(int fo) {
        figureOffset = fo;
    }
}
