/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Geometry extends SQLServerSpatialDatatype {
    
    /**
     * Private constructor used for creating a Geometry object from WKT and srid.
     */
    private Geometry(String WellKnownText, int srid) {
        this.wkt = WellKnownText;
        this.srid = srid;
        
        try {
            parseWKTForSerialization(this, currentWktPos, -1, false);
        }
        catch (StringIndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Reached unexpected end of WKT. Please make sure WKT is valid.");
        }
        
        serializeToWkb(false);
        isNull = false;
    }

    /**
     * Private constructor used for creating a Geometry object from WKB.
     */
    private Geometry(byte[] wkb) {
        this.wkb = wkb;
        buffer = ByteBuffer.wrap(wkb);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        
        parseWkb();
        
        WKTsb = new StringBuffer();
        WKTsbNoZM = new StringBuffer();
        
        constructWKT(this, internalType, numberOfPoints, numberOfFigures, numberOfSegments, numberOfShapes);
        
        wkt = WKTsb.toString();
        wktNoZM = WKTsbNoZM.toString();
        isNull = false;
    }
    
    public Geometry() {
        // TODO Auto-generated constructor stub
    }
    
    /**
     * Returns a Geometry instance from an Open Geospatial Consortium (OGC) Well-Known Text (WKT)
     *  representation augmented with any Z (elevation) and M (measure) values carried by the instance.
     *  
     * @param wkt WKT
     * @param srid SRID
     * @return Geometry instance
     */
    public static Geometry STGeomFromText(String wkt, int srid) {
        return new Geometry(wkt, srid);
    }
    
    /**
     * Returns a Geometry instance from an Open Geospatial Consortium (OGC) 
     * Well-Known Binary (WKB) representation.
     * 
     * @param wkb WKB
     * @return Geometry instance
     */
    public static Geometry STGeomFromWKB(byte[] wkb) {
        return new Geometry(wkb);
    }
    
    /**
     * Returns a constructed Geometry from an internal SQL Server format for spatial data.
     * 
     * @param wkb WKB
     * @return Geometry instance
     */
    public static Geometry deserialize(byte[] wkb) {
        return new Geometry(wkb);
    }

    /**
     * Returns a Geometry instance from an Open Geospatial Consortium (OGC) Well-Known Text (WKT) representation.
     * SRID is defaulted to 0.
     * 
     * @param wkt WKT
     * @return Geometry instance
     */
    public static Geometry parse(String wkt) {
        return new Geometry(wkt, 0);
    }
    
    /**
     * Constructs a Geometry instance that represents a Point instance from its X and Y values and an SRID.
     * 
     * @param x x coordinate
     * @param y y coordinate
     * @param srid SRID
     * @return Geometry instance
     */
    public static Geometry point(double x, double y, int srid) {
        return new Geometry("POINT (" + x + " " + y + ")", srid);
    }
    
    /**
     * Returns the Open Geospatial Consortium (OGC) Well-Known Text (WKT) representation of a 
     * Geometry instance. This text will not contain any Z (elevation) or M (measure) values carried by the instance.
     * 
     * @return the WKT representation without the Z and M values.
     */
    public String STAsText() {
        if (null == wktNoZM) {
            buffer = ByteBuffer.wrap(wkb);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            
            parseWkb();
            
            WKTsb = new StringBuffer();
            WKTsbNoZM = new StringBuffer();
            constructWKT(this, internalType, numberOfPoints, numberOfFigures, numberOfSegments, numberOfShapes);
            wktNoZM = WKTsbNoZM.toString();
        }
        return wktNoZM;
    }
    
    /**
     *  Returns the Open Geospatial Consortium (OGC) Well-Known Binary (WKB) representation of a 
     *  Geometry instance. This value will not contain any Z or M values carried by the instance.
     * @return WKB
     */
    public byte[] STAsBinary() {
        if (null == wkbNoZM) {
            serializeToWkb(true);
        }
        return wkbNoZM;
    }
    
    /**
     *  Returns the bytes that represent an internal SQL Server format of Geometry type.
     * 
     * @return WKB
     */
    public byte[] serialize() {
        return wkb;
    }
    
    public boolean hasM() {
        return hasMvalues;
    }
    
    public boolean hasZ() {
        return hasZvalues;
    }
    
    public Double getX() {
        if (null != internalType && internalType == InternalSpatialDatatype.POINT && points.length == 2) {
            return points[0];
        }
        return null;
    }
    
    public Double getY() {
        if (null != internalType && internalType == InternalSpatialDatatype.POINT && points.length == 2) {
            return points[1];
        }
        return null;
    }
    
    public Double getM() {
        if (null != internalType && internalType == InternalSpatialDatatype.POINT && hasM()) {
            return mValues[0];
        }
        return null;
    }
    
    public Double getZ() {
        if (null != internalType && internalType == InternalSpatialDatatype.POINT && hasZ()) {
            return zValues[0];
        }
        return null;
    }

    public int getSrid() {
        return srid;
    }
    
    public boolean isNull() {
        return isNull;
    }
    
    public int STNumPoints() {
        return numberOfPoints;
    }

    /**
     * Returns the Open Geospatial Consortium (OGC) type name represented by a geometry instance.
     * 
     * @return type name
     */
    public String STGeometryType() {
        if (null != internalType) {
            return internalType.getTypeName();
        }
        return null;
    }
    
    public String asTextZM() {
        return wkt;
    }
    
    public String toString() {
        return wkt;
    }
    
    protected void serializeToWkb(boolean noZM) {
        ByteBuffer buf = ByteBuffer.allocate(determineWkbCapacity());
        createSerializationProperties();
        
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(srid);
        buf.put(version);
        buf.put(serializationProperties);
        
        if (!isSinglePoint && !isSingleLineSegment) {
            buf.putInt(numberOfPoints);
        }
        
        for (int i = 0; i < numberOfPoints; i++) {
            buf.putDouble(points[2 * i]);
            buf.putDouble(points[2 * i + 1]);
        }
        
        if (!noZM ) {
            if (hasZvalues) {
                for (int i = 0; i < numberOfPoints; i++) {
                    buf.putDouble(zValues[i]);
                }
            }
            
            if (hasMvalues) {
                for (int i = 0; i < numberOfPoints; i++) {
                    buf.putDouble(mValues[i]);
                }
            }
        }
        
        if (isSinglePoint || isSingleLineSegment) {
            wkb = buf.array();
            return;
        }
        
        buf.putInt(numberOfFigures);
        for (int i = 0; i < numberOfFigures; i++) {
            buf.put(figures[i].getFiguresAttribute());
            buf.putInt(figures[i].getPointOffset());
        }
        
        buf.putInt(numberOfShapes);
        for (int i = 0; i < numberOfShapes; i++) {
            buf.putInt(shapes[i].getParentOffset());
            buf.putInt(shapes[i].getFigureOffset());
            buf.put(shapes[i].getOpenGISType());
        }
        
        if (version == 2 && null != segments) {
            buf.putInt(numberOfSegments);
            for (int i = 0; i < numberOfSegments; i++) {
                buf.put(segments[i].getSegmentType());
            }
        }
        
        if (noZM) { 
            wkbNoZM = buf.array();
        } else {
            wkb = buf.array();

        }
        return;
    }
    
    protected void parseWkb() {
        srid = buffer.getInt();
        version = buffer.get();
        serializationProperties = buffer.get();
        
        interpretSerializationPropBytes();
        readNumberOfPoints();
        readPoints();
        
        if (hasZvalues) {
            readZvalues();
        }
        
        if (hasMvalues) {
            readMvalues();
        }
        
        if (!(isSinglePoint || isSingleLineSegment)) {
            readNumberOfFigures();
            readFigures();
            readNumberOfShapes();
            readShapes();
        }
        
        determineInternalType();

        if (buffer.hasRemaining()) {
            if (version == 2 && internalType.getTypeCode() != 8 && internalType.getTypeCode() != 11) {
                readNumberOfSegments();
                readSegments();
            }
        }
    }
    
    private void readPoints() {
        points = new double[2 * numberOfPoints];
        for (int i = 0; i < numberOfPoints; i++) {
            points[2 * i] = buffer.getDouble();
            points[2 * i + 1] = buffer.getDouble();
        }
    }
}
