/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.MessageFormat;


public class Geography extends SQLServerSpatialDatatype {

    /**
     * Private constructor used for creating a Geography object from WKT and srid.
     * 
     * @param WellKnownText
     *        Well-Known Text (WKT) provided by the user.
     * @param srid
     *        Spatial Reference Identifier (SRID) provided by the user.
     * @throws SQLServerException
     *         if an exception occurs
     */
    private Geography(String WellKnownText, int srid) throws SQLServerException {
        this.wkt = WellKnownText;
        this.srid = srid;

        try {
            parseWKTForSerialization(this, currentWktPos, -1, false);
        } catch (StringIndexOutOfBoundsException e) {
            String strError = SQLServerException.getErrString("R_illegalWKT");
            throw new SQLServerException(strError, null, 0, null);
        }

        serializeToWkb(false);
        isNull = false;
    }

    /**
     * Private constructor used for creating a Geography object from WKB.
     * 
     * @param wkb
     *        Well-Known Binary (WKB) provided by the user.
     * @throws SQLServerException
     *         if an exception occurs
     */
    private Geography(byte[] wkb) throws SQLServerException {
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

    /**
     * Returns a Geography instance from an Open Geospatial Consortium (OGC) Well-Known Text (WKT) representation
     * augmented with any Z (elevation) and M (measure) values carried by the instance.
     * 
     * @param wkt
     *        Well-Known Text (WKT) provided by the user.
     * @param srid
     *        Spatial Reference Identifier (SRID) provided by the user.
     * @return Geography Geography instance created from WKT and SRID
     * @throws SQLServerException
     *         if an exception occurs
     */
    public static Geography STGeomFromText(String wkt, int srid) throws SQLServerException {
        return new Geography(wkt, srid);
    }

    /**
     * Returns a Geography instance from an Open Geospatial Consortium (OGC) Well-Known Binary (WKB) representation.
     * 
     * @param wkb
     *        Well-Known Binary (WKB) provided by the user.
     * @return Geography Geography instance created from WKB
     * @throws SQLServerException
     *         if an exception occurs
     */
    public static Geography STGeomFromWKB(byte[] wkb) throws SQLServerException {
        return new Geography(wkb);
    }

    /**
     * Returns a constructed Geography from an internal SQL Server format for spatial data.
     * 
     * @param wkb
     *        Well-Known Binary (WKB) provided by the user.
     * @return Geography Geography instance created from WKB
     * @throws SQLServerException
     *         if an exception occurs
     */
    public static Geography deserialize(byte[] wkb) throws SQLServerException {
        return new Geography(wkb);
    }

    /**
     * Returns a Geography instance from an Open Geospatial Consortium (OGC) Well-Known Text (WKT) representation. SRID
     * is defaulted to 4326.
     * 
     * @param wkt
     *        Well-Known Text (WKT) provided by the user.
     * @return Geography Geography instance created from WKT
     * @throws SQLServerException
     *         if an exception occurs
     */
    public static Geography parse(String wkt) throws SQLServerException {
        return new Geography(wkt, 4326);
    }

    /**
     * Constructs a Geography instance that represents a Point instance from its X and Y values and an SRID.
     * 
     * @param x
     *        x coordinate
     * @param y
     *        y coordinate
     * @param srid
     *        SRID
     * @return Geography Geography instance
     * @throws SQLServerException
     *         if an exception occurs
     */
    public static Geography point(double x, double y, int srid) throws SQLServerException {
        return new Geography("POINT (" + x + " " + y + ")", srid);
    }

    /**
     * Returns the Open Geospatial Consortium (OGC) Well-Known Text (WKT) representation of a Geography instance. This
     * text will not contain any Z (elevation) or M (measure) values carried by the instance.
     * 
     * @return the WKT representation without the Z and M values.
     * @throws SQLServerException
     *         if an exception occurs
     */
    public String STAsText() throws SQLServerException {
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
     * Returns the Open Geospatial Consortium (OGC) Well-Known Binary (WKB) representation of a Geography instance. This
     * value will not contain any Z or M values carried by the instance.
     * 
     * @return byte array representation of the Geography object.
     */
    public byte[] STAsBinary() {
        if (null == wkbNoZM) {
            serializeToWkb(true);
        }
        return wkbNoZM;
    }

    /**
     * Returns the bytes that represent an internal SQL Server format of Geography type.
     * 
     * @return byte array representation of the Geography object.
     */
    public byte[] serialize() {
        return wkb;
    }

    /**
     * Returns if the object contains a M (measure) value.
     * 
     * @return boolean that indicates if the object contains M value.
     */
    public boolean hasM() {
        return hasMvalues;
    }

    /**
     * Returns if the object contains a Z (elevation) value.
     * 
     * @return boolean that indicates if the object contains Z value.
     */
    public boolean hasZ() {
        return hasZvalues;
    }

    /**
     * Returns the X coordinate value.
     * 
     * @return double value that represents the X coordinate.
     */
    public Double getX() {
        if (null != internalType && internalType == InternalSpatialDatatype.POINT && points.length == 2) {
            return points[0];
        }
        return null;
    }

    /**
     * Returns the Y coordinate value.
     * 
     * @return double value that represents the Y coordinate.
     */
    public Double getY() {
        if (null != internalType && internalType == InternalSpatialDatatype.POINT && points.length == 2) {
            return points[1];
        }
        return null;
    }

    /**
     * Returns the M (measure) value of the object.
     * 
     * @return double value that represents the M value.
     */
    public Double getM() {
        if (null != internalType && internalType == InternalSpatialDatatype.POINT && hasM()) {
            return mValues[0];
        }
        return null;
    }

    /**
     * Returns the Z (elevation) value of the object.
     * 
     * @return double value that represents the Z value.
     */
    public Double getZ() {
        if (null != internalType && internalType == InternalSpatialDatatype.POINT && hasZ()) {
            return zValues[0];
        }
        return null;
    }

    /**
     * Returns the Spatial Reference Identifier (SRID) value.
     * 
     * @return int value of SRID.
     */
    public int getSrid() {
        return srid;
    }

    /**
     * Returns if the Geography object is null.
     * 
     * @return boolean that indicates if the object is null.
     */
    public boolean isNull() {
        return isNull;
    }

    /**
     * Returns the number of points in the Geography object.
     * 
     * @return int that indicates the number of points in the Geography object.
     */
    public int STNumPoints() {
        return numberOfPoints;
    }

    /**
     * Returns the Open Geospatial Consortium (OGC) type name represented by a Geography instance.
     * 
     * @return String that contains the Geography object's type name
     */
    public String STGeographyType() {
        if (null != internalType) {
            return internalType.getTypeName();
        }
        return null;
    }

    /**
     * Returns the Well-Known Text (WKT) representation of the Geography object.
     * 
     * @return String that contains the WKT representation of the Geography object.
     */
    public String asTextZM() {
        return wkt;
    }

    /**
     * Returns the String representation of the Geography object.
     * 
     * @return String that contains the WKT representation of the Geography object.
     */
    @Override
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
            buf.putDouble(points[2 * i + 1]);
            buf.putDouble(points[2 * i]);
        }

        if (!noZM) {
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

    protected void parseWkb() throws SQLServerException {
        srid = readInt();
        version = readByte();
        serializationProperties = readByte();

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

    private void readPoints() throws SQLServerException {
        try {//if no limit of points is allowed, this should be an array of arrays
            points = new double[2 * numberOfPoints];
        } catch (NegativeArraySizeException | OutOfMemoryError e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ParsingError"));
            Object[] msgArgs = {JDBCType.VARBINARY};//should throw some kind of 'array size too large error here'
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }
        for (int i = 0; i < numberOfPoints; i++) {
            points[2 * i + 1] = readDouble();
            points[2 * i] = readDouble();
        }
    }
}