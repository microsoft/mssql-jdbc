/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;


/**
 * Geography datatype represents data in a round-earth coordinate system. This class will stay in this current package
 * for backwards compatibility.
 */
public class Geography extends SQLServerSpatialDatatype {

    Geography() {}

    /**
     * Private constructor used for creating a Geography object from WKT and Spatial Reference Identifier.
     * 
     * @param wkt
     *        Well-Known Text (WKT) provided by the user.
     * @param srid
     *        Spatial Reference Identifier (SRID) provided by the user.
     * @throws SQLServerException
     *         if an exception occurs
     */
    Geography(String wkt, int srid) throws SQLServerException {
        if (null == wkt || wkt.length() <= 0) {
            throwIllegalWKT();
        }

        this.wkt = wkt;
        this.srid = srid;

        parseWKTForSerialization(this, currentWktPos, -1, false);

        serializeToClr(false, this);
        isNull = false;
    }

    /**
     * Private constructor used for creating a Geography object from internal SQL Server format.
     * 
     * @param clr
     *        Internal SQL Server format provided by the user.
     * @throws SQLServerException
     *         if an exception occurs
     */
    protected Geography(byte[] clr) throws SQLServerException {
        if (null == clr || clr.length <= 0) {
            throwIllegalByteArray();
        }

        this.clr = clr;
        buffer = ByteBuffer.wrap(clr);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        parseClr(this);

        WKTsb = new StringBuffer();
        WKTsbNoZM = new StringBuffer();

        constructWKT(this, internalType, numberOfPoints, numberOfFigures, numberOfSegments, numberOfShapes);

        wkt = WKTsb.toString();
        wktNoZM = WKTsbNoZM.toString();
        isNull = false;
    }

    /**
     * Constructor for a Geography instance from an Open Geospatial Consortium (OGC) Well-Known Text (WKT)
     * representation augmented with any Z (elevation) and M (measure) values carried by the instance.
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
     * Constructor for a Geography instance from an Open Geospatial Consortium (OGC) Well-Known Binary (WKB)
     * representation.
     * 
     * Note: This method currently uses internal SQL Server format (CLR) to create a Geography instance, but in the
     * future this will be changed to accept WKB data instead, as the SQL Server counterpart of this method
     * (STGeomFromWKB) uses WKB. For existing users who are already using this method, consider switching to
     * deserialize(byte) instead.
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
     * Constructor for a Geography instance from an internal SQL Server format for spatial data.
     * 
     * @param clr
     *        Internal SQL Server format provided by the user.
     * @return Geography Geography instance created from clr
     * @throws SQLServerException
     *         if an exception occurs
     */
    public static Geography deserialize(byte[] clr) throws SQLServerException {
        return new Geography(clr);
    }

    /**
     * Constructor for a Geography instance from an Open Geospatial Consortium (OGC) Well-Known Text (WKT)
     * representation. Spatial Reference Identifier is defaulted to 4326.
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
     * Constructor for a Geography instance that represents a Point instance from its latitude and longitude values and
     * a Spatial Reference Identifier.
     * 
     * @param lat
     *        latitude
     * @param lon
     *        longitude
     * @param srid
     *        Spatial Reference Identifier value
     * @return Geography Geography instance
     * @throws SQLServerException
     *         if an exception occurs
     */
    public static Geography point(double lat, double lon, int srid) throws SQLServerException {
        return new Geography("POINT (" + lon + " " + lat + ")", srid);
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
            buffer = ByteBuffer.wrap(clr);
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            parseClr(this);

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
        if (null == wkb) {
            serializeToWkb(this);
        }
        return wkb;
    }

    /**
     * Returns the bytes that represent an internal SQL Server format of Geography type.
     * 
     * @return byte array representation of the Geography object.
     */
    public byte[] serialize() {
        return clr;
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
     * Returns the latitude value.
     * 
     * @return double value that represents the latitude.
     */
    public Double getLatitude() {
        if (null != internalType && internalType == InternalSpatialDatatype.POINT && yValues.length == 1) {
            return yValues[0];
        }
        return null;
    }

    /**
     * Returns the longitude value.
     * 
     * @return double value that represents the longitude.
     */
    public Double getLongitude() {
        if (null != internalType && internalType == InternalSpatialDatatype.POINT && xValues.length == 1) {
            return xValues[0];
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
}
