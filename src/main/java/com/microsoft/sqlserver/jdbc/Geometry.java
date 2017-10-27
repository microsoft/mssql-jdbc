package com.microsoft.sqlserver.jdbc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Locale;

public class Geometry extends SQLServerSpatialDatatype {
    private Geometry(String WellKnownText, int srid) {
        this.wkt = WellKnownText;
        this.srid = srid;
        
        parseWKTForSerialization(currentWktPos, -1, false);
        serializeToWkb(false);
        isNull = false;
    }

    private Geometry(byte[] wkb) {
        this.wkb = wkb;
        buffer = ByteBuffer.wrap(wkb);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        
        parseWkb();
        
        WKTsb = new StringBuffer();
        WKTsbNoZM = new StringBuffer();
        
        constructWKT(internalType, numberOfPoints, numberOfFigures, numberOfSegments, numberOfShapes);
        
        wkt = WKTsb.toString();
        wktNoZM = WKTsbNoZM.toString();
        isNull = false;
    }
    
    public Geometry() {
        // TODO Auto-generated constructor stub
    }
    
    public static Geometry STGeomFromText(String wkt, int srid) {
        return new Geometry(wkt, srid);
    }
    
    public static Geometry STGeomFromWKB(byte[] wkb) {
        return new Geometry(wkb);
    }
    
    public static Geometry deserialize(byte[] wkb) {
        return new Geometry(wkb);
    }

    public static Geometry parse(String wkt) {
        return new Geometry(wkt, 0);
    }
    
    public static Geometry point(double x, double y, int srid) {
        return new Geometry("POINT (" + x + " " + y + ")", srid);
    }
    
    public String STAsText() {
        if (null == wktNoZM) {
            buffer = ByteBuffer.wrap(wkb);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            
            parseWkb();
            
            WKTsb = new StringBuffer();
            WKTsbNoZM = new StringBuffer();
            constructWKT(internalType, numberOfPoints, numberOfFigures, numberOfSegments, numberOfShapes);
            wktNoZM = WKTsbNoZM.toString();
        }
        return wktNoZM;
    }
    
    public byte[] STAsBinary() {
        if (null == wkbNoZM) {
            serializeToWkb(true);
        }
        return wkbNoZM;
    }
    
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
    
    protected void constructWKT(InternalSpatialDatatype isd, int pointIndexEnd, int figureIndexEnd, int segmentIndexEnd, int shapeIndexEnd) {
        if (null == points || numberOfPoints == 0) {
            if (isd.getTypeCode() == 11) { // FULLGLOBE
                throw new IllegalArgumentException("Fullglobe is not supported for Geometry.");
            }
            appendToWKTBuffers(internalType + " EMPTY");
            return;
        }
        
        appendToWKTBuffers(isd.getTypeName());
        appendToWKTBuffers("(");

        switch (isd) {
            case POINT:
                constructPointWKT(currentPointIndex);
                break;
            case LINESTRING:
            case CIRCULARSTRING:
                constructLineWKT(currentPointIndex, pointIndexEnd);
                break;
            case POLYGON:
            case MULTIPOINT:
            case MULTILINESTRING:
                constructShapeWKT(currentFigureIndex, figureIndexEnd);
                break;
            case COMPOUNDCURVE:
                constructCompoundcurveWKT(currentSegmentIndex, segmentIndexEnd, pointIndexEnd);
                break;
            case MULTIPOLYGON:
                constructMultipolygonWKT(currentFigureIndex, figureIndexEnd);
                break;
            case GEOMETRYCOLLECTION:
                constructGeometryCollectionWKT(shapeIndexEnd);
                break;
            case CURVEPOLYGON:
                constructCurvepolygonWKT(currentFigureIndex, figureIndexEnd, currentSegmentIndex, segmentIndexEnd);
                break;
            default:
                throw new IllegalArgumentException();
        }
        
        appendToWKTBuffers(")");
    }
    
    protected void parseWKTForSerialization(int startPos, int parentShapeIndex, boolean isGeoCollection) {
        //after every iteration of this while loop, the currentWktPosition will be set to the
        //end of the geometry/geography shape, except for the very first iteration of it.
        //This means that there has to be comma (that separates the previous shape with the next shape),
        //or we expect a ')' that will close the entire shape and exit the method.
        
        while (hasMoreToken()) {
            if (startPos != 0) {
                if (wkt.charAt(currentWktPos) == ')') {
                    return;
                } else if (wkt.charAt(currentWktPos) == ',') {
                    currentWktPos++;
                }
            }

            String nextToken = getNextStringToken().toUpperCase(Locale.US);
            int thisShapeIndex;
            InternalSpatialDatatype isd = InternalSpatialDatatype.valueOf(nextToken);
            byte fa = 0;
            
            readOpenBracket();
            
            if (version == 1 && (nextToken.equals("CIRCULARSTRING") || nextToken.equals("COMPOUNDCURVE") ||
                    nextToken.equals("CURVEPOLYGON"))) {
                version = 2;
            }

            switch (nextToken) {
                case "POINT":
                    if (startPos == 0 && nextToken.toUpperCase().equals("POINT")) {
                        isSinglePoint = true;
                    }
                    
                    if (isGeoCollection) {
                        shapeList.add(new Shape(parentShapeIndex, figureList.size(), isd.getTypeCode()));
                        figureList.add(new Figure(FA_LINE, pointList.size()));
                    }
                    
                    readPointWkt();
                    break;
                case "LINESTRING":
                case "CIRCULARSTRING":
                    shapeList.add(new Shape(parentShapeIndex, figureList.size(), isd.getTypeCode()));
                    fa = isd.getTypeCode() == InternalSpatialDatatype.LINESTRING.getTypeCode() ? FA_STROKE : FA_EXTERIOR_RING;
                    figureList.add(new Figure(fa, pointList.size()));
                    
                    readLineWkt();
                    
                    if (startPos == 0 && nextToken.toUpperCase().equals("LINESTRING") && pointList.size() == 2) {
                        isSingleLineSegment = true;
                    }
                    break;
                case "POLYGON":
                case "MULTIPOINT":
                case "MULTILINESTRING":
                    thisShapeIndex = shapeList.size();
                    shapeList.add(new Shape(parentShapeIndex, figureList.size(), isd.getTypeCode()));
                    
                    readShapeWkt(thisShapeIndex, nextToken);

                    break;
                case "MULTIPOLYGON":
                    thisShapeIndex = shapeList.size();
                    shapeList.add(new Shape(parentShapeIndex, figureList.size(), isd.getTypeCode()));
                    
                    readMultiPolygonWkt(thisShapeIndex, nextToken);
   
                    break;
                case "COMPOUNDCURVE":
                    shapeList.add(new Shape(parentShapeIndex, figureList.size(), isd.getTypeCode()));
                    figureList.add(new Figure(FA_COMPOSITE_CURVE, pointList.size()));
                    
                    readCompoundCurveWkt(true);
                    
                    break;
                case "CURVEPOLYGON":
                    shapeList.add(new Shape(parentShapeIndex, figureList.size(), isd.getTypeCode()));

                    readCurvePolygon();
                    
                    break;
                case "GEOMETRYCOLLECTION":
                    thisShapeIndex = shapeList.size();
                    shapeList.add(new Shape(parentShapeIndex, figureList.size(), isd.getTypeCode()));
                    
                    parseWKTForSerialization(currentWktPos, thisShapeIndex, true);
                    
                    break;
                case "FULLGLOBE":
                    throw new IllegalArgumentException("Fullglobe is not supported for Geometry.");
                default:
                    throw new IllegalArgumentException("Illegal character at wkt position " + currentWktPos);
            }
            readCloseBracket();
        }
        
        populateStructures();
    }
}
