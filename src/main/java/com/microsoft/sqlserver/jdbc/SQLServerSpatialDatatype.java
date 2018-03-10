/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

abstract class SQLServerSpatialDatatype {
    
    /**WKT = Well-Known-Text, WKB = Well-Knwon-Binary */
    /**As a general rule, the ~IndexEnd variables are non-inclusive (i.e. pointIndexEnd = 8 means the shape using it will
     * only go up to the 7th index of the array) */
    protected ByteBuffer buffer;
    protected InternalSpatialDatatype internalType;
    protected String wkt;
    protected String wktNoZM;
    protected byte[] wkb;
    protected byte[] wkbNoZM;
    protected int srid;
    protected byte version = 1;
    protected int numberOfPoints;
    protected int numberOfFigures;
    protected int numberOfShapes;
    protected int numberOfSegments;
    protected StringBuffer WKTsb;
    protected StringBuffer WKTsbNoZM;
    protected int currentPointIndex = 0;
    protected int currentFigureIndex = 0;
    protected int currentSegmentIndex = 0;
    protected int currentShapeIndex = 0;
    protected double points[];
    protected double zValues[];
    protected double mValues[];
    protected Figure figures[];
    protected Shape shapes[];
    protected Segment segments[];

    //serialization properties
    protected boolean hasZvalues = false;
    protected boolean hasMvalues = false;
    protected boolean isValid = false;
    protected boolean isSinglePoint = false;
    protected boolean isSingleLineSegment = false;
    protected boolean isLargerThanHemisphere = false;
    protected boolean isNull = true;
    
    protected final byte FA_INTERIOR_RING = 0;
    protected final byte FA_STROKE = 1;
    protected final byte FA_EXTERIOR_RING = 2;
    
    protected final byte FA_POINT = 0;
    protected final byte FA_LINE = 1;
    protected final byte FA_ARC = 2;
    protected final byte FA_COMPOSITE_CURVE = 3;
    
    // WKT to WKB properties
    protected int currentWktPos = 0;
    protected List<Point> pointList = new ArrayList<Point>();
    protected List<Figure> figureList = new ArrayList<Figure>();
    protected List<Shape> shapeList = new ArrayList<Shape>();
    protected List<Segment> segmentList = new ArrayList<Segment>();
    protected byte serializationProperties = 0;
    
    private final byte SEGMENT_LINE = 0;
    private final byte SEGMENT_ARC = 1;
    private final byte SEGMENT_FIRST_LINE = 2;
    private final byte SEGMENT_FIRST_ARC = 3;
    
    private final byte hasZvaluesMask =              0b00000001;
    private final byte hasMvaluesMask =              0b00000010;
    private final byte isValidMask =                 0b00000100;
    private final byte isSinglePointMask =           0b00001000;
    private final byte isSingleLineSegmentMask =     0b00010000;
    private final byte isLargerThanHemisphereMask =  0b00100000;
    
    private List<Integer> version_one_shape_indexes = new ArrayList<Integer>();
    
    /**
     * Serializes the Geogemetry/Geography instance to WKB.
     * 
     * @param noZM flag to indicate if Z and M coordinates should be included
     */
    protected abstract void serializeToWkb(boolean noZM);
    
    /**
     * Deserialize the buffer (that contains WKB representation of Geometry/Geography data), and stores it
     * into multiple corresponding data structures.
     * 
     */
    protected abstract void parseWkb();
    
    /**
     * Create the WKT representation of Geometry/Geography from the deserialized data.
     * 
     * @param sd the Geometry/Geography instance.
     * @param isd internal spatial datatype object
     * @param pointIndexEnd upper bound for reading points
     * @param figureIndexEnd upper bound for reading figures
     * @param segmentIndexEnd upper bound for reading segments
     * @param shapeIndexEnd upper bound for reading shapes
     */
    protected void constructWKT(SQLServerSpatialDatatype sd, InternalSpatialDatatype isd, int pointIndexEnd, int figureIndexEnd, 
            int segmentIndexEnd, int shapeIndexEnd) {
        if (null == points || numberOfPoints == 0) {
            if (isd.getTypeCode() == 11) { // FULLGLOBE
                if (sd instanceof Geometry) {
                    throw new IllegalArgumentException("Fullglobe is not supported for Geometry.");
                } else {
                    appendToWKTBuffers("FULLGLOBE");
                    return;
                }
            }
            // handle the case of GeometryCollection having empty objects
            if (isd.getTypeCode() == 7 && currentShapeIndex != shapeIndexEnd - 1) {
                currentShapeIndex++;
                appendToWKTBuffers(isd.getTypeName() + "(");
                constructWKT(this, InternalSpatialDatatype.valueOf(shapes[currentShapeIndex].getOpenGISType()),
                        numberOfPoints, numberOfFigures, numberOfSegments, numberOfShapes);
                appendToWKTBuffers(")");
                return;                
            }
            appendToWKTBuffers(isd.getTypeName() + " EMPTY");
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
                constructShapeWKT(currentFigureIndex, figureIndexEnd);
                break;
            case MULTIPOINT:
            case MULTILINESTRING:
                constructMultiShapeWKT(currentShapeIndex, shapeIndexEnd);
                break;
            case COMPOUNDCURVE:
                constructCompoundcurveWKT(currentSegmentIndex, segmentIndexEnd, pointIndexEnd);
                break;
            case MULTIPOLYGON:
                constructMultipolygonWKT(currentShapeIndex, shapeIndexEnd);
                break;
            case GEOMETRYCOLLECTION:
                constructGeometryCollectionWKT(shapeIndexEnd);
                break;
            case CURVEPOLYGON:
                constructCurvepolygonWKT(currentFigureIndex, figureIndexEnd, currentSegmentIndex, segmentIndexEnd);
                break;
            default:
                throw new IllegalArgumentException("Illegal character at wkt position " + currentWktPos);
        }
        
        appendToWKTBuffers(")");
    }
    
    /**
     * Parses WKT and populates the data structures of the Geometry/Geography instance.
     * 
     * @param sd the Geometry/Geography instance.
     * @param startPos The index to start from from the WKT.
     * @param parentShapeIndex The index of the parent's Shape in the shapes array. Used to determine this shape's parent.
     * @param isGeoCollection flag to indicate if this is part of a GeometryCollection.
     */
    protected void parseWKTForSerialization(SQLServerSpatialDatatype sd, int startPos, int parentShapeIndex, boolean isGeoCollection) {
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
            InternalSpatialDatatype isd = InternalSpatialDatatype.INVALID_TYPE;
            try {
                isd = InternalSpatialDatatype.valueOf(nextToken);
            }
            catch (Exception e) {
                throw new IllegalArgumentException("Illegal character at wkt position " + currentWktPos);
            }
            byte fa = 0;
            
            if (version == 1 && (nextToken.equals("CIRCULARSTRING") || nextToken.equals("COMPOUNDCURVE") ||
                    nextToken.equals("CURVEPOLYGON"))) {
                version = 2;
            }
            
            // check for FULLGLOBE before reading the first open bracket, since FULLGLOBE doesn't have one.
            if (nextToken.equals("FULLGLOBE")) {
                if (sd instanceof Geometry) {
                    throw new IllegalArgumentException("Fullglobe is not supported for Geometry.");
                }
                
                if (startPos != 0) {
                    throw new IllegalArgumentException("Illegal character at wkt position " + currentWktPos);
                }
                
                shapeList.add(new Shape(parentShapeIndex, -1, isd.getTypeCode()));
                isLargerThanHemisphere = true;
                version = 2;
                break;
            }

            // if next keyword is empty, continue the loop. 
            if (checkEmptyKeyword(parentShapeIndex, isd, false)) {
                continue;
            }
            
            readOpenBracket();
            
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
                    
                    parseWKTForSerialization(this, currentWktPos, thisShapeIndex, true);
                    
                    break;
                default:
                    throw new IllegalArgumentException("Illegal character at wkt position " + currentWktPos);
            }
            readCloseBracket();
        }
        
        populateStructures();
    }
    
    /**
     * Constructs and appends a Point type in WKT form to the stringbuffer.
     * There are two stringbuffers - WKTsb and WKTsbNoZM. WKTsb contains the X, Y, Z and M coordinates,
     * whereas WKTsbNoZM contains only X and Y coordinates.
     * 
     * @param pointIndex indicates which point to append to the stringbuffer.
     * 
     */
    protected void constructPointWKT(int pointIndex) {
        int firstPointIndex = pointIndex * 2;
        int secondPointIndex = firstPointIndex + 1;
        int zValueIndex = pointIndex;
        int mValueIndex = pointIndex;
        
        if (points[firstPointIndex] % 1 == 0) {
            appendToWKTBuffers((int) points[firstPointIndex]);
        } else {
            appendToWKTBuffers(points[firstPointIndex]);
        }
        appendToWKTBuffers(" ");

        if (points[secondPointIndex] % 1 == 0) {
            appendToWKTBuffers((int) points[secondPointIndex]);
        } else {
            appendToWKTBuffers(points[secondPointIndex]);
        }
        appendToWKTBuffers(" ");
        
        if (hasZvalues && !Double.isNaN(zValues[zValueIndex]) && !(zValues[zValueIndex] == 0)) {
            if (zValues[zValueIndex] % 1 == 0) {
                WKTsb.append((int) zValues[zValueIndex]);
            } else {
                WKTsb.append(zValues[zValueIndex]);
            }
            WKTsb.append(" ");
            
            if (hasMvalues && !Double.isNaN(mValues[mValueIndex]) && !(mValues[mValueIndex] <= 0)) {
                if (mValues[mValueIndex] % 1 == 0) {
                    WKTsb.append((int) mValues[mValueIndex]);
                } else {
                    WKTsb.append(mValues[mValueIndex]);
                }
                WKTsb.append(" ");
            }
        }
        
        currentPointIndex++;
        // truncate last space
        WKTsb.setLength(WKTsb.length() - 1);
        WKTsbNoZM.setLength(WKTsbNoZM.length() - 1);
    }

    /** 
     * Constructs a line in WKT form.
     * 
     * @param pointStartIndex .
     * @param pointEndIndex .
     */
    protected void constructLineWKT(int pointStartIndex, int pointEndIndex) {
        for (int i = pointStartIndex; i < pointEndIndex; i++) {
            constructPointWKT(i);
            
            // add ', ' to separate points, except for the last point
            if (i != pointEndIndex - 1) {
                appendToWKTBuffers(", ");
            }
        }
    }
    
    /**
     * Constructs a shape (simple Geometry/Geography entities that are contained within a single bracket) in WKT form.
     * 
     * @param figureStartIndex .
     * @param figureEndIndex .
     */
    protected void constructShapeWKT(int figureStartIndex, int figureEndIndex) {
        for (int i = figureStartIndex; i < figureEndIndex; i++) {
            appendToWKTBuffers("(");
            if (i != numberOfFigures - 1) { //not the last figure
                constructLineWKT(figures[i].getPointOffset(), figures[i + 1].getPointOffset());
            } else {
                constructLineWKT(figures[i].getPointOffset(), numberOfPoints);
            }
            
            if (i != figureEndIndex - 1) {
                appendToWKTBuffers("), ");
            } else {
                appendToWKTBuffers(")");
            }
        }
    }

    /**
     * Constructs a mutli-shape (MultiPoint / MultiLineString) in WKT form.
     * 
     * @param shapeStartIndex .
     * @param shapeEndIndex .
     */
    protected void constructMultiShapeWKT(int shapeStartIndex, int shapeEndIndex) {
        for (int i = shapeStartIndex + 1; i < shapeEndIndex; i++) {
            if (shapes[i].getFigureOffset() == -1) { // EMPTY
                appendToWKTBuffers("EMPTY");
            } else {
                constructShapeWKT(shapes[i].getFigureOffset(), shapes[i].getFigureOffset() + 1);
            }
            if (i != shapeEndIndex - 1) {
                appendToWKTBuffers(", ");
            }
        }
    }
    
    /**
     * Constructs a CompoundCurve in WKT form.
     * 
     * @param segmentStartIndex .
     * @param segmentEndIndex .
     * @param pointEndIndex .
     */
    protected void constructCompoundcurveWKT(int segmentStartIndex, int segmentEndIndex, int pointEndIndex) {
        for (int i = segmentStartIndex; i < segmentEndIndex; i++) {
            byte segment = segments[i].getSegmentType();
            constructSegmentWKT(i, segment, pointEndIndex);
            
            if (i == segmentEndIndex - 1) {
                appendToWKTBuffers(")");
                break;
            }
            
            switch (segment) {
                case 0:
                case 2:
                    if (segments[i + 1].getSegmentType() != 0) {
                        appendToWKTBuffers("), ");
                    }
                    break;
                case 1:
                case 3:
                    if (segments[i + 1].getSegmentType() != 1) {
                        appendToWKTBuffers("), ");
                    }
                    break;
                default:
                    return;
            }
        }
    }
    
    /**
     * Constructs a MultiPolygon in WKT form.
     * 
     * @param shapeStartIndex .
     * @param shapeEndIndex .
     */
    protected void constructMultipolygonWKT(int shapeStartIndex, int shapeEndIndex) {
        int figureStartIndex;
        int figureEndIndex;
        
        for (int i = shapeStartIndex + 1; i < shapeEndIndex; i++) {
            figureEndIndex = figures.length;
            if (shapes[i].getFigureOffset() == -1) { // EMPTY
                appendToWKTBuffers("EMPTY");
                if (!(i == shapeEndIndex - 1)) { // not the last exterior polygon of this multipolygon, add a comma
                    appendToWKTBuffers(", ");
                }
                continue;
            }
            figureStartIndex = shapes[i].getFigureOffset();
            if (i == shapes.length - 1) { // last shape
                figureEndIndex = figures.length;
            } else {
                // look ahead and find the next shape that doesn't have -1 as its figure offset (which signifies EMPTY)
                int tempCurrentShapeIndex = i + 1;
                // We need to iterate this through until the very end of the shapes list, since if the last shape
                // in this MultiPolygon is an EMPTY, it won't know what the correct figureEndIndex would be.
                while (tempCurrentShapeIndex < shapes.length) {
                    if (shapes[tempCurrentShapeIndex].getFigureOffset() == -1) {
                        tempCurrentShapeIndex++;
                        continue;
                    } else {
                        figureEndIndex = shapes[tempCurrentShapeIndex].getFigureOffset();
                        break;
                    }
                }
            }

            appendToWKTBuffers("(");
            
            for (int j = figureStartIndex; j < figureEndIndex; j++) {
                appendToWKTBuffers("(");// interior ring
                
                if (j == figures.length - 1) { // last figure
                    constructLineWKT(figures[j].getPointOffset(), numberOfPoints);
                } else {
                    constructLineWKT(figures[j].getPointOffset(), figures[j + 1].getPointOffset());
                }
                
                if (j == figureEndIndex - 1) { // last polygon of this multipolygon, close off the Multipolygon
                    appendToWKTBuffers(")");
                } else {  // not the last polygon, followed by an interior ring
                    appendToWKTBuffers("), ");
                }
            }
            
            appendToWKTBuffers(")");
            
            if (!(i == shapeEndIndex - 1)) { // not the last exterior polygon of this multipolygon, add a comma
                appendToWKTBuffers(", ");
            }
        }
    }
    
    /**
     * Constructs a CurvePolygon in WKT form.
     * 
     * @param figureStartIndex .
     * @param figureEndIndex .
     * @param segmentStartIndex .
     * @param segmentEndIndex .
     */
    protected void constructCurvepolygonWKT(int figureStartIndex, int figureEndIndex, int segmentStartIndex, int segmentEndIndex) {        
        for (int i = figureStartIndex; i < figureEndIndex; i++) {
            switch (figures[i].getFiguresAttribute()) {
                case 1: // line
                    appendToWKTBuffers("(");
                    
                    if (i == figures.length - 1) {
                        constructLineWKT(currentPointIndex, numberOfPoints);
                    } else {
                        constructLineWKT(currentPointIndex, figures[i + 1].getPointOffset());
                    }
                        
                    appendToWKTBuffers(")");
                    break;
                case 2: // arc
                    appendToWKTBuffers("CIRCULARSTRING(");
                    
                    if (i == figures.length - 1) {
                        constructLineWKT(currentPointIndex, numberOfPoints);
                    } else {
                        constructLineWKT(currentPointIndex, figures[i + 1].getPointOffset());
                    }
                        
                    appendToWKTBuffers(")");
                    
                    break;
                case 3: // composite curve
                    appendToWKTBuffers("COMPOUNDCURVE(");
                    
                    int pointEndIndex = 0;
                    
                    if (i == figures.length - 1) {
                        pointEndIndex = numberOfPoints;
                    } else {
                        pointEndIndex = figures[i + 1].getPointOffset();
                    }
                    
                    while (currentPointIndex < pointEndIndex) {
                        byte segment = segments[segmentStartIndex].getSegmentType();
                        constructSegmentWKT(segmentStartIndex, segment, pointEndIndex);
                        
                        if (!(currentPointIndex < pointEndIndex)) {
                            appendToWKTBuffers("))");
                        } else {
                            switch (segment) {
                                case 0:
                                case 2:
                                    if (segments[segmentStartIndex + 1].getSegmentType() != 0) {
                                        appendToWKTBuffers("), ");
                                    }
                                    break;
                                case 1:
                                case 3:
                                    if (segments[segmentStartIndex + 1].getSegmentType() != 1) {
                                        appendToWKTBuffers("), ");
                                    }
                                    break;
                                default:
                                    return;
                            }
                        }

                        segmentStartIndex++;
                    }
                    
                    break;
                default:
                    return;
            }
            
            //Append a comma if this is not the last figure of the shape.
            if (i != figureEndIndex - 1) {
                appendToWKTBuffers(", ");
            }
        }
    }
    
    /**
     * Constructs a Segment in WKT form.
     * SQL Server re-uses the last point of a segment if the following segment is of type 3 (first arc) or
     * type 2 (first line). This makes sense because the last point of a segment and the first point of the next
     * segment have to match for a valid curve. This means that the code has to look ahead and decide to decrement
     * the currentPointIndex depending on what segment comes next, since it may have been reused (and it's reflected
     * in the array of points)
     * 
     * @param currentSegment .
     * @param segment .
     * @param pointEndIndex .
     */
    protected void constructSegmentWKT(int currentSegment, byte segment, int pointEndIndex) {
        switch (segment) {
            case 0:
                appendToWKTBuffers(", ");
                constructLineWKT(currentPointIndex, currentPointIndex + 1);
                
                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 0) { // not being followed by another line, but not the last segment
                    currentPointIndex = currentPointIndex - 1;
                    incrementPointNumStartIfPointNotReused(pointEndIndex);
                }
                break;
                
            case 1:
                appendToWKTBuffers(", ");
                constructLineWKT(currentPointIndex, currentPointIndex + 2);
                
                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 1) { // not being followed by another arc, but not the last segment
                    currentPointIndex = currentPointIndex - 1; // only increment pointNumStart by one less than what we should be, since the last point will be reused
                    incrementPointNumStartIfPointNotReused(pointEndIndex);
                }

                break;
            case 2:
                appendToWKTBuffers("(");
                constructLineWKT(currentPointIndex, currentPointIndex + 2);
                
                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 0) { // not being followed by another line, but not the last segment
                    currentPointIndex = currentPointIndex - 1; // only increment pointNumStart by one less than what we should be, since the last point will be reused
                    incrementPointNumStartIfPointNotReused(pointEndIndex);
                }
                
                break;
            case 3:
                appendToWKTBuffers("CIRCULARSTRING(");
                constructLineWKT(currentPointIndex, currentPointIndex + 3);
                
                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 1) { // not being followed by another arc
                    currentPointIndex = currentPointIndex - 1; // only increment pointNumStart by one less than what we should be, since the last point will be reused
                    incrementPointNumStartIfPointNotReused(pointEndIndex);
                }

                break;
            default:
                return;
        }
    }
    
    /**
     * The starting point for constructing a GeometryCollection type in WKT form.
     * 
     * @param shapeEndIndex .
     */
    protected void constructGeometryCollectionWKT(int shapeEndIndex) {
        currentShapeIndex++;
        constructGeometryCollectionWKThelper(shapeEndIndex);
    }
    
    /**
     * Reads Point WKT and adds it to the list of points.
     * This method will read up until and including the comma that may come at the end of the Point WKT.
     */
    protected void readPointWkt() {
        int numOfCoordinates = 0;
        double sign;
        double coords[] = new double[4];
        
        while (numOfCoordinates < 4) {
            sign = 1;
            if (wkt.charAt(currentWktPos) == '-') {
                sign = -1;
                currentWktPos++;
            }
            
            int startPos = currentWktPos;
            
            if (wkt.charAt(currentWktPos) == ')') {
                break;
            }
            
            while (currentWktPos < wkt.length() && 
                    (Character.isDigit(wkt.charAt(currentWktPos))
                            || wkt.charAt(currentWktPos) == '.'       
                            || wkt.charAt(currentWktPos) == 'E'
                            || wkt.charAt(currentWktPos) == 'e')) {
                currentWktPos++;
            }
            
            try {
                coords[numOfCoordinates] = sign *
                        new BigDecimal(wkt.substring(startPos, currentWktPos)).doubleValue();
            } catch (Exception e) { //modify to conversion exception
                throw new IllegalArgumentException("Illegal character at wkt position " + currentWktPos); 
            }
            
            numOfCoordinates++;
            
            skipWhiteSpaces();
            
            // After skipping white space after the 4th coordinate has been read, the next
            // character has to be either a , or ), or the WKT is invalid.
            if (numOfCoordinates == 4) {
                if (wkt.charAt(currentWktPos) != ',' && wkt.charAt(currentWktPos) != ')') {
                    throw new IllegalArgumentException("Illegal character at wkt position " + currentWktPos); 
                }
            }
            
            if (wkt.charAt(currentWktPos) == ',') {
                // need at least 2 coordinates
                if (numOfCoordinates == 1) {
                    throw new IllegalArgumentException("Illegal character at wkt position " + currentWktPos); 
                }
                currentWktPos++;
                skipWhiteSpaces();
                break;
            }
            skipWhiteSpaces();
        }
        
        if (numOfCoordinates == 4) {
            hasZvalues = true;
            hasMvalues = true;
        } else if (numOfCoordinates == 3) {
            hasZvalues = true;
        }
        
        pointList.add(new Point(coords[0], coords[1], coords[2], coords[3]));
    }
    
    /**
     * Reads a series of Point types.
     */
    protected void readLineWkt() {
        while (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
            readPointWkt();
        }
    }
    
    /**
     * Reads a shape (simple Geometry/Geography entities that are contained within a single bracket) WKT.
     * 
     * @param parentShapeIndex shape index of the parent shape that called this method
     * @param nextToken next string token
     */
    protected void readShapeWkt(int parentShapeIndex, String nextToken) {
        byte fa = FA_POINT;
        while (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
            
            // if next keyword is empty, continue the loop.
            // Do not check this for polygon.
            if (!nextToken.equals("POLYGON") &&
                    checkEmptyKeyword(parentShapeIndex, InternalSpatialDatatype.valueOf(nextToken), true)) {
                continue;
            }
            
            if (nextToken.equals("MULTIPOINT")) {
                shapeList.add(new Shape(parentShapeIndex, figureList.size(), InternalSpatialDatatype.POINT.getTypeCode()));
            } else if (nextToken.equals("MULTILINESTRING")) {
                shapeList.add(new Shape(parentShapeIndex, figureList.size(), InternalSpatialDatatype.LINESTRING.getTypeCode()));
            }
            
            if (version == 1) {
                if (nextToken.equals("MULTIPOINT")) {
                    fa = FA_STROKE;
                } else if (nextToken.equals("MULTILINESTRING") || nextToken.equals("POLYGON")) {
                    fa = FA_EXTERIOR_RING;
                }
                version_one_shape_indexes.add(figureList.size());
            } else if (version == 2) {
                if (nextToken.equals("MULTIPOINT") || nextToken.equals("MULTILINESTRING") || 
                        nextToken.equals("POLYGON") || nextToken.equals("MULTIPOLYGON")) {
                    fa = FA_LINE;
                }
            }
            
            figureList.add(new Figure(fa, pointList.size()));
            readOpenBracket();
            readLineWkt();
            readCloseBracket();

            skipWhiteSpaces();
            
            if (wkt.charAt(currentWktPos) == ',') { // more rings to follow
                readComma();
            } else if (wkt.charAt(currentWktPos) == ')') { // about to exit while loop
                continue;
            } else { // unexpected input
                throw new IllegalArgumentException("Illegal character at wkt position " + currentWktPos);
            }
        }
    }
    
    /**
     * Reads a CurvePolygon WKT
     */
    protected void readCurvePolygon() {
        while (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
            String nextPotentialToken = getNextStringToken().toUpperCase(Locale.US);
            if (nextPotentialToken.equals("CIRCULARSTRING")) {
                figureList.add(new Figure(FA_ARC, pointList.size()));
                readOpenBracket();
                readLineWkt();
                readCloseBracket();
            } else if (nextPotentialToken.equals("COMPOUNDCURVE")) {
                figureList.add(new Figure(FA_COMPOSITE_CURVE, pointList.size()));
                readOpenBracket();
                readCompoundCurveWkt(true);
                readCloseBracket();
            } else if (wkt.charAt(currentWktPos) == '(') { //LineString
                figureList.add(new Figure(FA_LINE, pointList.size()));
                readOpenBracket();
                readLineWkt();
                readCloseBracket();
            } else {
                throw new IllegalArgumentException("Illegal character at wkt position " + currentWktPos);
            }
            
            if (wkt.charAt(currentWktPos) == ',') { // more polygons to follow
                readComma();
            } else if (wkt.charAt(currentWktPos) == ')') { // about to exit while loop
                continue;
            } else { // unexpected input
                throw new IllegalArgumentException("Illegal character at wkt position " + currentWktPos);
            }
        }
    }

    /**
     * Reads a MultiPolygon WKT
     * 
     * @param thisShapeIndex shape index of current shape
     * @param nextToken next string token
     */
    protected void readMultiPolygonWkt(int thisShapeIndex, String nextToken) {
        while (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
            if (checkEmptyKeyword(thisShapeIndex, InternalSpatialDatatype.valueOf(nextToken), true)) {
                continue;
            }
            shapeList.add(new Shape(thisShapeIndex, figureList.size(), InternalSpatialDatatype.POLYGON.getTypeCode())); //exterior polygon
            readOpenBracket();
            readShapeWkt(thisShapeIndex, nextToken);
            readCloseBracket();
            
            if (wkt.charAt(currentWktPos) == ',') { // more polygons to follow
                readComma();
            } else if (wkt.charAt(currentWktPos) == ')') { // about to exit while loop
                continue;
            } else { // unexpected input
                throw new IllegalArgumentException("Illegal character at wkt position " + currentWktPos);
            }
        }
    }
    
    /**
     * Reads a Segment WKT
     * 
     * @param segmentType segment type
     * @param isFirstIteration flag that indicates if this is the first iteration from the loop outside
     */
    protected void readSegmentWkt(int segmentType, boolean isFirstIteration) {
        segmentList.add(new Segment((byte) segmentType));
        
        int segmentLength = segmentType;
        
        // under 2 means 0 or 1 (possible values). 0 (line) has 1 point, and 1 (arc) has 2 points, so increment by one
        if (segmentLength < 2) { 
            segmentLength++;
        }
        
        for (int i = 0; i < segmentLength; i++) {
            //If a segment type of 2 (first line) or 3 (first arc) is not from the very first iteration of the while loop,
            //then the first point has to be a duplicate point from the previous segment, so skip the first point.
            if (i == 0 && !isFirstIteration && segmentType >= 2) {
                skipFirstPointWkt();
            } else {
                readPointWkt();
            }
        }
        
        if (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
            if (segmentType == SEGMENT_FIRST_ARC || segmentType == SEGMENT_ARC) {
                readSegmentWkt(SEGMENT_ARC, false);
            } else if (segmentType == SEGMENT_FIRST_LINE | segmentType == SEGMENT_LINE) {
                readSegmentWkt(SEGMENT_LINE, false);
            }
        }
    }

    /**
     * Reads a CompoundCurve WKT
     * 
     * @param isFirstIteration flag that indicates if this is the first iteration from the loop outside
     */
    protected void readCompoundCurveWkt(boolean isFirstIteration) {
        while (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
            String nextPotentialToken = getNextStringToken().toUpperCase(Locale.US);
            if (nextPotentialToken.equals("CIRCULARSTRING")) {
                readOpenBracket();
                readSegmentWkt(SEGMENT_FIRST_ARC, isFirstIteration);
                readCloseBracket();
            } else if (wkt.charAt(currentWktPos) == '(') {//LineString
                readOpenBracket();
                readSegmentWkt(SEGMENT_FIRST_LINE, isFirstIteration);
                readCloseBracket();
            } else {
                throw new IllegalArgumentException("Illegal character at wkt position " + currentWktPos);
            }
            
            isFirstIteration = false;
            
            if (wkt.charAt(currentWktPos) == ',') { // more polygons to follow
                readComma();
            } else if (wkt.charAt(currentWktPos) == ')') { // about to exit while loop
                continue;
            } else { // unexpected input
                throw new IllegalArgumentException("Illegal character at wkt position " + currentWktPos);
            }
        }        
    }

    /**
     * Reads the next string token (usually POINT, LINESTRING, etc.).
     * Then increments currentWktPos to the end of the string token.
     * 
     * @return the next string token
     */
    protected String getNextStringToken() {
        skipWhiteSpaces();
        int endIndex = currentWktPos;
        while (endIndex < wkt.length() && Character.isLetter(wkt.charAt(endIndex))) {
            endIndex++;
        }
        int temp = currentWktPos;
        currentWktPos = endIndex;
        skipWhiteSpaces();
        
        return wkt.substring(temp, endIndex);
    }

    /**
     * Populates the various data structures contained within the Geometry/Geography instace.
     */
    protected void populateStructures() {
        if (pointList.size() > 0) {
            points = new double[pointList.size()  * 2];
            
            for (int i = 0; i < pointList.size(); i++) {
                points[i * 2] = pointList.get(i).getX();
                points[i * 2 + 1] = pointList.get(i).getY();
            }
            
            if (hasZvalues) {
                zValues = new double[pointList.size()];
                for (int i = 0; i < pointList.size(); i++) {
                    zValues[i] = pointList.get(i).getZ();
                }
            }
            
            if (hasMvalues) {
                mValues = new double[pointList.size()];
                for (int i = 0; i < pointList.size(); i++) {
                    mValues[i] = pointList.get(i).getM();
                }
            }
        }
        
        // if version is 2, then we need to check for potential shapes (polygon & multi-shapes) that were
        // given their figure attributes as if it was version 1, since we don't know what would be the
        // version of the geometry/geography before we parse the entire WKT.
        if (version == 2) {
            for (int i = 0; i < version_one_shape_indexes.size(); i++) {
                figureList.get(version_one_shape_indexes.get(i)).setFiguresAttribute((byte) 1);
            }
        }
        
        if (figureList.size() > 0) {
            figures = new Figure[figureList.size()];
            
            for (int i = 0; i < figureList.size(); i++) {
                figures[i] = figureList.get(i);
            }
        }
        
        // There is an edge case of empty GeometryCollections being inside other GeometryCollections. In this case,
        // the figure offset of the very first shape (GeometryCollections) has to be -1, but this is not possible to know until
        // We've parsed through the entire WKT and confirmed that there are 0 points.
        // Therefore, if so, we make the figure offset of the first shape to be -1.
        if (pointList.size() == 0 && shapeList.size() > 0 && shapeList.get(0).getOpenGISType() == 7) {
            shapeList.get(0).setFigureOffset(-1);
        }
        
        if (shapeList.size() > 0) {
            shapes = new Shape[shapeList.size()];
            
            for (int i = 0; i < shapeList.size(); i++) {
                shapes[i] = shapeList.get(i);
            }
        }
        
        if (segmentList.size() > 0) {
            segments = new Segment[segmentList.size()];
            
            for (int i = 0; i < segmentList.size(); i++) {
                segments[i] = segmentList.get(i);
            }
        }
        
        numberOfPoints = pointList.size();
        numberOfFigures = figureList.size();
        numberOfShapes = shapeList.size();
        numberOfSegments = segmentList.size();
    }
    
    protected void readOpenBracket() {
        skipWhiteSpaces();
        if (wkt.charAt(currentWktPos) == '(') {
            currentWktPos++;
            skipWhiteSpaces();
        } else {
            throw new IllegalArgumentException("Illegal character at wkt position " + currentWktPos);
        }
    }
    
    protected void readCloseBracket() {
        skipWhiteSpaces();
        if (wkt.charAt(currentWktPos) == ')') {
            currentWktPos++;
            skipWhiteSpaces();
        } else {
            throw new IllegalArgumentException("Illegal character at wkt position " + currentWktPos);
        }
    }

    protected boolean hasMoreToken() {
        skipWhiteSpaces();
        return currentWktPos < wkt.length();
    }
    
    protected void createSerializationProperties() {
        serializationProperties = 0;
        if (hasZvalues) {
            serializationProperties += hasZvaluesMask;
        }
        
        if (hasMvalues) {
            serializationProperties += hasMvaluesMask;
        }
        
        if (isValid) {
            serializationProperties += isValidMask;
        }
        
        if (isSinglePoint) {
            serializationProperties += isSinglePointMask;
        }
        
        if (isSingleLineSegment) {
            serializationProperties += isSingleLineSegmentMask;
        }
        
        if (version == 2) {
            if (isLargerThanHemisphere) {
                serializationProperties += isLargerThanHemisphereMask;
            }
        }
    }
    
    protected int determineWkbCapacity() {
        int totalSize = 0;
        
        totalSize+=6; // SRID + version + SerializationPropertiesByte
        
        if (isSinglePoint || isSingleLineSegment) {
            totalSize += 16 * numberOfPoints;
            
            if (hasZvalues) {
                totalSize += 8 * numberOfPoints;
            }
            
            if (hasMvalues) {
                totalSize += 8 * numberOfPoints;
            }
            
            return totalSize;
        }
        
        int pointSize = 16;
        if (hasZvalues) {
            pointSize += 8;
        }
        
        if (hasMvalues) {
            pointSize += 8;
        }
        
        totalSize += 12; // 4 bytes for 3 ints, each representing the number of points, shapes and figures
        totalSize += numberOfPoints * pointSize;
        totalSize += numberOfFigures * 5;
        totalSize += numberOfShapes * 9;
        
        if (version == 2) {
            totalSize += 4; // 4 bytes for 1 int, representing the number of segments
            totalSize += numberOfSegments;
        }
        
        return totalSize;
    }
    
    /**
     * Append the data to both stringbuffers.
     * @param o data to append to the stringbuffers.
     */
    protected void appendToWKTBuffers(Object o) {
        WKTsb.append(o);
        WKTsbNoZM.append(o);
    }

    protected void interpretSerializationPropBytes() {
        hasZvalues = (serializationProperties & hasZvaluesMask) != 0;
        hasMvalues = (serializationProperties & hasMvaluesMask) != 0;
        isValid = (serializationProperties & isValidMask) != 0;
        isSinglePoint = (serializationProperties & isSinglePointMask) != 0;
        isSingleLineSegment = (serializationProperties & isSingleLineSegmentMask) != 0;
        isLargerThanHemisphere = (serializationProperties & isLargerThanHemisphereMask) != 0;
    }
    
    protected void readNumberOfPoints() {
        if (isSinglePoint) {
            numberOfPoints = 1;
        } else if (isSingleLineSegment) {
            numberOfPoints = 2;
        } else {
            numberOfPoints = buffer.getInt();
        }
    }
    
    protected void readZvalues() {
        zValues = new double[numberOfPoints];
        for (int i = 0; i < numberOfPoints; i++) {
            zValues[i] = buffer.getDouble();
        }
    }
    
    protected void readMvalues() {
        mValues = new double[numberOfPoints];
        for (int i = 0; i < numberOfPoints; i++) {
            mValues[i] = buffer.getDouble();
        }
    }
    
    protected void readNumberOfFigures() {
        numberOfFigures = buffer.getInt();
    }
    
    protected void readFigures() {
        byte fa;
        int po;
        figures = new Figure[numberOfFigures];
        for (int i = 0; i < numberOfFigures; i++) {
            fa = buffer.get();
            po = buffer.getInt();
            figures[i] = new Figure(fa, po);
        }
    }
    
    protected void readNumberOfShapes() {
        numberOfShapes = buffer.getInt();
    }
    
    protected void readShapes() {
        int po;
        int fo;
        byte ogt;
        shapes = new Shape[numberOfShapes];
        for (int i = 0; i < numberOfShapes; i++) {
            po = buffer.getInt();
            fo = buffer.getInt();
            ogt = buffer.get();
            shapes[i] = new Shape(po, fo, ogt);
        }
    }
    
    protected void readNumberOfSegments() {
        numberOfSegments = buffer.getInt();
    }
    
    protected void readSegments() {
        byte st;
        segments = new Segment[numberOfSegments];
        for (int i = 0; i < numberOfSegments; i++) {
            st = buffer.get();
            segments[i] = new Segment(st);
        }
    }

    protected void determineInternalType() {
        if (isSinglePoint) {
            internalType = InternalSpatialDatatype.POINT;
        } else if (isSingleLineSegment) {
            internalType = InternalSpatialDatatype.LINESTRING;
        } else {
            internalType = InternalSpatialDatatype.valueOf(shapes[0].getOpenGISType());
        }
    }
    
    protected boolean checkEmptyKeyword(int parentShapeIndex, InternalSpatialDatatype isd, boolean isInsideAnotherShape) {
        String potentialEmptyKeyword = getNextStringToken().toUpperCase(Locale.US);
        if (potentialEmptyKeyword.equals("EMPTY")) {
            
            byte typeCode = 0;
            
            if (isInsideAnotherShape) {
                byte parentTypeCode = isd.getTypeCode();
                if (parentTypeCode == 4) { // MultiPoint
                    typeCode = InternalSpatialDatatype.POINT.getTypeCode();
                } else if (parentTypeCode == 5) { // MultiLineString
                    typeCode = InternalSpatialDatatype.LINESTRING.getTypeCode();
                } else if (parentTypeCode == 6) { // MultiPolygon
                    typeCode = InternalSpatialDatatype.POLYGON.getTypeCode();
                } else if (parentTypeCode == 7) { // GeometryCollection
                    typeCode = InternalSpatialDatatype.GEOMETRYCOLLECTION.getTypeCode();
                } else {
                    throw new IllegalArgumentException("Illegal parentTypeCode."); 
                }
            } else {
                typeCode = isd.getTypeCode();
            }
            
            shapeList.add(new Shape(parentShapeIndex, -1, typeCode));
            skipWhiteSpaces();
            if (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) == ',') {
                currentWktPos++;
                skipWhiteSpaces();
            }
            return true;
        }
        
        if (!potentialEmptyKeyword.equals("")) {
            throw new IllegalArgumentException("Illegal character at wkt position " + currentWktPos); 
        }
        return false;
    }
    
    private void incrementPointNumStartIfPointNotReused(int pointEndIndex) {
        // We need to increment PointNumStart if the last point was actually not re-used in the points array.
        // 0 for pointNumEnd indicates that this check is not applicable.
        if (currentPointIndex + 1 >= pointEndIndex) {
            currentPointIndex++;
        }
    }
    
    /**
     * Helper used for resurcive iteration for constructing GeometryCollection in WKT form.
     * 
     * @param shapeEndIndex .
     */
    private void constructGeometryCollectionWKThelper(int shapeEndIndex) {
        //phase 1: assume that there is no multi - stuff and no geometrycollection
        while (currentShapeIndex < shapeEndIndex) {
            InternalSpatialDatatype isd = InternalSpatialDatatype.valueOf(shapes[currentShapeIndex].getOpenGISType());
            
            int figureIndex = shapes[currentShapeIndex].getFigureOffset();
            int pointIndexEnd = numberOfPoints;
            int figureIndexEnd = numberOfFigures;
            int segmentIndexEnd = numberOfSegments;
            int shapeIndexEnd = numberOfShapes;
            int figureIndexIncrement = 0;
            int segmentIndexIncrement = 0;
            int shapeIndexIncrement = 0;
            int localCurrentSegmentIndex = 0;
            int localCurrentShapeIndex = 0;
            
            switch (isd) {
                case POINT:
                    figureIndexIncrement++;
                    currentShapeIndex++;
                    break;
                case LINESTRING:
                case CIRCULARSTRING:
                    figureIndexIncrement++;
                    currentShapeIndex++;
                    pointIndexEnd = figures[figureIndex + 1].getPointOffset();
                    break;
                case POLYGON:
                case CURVEPOLYGON:
                    if (currentShapeIndex < shapes.length - 1) {
                        figureIndexEnd = shapes[currentShapeIndex + 1].getFigureOffset();
                    }
                    
                    figureIndexIncrement = figureIndexEnd - currentFigureIndex;
                    currentShapeIndex++;
                    
                    // Needed to keep track of which segment we are at, inside the for loop
                    localCurrentSegmentIndex = currentSegmentIndex;
                    
                    if (isd.equals(InternalSpatialDatatype.CURVEPOLYGON)) {
                        // assume Version 2

                        for (int i = currentFigureIndex; i < figureIndexEnd; i++) {
                            // Only Compoundcurves (with figure attribute 3) can have segments
                            if (figures[i].getFiguresAttribute() == 3) {
                                
                                int pointOffsetEnd;
                                if (i == figures.length - 1) {
                                    pointOffsetEnd = numberOfPoints;
                                } else {
                                    pointOffsetEnd = figures[i + 1].getPointOffset();
                                }
                                
                                int increment = calculateSegmentIncrement(localCurrentSegmentIndex, pointOffsetEnd - figures[i].getPointOffset());

                                segmentIndexIncrement = segmentIndexIncrement + increment;
                                localCurrentSegmentIndex = localCurrentSegmentIndex + increment;
                            }
                        }
                    }
                    
                    segmentIndexEnd = localCurrentSegmentIndex;
                    
                    break;
                case MULTIPOINT:
                case MULTILINESTRING:
                case MULTIPOLYGON:
                    //Multipoint and MultiLineString can go on for multiple Shapes, but eventually
                    //the parentOffset will signal the end of the object, or it's reached the end of the
                    //shapes array.
                    //There is also no possibility that a MultiPoint or MultiLineString would branch
                    //into another parent.
                    
                    int thisShapesParentOffset = shapes[currentShapeIndex].getParentOffset();
                    
                    int tempShapeIndex = currentShapeIndex;
                    
                    // Increment shapeStartIndex to account for the shape index that either Multipoint, MultiLineString
                    // or MultiPolygon takes up
                    tempShapeIndex++;
                    while (tempShapeIndex < shapes.length && 
                            shapes[tempShapeIndex].getParentOffset() != thisShapesParentOffset) {
                        if (!(tempShapeIndex == shapes.length - 1) && // last iteration, don't check for shapes[tempShapeIndex + 1]
                                !(shapes[tempShapeIndex + 1].getFigureOffset() == -1)) { // disregard EMPTY cases
                            figureIndexEnd = shapes[tempShapeIndex + 1].getFigureOffset();
                        }
                        tempShapeIndex++;
                    }
                    
                    figureIndexIncrement = figureIndexEnd - currentFigureIndex;
                    shapeIndexIncrement = tempShapeIndex - currentShapeIndex;
                    shapeIndexEnd = tempShapeIndex;
                    break;
                case GEOMETRYCOLLECTION:
                    appendToWKTBuffers(isd.getTypeName());
                    
                    // handle Empty GeometryCollection cases
                    if (shapes[currentShapeIndex].getFigureOffset() == -1) {
                        appendToWKTBuffers(" EMPTY");
                        currentShapeIndex++;
                        if (currentShapeIndex < shapeEndIndex) {
                            appendToWKTBuffers(", ");
                        }
                        continue;
                    }
                    
                    appendToWKTBuffers("(");
                    
                    int geometryCollectionParentIndex = shapes[currentShapeIndex].getParentOffset();
                    
                    // Needed to keep track of which shape we are at, inside the for loop
                    localCurrentShapeIndex = currentShapeIndex;
                    
                    while (localCurrentShapeIndex < shapes.length - 1 && 
                            shapes[localCurrentShapeIndex + 1].getParentOffset() > geometryCollectionParentIndex) {
                        localCurrentShapeIndex++;
                    }
                    // increment localCurrentShapeIndex one more time since it will be used as a shapeEndIndex parameter
                    // for constructGeometryCollectionWKT, and the shapeEndIndex parameter is used non-inclusively
                    localCurrentShapeIndex++;
                    
                    currentShapeIndex++;
                    constructGeometryCollectionWKThelper(localCurrentShapeIndex);
                    
                    if (currentShapeIndex < shapeEndIndex) {
                        appendToWKTBuffers("), ");
                    } else {
                        appendToWKTBuffers(")");
                    }

                    continue;
                case COMPOUNDCURVE:
                    if (currentFigureIndex == figures.length - 1) {
                        pointIndexEnd = numberOfPoints;
                    } else {
                        pointIndexEnd = figures[currentFigureIndex + 1].getPointOffset();
                    }

                    int increment = calculateSegmentIncrement(currentSegmentIndex, pointIndexEnd - 
                            figures[currentFigureIndex].getPointOffset());

                    segmentIndexIncrement = increment;
                    segmentIndexEnd = currentSegmentIndex + increment;
                    figureIndexIncrement++;
                    currentShapeIndex++;
                    break;
                case FULLGLOBE:
                    appendToWKTBuffers("FULLGLOBE");
                    break;
                default:
                    break;
            }
            
            constructWKT(this, isd, pointIndexEnd, figureIndexEnd, segmentIndexEnd, shapeIndexEnd);
            currentFigureIndex = currentFigureIndex + figureIndexIncrement;
            currentSegmentIndex = currentSegmentIndex + segmentIndexIncrement;
            currentShapeIndex = currentShapeIndex + shapeIndexIncrement;
            
            if (currentShapeIndex < shapeEndIndex) {
                appendToWKTBuffers(", ");
            }
        }
    }

    /**
     * Calculates how many segments will be used by this shape.
     * Needed to determine when the shape that uses segments (e.g. CompoundCurve) needs to stop reading
     * in cases where the CompoundCurve is included as part of GeometryCollection.
     * 
     * @param segmentStart .
     * @param pointDifference number of points that were assigned to this segment to be used.
     * @return the number of segments that will be used by this shape.
     */
    private int calculateSegmentIncrement(int segmentStart,
            int pointDifference) {
        
        int segmentIncrement = 0;
        
        while (pointDifference > 0) {
            switch (segments[segmentStart].getSegmentType()) {
                case 0:
                    pointDifference = pointDifference - 1;
                    
                    if (segmentStart == segments.length - 1 || pointDifference < 1) { // last segment
                        break;
                    } else if (segments[segmentStart + 1].getSegmentType() != 0) { // one point will be reused
                        pointDifference = pointDifference + 1;
                    }
                    break;
                case 1:
                    pointDifference = pointDifference - 2;
                    
                    if (segmentStart == segments.length - 1 || pointDifference < 1) { // last segment
                        break;
                    } else if (segments[segmentStart + 1].getSegmentType() != 1) { // one point will be reused
                        pointDifference = pointDifference + 1;
                    }
                    break;
                case 2:
                    pointDifference = pointDifference - 2;
                    
                    if (segmentStart == segments.length - 1 || pointDifference < 1) { // last segment
                        break;
                    } else if (segments[segmentStart + 1].getSegmentType() != 0) { // one point will be reused
                        pointDifference = pointDifference + 1;
                    }
                    break;
                case 3:
                    pointDifference = pointDifference - 3;
                    
                    if (segmentStart == segments.length - 1 || pointDifference < 1) { // last segment
                        break;
                    } else if (segments[segmentStart + 1].getSegmentType() != 1) { // one point will be reused
                        pointDifference = pointDifference + 1;
                    }
                    break;
                default:
                    return segmentIncrement;
            }
            segmentStart++;
            segmentIncrement++;
        }
        
        return segmentIncrement;
    }

    private void skipFirstPointWkt() {
        int numOfCoordinates = 0;
        
        while (numOfCoordinates < 4) {
            if (wkt.charAt(currentWktPos) == '-') {
                currentWktPos++;
            }
            
            if (wkt.charAt(currentWktPos) == ')') {
                break;
            }
            
            while (currentWktPos < wkt.length() && 
                    (Character.isDigit(wkt.charAt(currentWktPos))
                            || wkt.charAt(currentWktPos) == '.'       
                            || wkt.charAt(currentWktPos) == 'E'
                            || wkt.charAt(currentWktPos) == 'e')) {
                currentWktPos++;
            }
            
            skipWhiteSpaces();
            if (wkt.charAt(currentWktPos) == ',') {
                currentWktPos++;
                skipWhiteSpaces();
                numOfCoordinates++;
                break;
            }
            skipWhiteSpaces();
            
            numOfCoordinates++;
        }
    }
    
    private void readComma() {
        skipWhiteSpaces();
        if (wkt.charAt(currentWktPos) == ',') {
            currentWktPos++;
            skipWhiteSpaces();
        } else {
            throw new IllegalArgumentException("Illegal character at wkt position " + currentWktPos);
        }
    }
    
    private void skipWhiteSpaces() {
        while (currentWktPos < wkt.length() && Character.isWhitespace(wkt.charAt(currentWktPos))) {
            currentWktPos++;
        }
    }
}

/**
 * Class to hold and represent the internal makings of a Figure.
 *
 */
class Figure {
    private byte figuresAttribute;
    private int pointOffset;
    
    Figure(byte figuresAttribute, int pointOffset) {
        this.figuresAttribute = figuresAttribute;
        this.pointOffset = pointOffset;
    }
    
    public byte getFiguresAttribute() {
        return figuresAttribute;
    }
    
    public int getPointOffset() {
        return pointOffset;
    }
    
    public void setFiguresAttribute(byte fa) {
        figuresAttribute = fa;
    }
}

/**
 * Class to hold and represent the internal makings of a Shape.
 *
 */
class Shape {
    private int parentOffset;
    private int figureOffset;
    private byte openGISType;
    
    Shape(int parentOffset, int figureOffset, byte openGISType) {
        this.parentOffset = parentOffset;
        this.figureOffset = figureOffset;
        this.openGISType = openGISType;
    }
    
    public int getParentOffset() {
        return parentOffset;
    }
    
    public int getFigureOffset() {
        return figureOffset;
    }
    
    public byte getOpenGISType() {
        return openGISType;
    }
    
    public void setFigureOffset(int fo) {
        figureOffset = fo;
    }
    
}

/**
 * Class to hold and represent the internal makings of a Segment.
 *
 */
class Segment {
    private byte segmentType;
    
    Segment(byte segmentType) {
        this.segmentType = segmentType;
    }
    
    public byte getSegmentType() {
        return segmentType;
    }
}

/**
 * Class to hold and represent the internal makings of a Point.
 *
 */
class Point {
    private final double x;
    private final double y;
    private final double z;
    private final double m;
    
    Point(double x, double y, double z, double m) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.m = m;
    }
    
    public double getX() {
        return x;
    }
    
    public double getY() {
        return y;
    }
    
    public double getZ() {
        return z;
    }
    
    public double getM() {
        return m;
    }
}