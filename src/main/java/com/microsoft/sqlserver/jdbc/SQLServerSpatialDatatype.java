package com.microsoft.sqlserver.jdbc;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

abstract class SQLServerSpatialDatatype {
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
    
    private int currentShapeIndex = 0;
    private byte serializationProperties = 0;
    
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

    protected abstract void constructWKT(InternalSpatialDatatype isd, int pointIndexEnd, 
            int figureIndexEnd, int segmentIndexEnd, int shapeIndexEnd);
    
    protected abstract void parseWKTForSerialization(int startPos, int parentShapeIndex, boolean isGeoCollection);
    
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
        
        //TODO: do I need to do anything when it's isSinglePoint or isSingleLineSegment?
        if (!(isSinglePoint || isSingleLineSegment)) {
            readNumberOfFigures();
            
            readFigures();
            
            readNumberOfShapes();
            
            readShapes();
        }
        
        determineInternalType();

        if (version == 2 && internalType.getTypeCode() != 8 && internalType.getTypeCode() != 11) {
            readNumberOfSegments();
            
            readSegments();
        }
    }
    
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
        
        if (hasZvalues && !Double.isNaN(zValues[zValueIndex])) {
            if (zValues[zValueIndex] % 1 == 0) {
                WKTsb.append((int) zValues[zValueIndex]);
            } else {
                WKTsb.append(zValues[zValueIndex]);
            }
            WKTsb.append(" ");
            
            if (hasMvalues && !Double.isNaN(mValues[mValueIndex])) {
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

    protected void constructLineWKT(int pointStartIndex, int pointEndIndex) {
        for (int i = pointStartIndex; i < pointEndIndex; i++) {
            constructPointWKT(i);
            
            // add ', ' to separate points, except for the last point
            if (i != pointEndIndex - 1) {
                appendToWKTBuffers(", ");
            }
        }
    }
    
    protected void constructShapeWKT(int figureStartIndex, int figureEndIndex) {
        // Method for constructing shapes (simple Geometry/Geography entities that are contained within a single bracket)
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
    
    protected void constructMultipolygonWKT(int figureStartIndex, int figureEndIndex) {
        for (int i = figureStartIndex; i < figureEndIndex; i++) {
            if (figures[i].getFiguresAttribute() == 2) { // exterior ring
                appendToWKTBuffers("((");
            } else { // interior ring
                appendToWKTBuffers("(");
            }
            
            if (i == figures.length - 1) { // last figure
                constructLineWKT(figures[i].getPointOffset(), numberOfPoints);
            } else {
                constructLineWKT(figures[i].getPointOffset(), figures[i + 1].getPointOffset());
            }
            
            if (i == figureEndIndex - 1) { // last polygon of this multipolygon, close off the Multipolygon and return
                appendToWKTBuffers("))");
                return;
            } else if (figures[i + 1].getFiguresAttribute() == 2) { // not the last polygon, followed by an exterior ring
                appendToWKTBuffers(")), ");
            } else {  // not the last polygon, followed by an interior ring
                appendToWKTBuffers("), ");
            }
        }
    }
    
    protected void constructCurvepolygonWKT(int figureStartIndex, int figureEndIndex, int segmentStartIndex, int segmentEndIndex) {        
        for (int i = figureStartIndex; i < figureEndIndex; i++) {
            switch (figures[i].getFiguresAttribute()) {
                case 1: // line
                    appendToWKTBuffers("(");
                    
                    if (i == figures.length - 1) {
                        constructLineWKT(currentPointIndex, numberOfPoints);
                    } else {
                        constructLineWKT(currentPointIndex, figures[i + 1].getPointOffset());
                        //currentPointIndex = figures[i + 1].getPointOffset();
                    }
                        
                    appendToWKTBuffers(")");
                    break;
                case 2: // arc
                    appendToWKTBuffers("CIRCULARSTRING(");
                    
                    if (i == figures.length - 1) {
                        constructLineWKT(currentPointIndex, numberOfPoints);
                    } else {
                        constructLineWKT(currentPointIndex, figures[i + 1].getPointOffset());
                        //currentPointIndex = figures[i + 1].getPointOffset();
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
                        
                        if (segmentStartIndex >= segmentEndIndex - 1) {
                            appendToWKTBuffers(")");
                        // about to exit while loop, but not the last segment = we are closing Compoundcurve.
                        } else if (!(currentPointIndex < pointEndIndex)) {
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
            
            if (i == figureEndIndex - 1) {
                appendToWKTBuffers(")");
            } else {
                appendToWKTBuffers(", ");
            }
            
        }
    }
    
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
    
    protected void constructGeometryCollectionWKT(int shapeEndIndex) {
        currentShapeIndex++;
        constructGeometryCollectionWKThelper(shapeEndIndex);
    }
    
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
                throw new IllegalArgumentException(); 
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
        
        if (numOfCoordinates == 4) {
            hasZvalues = true;
            hasMvalues = true;
        } else if (numOfCoordinates == 3) {
            hasZvalues = true;
        }
        
        pointList.add(new Point(coords[0], coords[1], coords[2], coords[3]));
    }
    
    protected void readLineWkt() {
        while (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
            readPointWkt();
        }
    }
    
    protected void readShapeWkt(int parentShapeIndex, String nextToken) {
        byte fa = FA_POINT;
        while (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
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
                throw new IllegalArgumentException();
            }
        }
    }
    
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
                throw new IllegalArgumentException();
            }
            
            if (wkt.charAt(currentWktPos) == ',') { // more polygons to follow
                readComma();
            } else if (wkt.charAt(currentWktPos) == ')') { // about to exit while loop
                continue;
            } else { // unexpected input
                throw new IllegalArgumentException();
            }
        }
    }

    protected void readMultiPolygonWkt(int thisShapeIndex, String nextToken) {
        while (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
            shapeList.add(new Shape(thisShapeIndex, figureList.size(), InternalSpatialDatatype.POLYGON.getTypeCode())); //exterior polygon
            readOpenBracket();
            readShapeWkt(thisShapeIndex, nextToken);
            readCloseBracket();
            
            if (wkt.charAt(currentWktPos) == ',') { // more polygons to follow
                readComma();
            } else if (wkt.charAt(currentWktPos) == ')') { // about to exit while loop
                continue;
            } else { // unexpected input
                throw new IllegalArgumentException();
            }
        }
    }
    
    
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
                throw new IllegalArgumentException();
            }
            
            isFirstIteration = false;
            
            if (wkt.charAt(currentWktPos) == ',') { // more polygons to follow
                readComma();
            } else if (wkt.charAt(currentWktPos) == ')') { // about to exit while loop
                continue;
            } else { // unexpected input
                throw new IllegalArgumentException();
            }
        }        
    }

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
            throw new IllegalArgumentException();
        }
    }
    
    protected void readCloseBracket() {
        skipWhiteSpaces();
        if (wkt.charAt(currentWktPos) == ')') {
            currentWktPos++;
            skipWhiteSpaces();
        } else {
            throw new IllegalArgumentException();
        }
    }

    protected boolean hasMoreToken() {
        skipWhiteSpaces();
        return currentWktPos < wkt.length();
    }
    
    private void createSerializationProperties() {
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
        
        //TODO look into how the isLargerThanHemisphere is created
        if (version == 2) {
            if (isLargerThanHemisphere) {
                serializationProperties += isLargerThanHemisphereMask;
            }
        }
    }
    
    private int determineWkbCapacity() {
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

    private void interpretSerializationPropBytes() {
        hasZvalues = (serializationProperties & hasZvaluesMask) != 0;
        hasMvalues = (serializationProperties & hasMvaluesMask) != 0;
        isValid = (serializationProperties & isValidMask) != 0;
        isSinglePoint = (serializationProperties & isSinglePointMask) != 0;
        isSingleLineSegment = (serializationProperties & isSingleLineSegmentMask) != 0;
        isLargerThanHemisphere = (serializationProperties & isLargerThanHemisphereMask) != 0;
    }
    
    private void readNumberOfPoints() {
        if (isSinglePoint) {
            numberOfPoints = 1;
        } else if (isSingleLineSegment) {
            numberOfPoints = 2;
        } else {
            numberOfPoints = buffer.getInt();
        }
    }

    private void readPoints() {
        points = new double[2 * numberOfPoints];
        for (int i = 0; i < numberOfPoints; i++) {
            points[2 * i] = buffer.getDouble();
            points[2 * i + 1] = buffer.getDouble();
        }
    }
    
    private void readZvalues() {
        zValues = new double[numberOfPoints];
        for (int i = 0; i < numberOfPoints; i++) {
            zValues[i] = buffer.getDouble();
        }
    }
    
    private void readMvalues() {
        mValues = new double[numberOfPoints];
        for (int i = 0; i < numberOfPoints; i++) {
            mValues[i] = buffer.getDouble();
        }
    }
    
    private void readNumberOfFigures() {
        numberOfFigures = buffer.getInt();
    }
    
    private void readFigures() {
        byte fa;
        int po;
        figures = new Figure[numberOfFigures];
        for (int i = 0; i < numberOfFigures; i++) {
            fa = buffer.get();
            po = buffer.getInt();
            figures[i] = new Figure(fa, po);
        }
    }
    
    private void readNumberOfShapes() {
        numberOfShapes = buffer.getInt();
    }
    
    private void readShapes() {
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
    
    private void readNumberOfSegments() {
        numberOfSegments = buffer.getInt();
    }
    
    private void readSegments() {
        byte st;
        segments = new Segment[numberOfSegments];
        for (int i = 0; i < numberOfSegments; i++) {
            st = buffer.get();
            segments[i] = new Segment(st);
        }
    }

    private void determineInternalType() {
        if (isSinglePoint) {
            internalType = InternalSpatialDatatype.POINT;
        } else if (isSingleLineSegment) {
            internalType = InternalSpatialDatatype.LINESTRING;
        } else {
            internalType = InternalSpatialDatatype.valueOf(shapes[0].getOpenGISType());
        }
    }
    
    private void incrementPointNumStartIfPointNotReused(int pointEndIndex) {
        // We need to increment PointNumStart if the last point was actually not re-used in the points array.
        // 0 for pointNumEnd indicates that this check is not applicable.
        if (currentPointIndex + 1 >= pointEndIndex) {
            currentPointIndex++;
        }
    }
    
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
                    
                    // Increment shapeStartIndex to account for the shape index that either Multipoint, MultiLineString
                    // or MultiPolygon takes up
                    currentShapeIndex++;
                    while (currentShapeIndex < shapes.length - 1 && 
                            shapes[currentShapeIndex].getParentOffset() != thisShapesParentOffset) {
                        figureIndexEnd = shapes[currentShapeIndex + 1].getFigureOffset();
                        currentShapeIndex++;
                    }
                    
                    figureIndexIncrement = figureIndexEnd - currentFigureIndex;
                    break;
                case GEOMETRYCOLLECTION:
                    appendToWKTBuffers(isd.getTypeName());
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
            
            constructWKT(isd, pointIndexEnd, figureIndexEnd, segmentIndexEnd, shapeIndexEnd);
            currentFigureIndex = currentFigureIndex + figureIndexIncrement;
            currentSegmentIndex = currentSegmentIndex + segmentIndexIncrement;
            
            if (currentShapeIndex < shapeEndIndex) {
                appendToWKTBuffers(", ");
            }
        }
    }

    //Calculates how many segments will be used by this CompoundCurve
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
            throw new IllegalArgumentException();
        }
    }
    
    private void skipWhiteSpaces() {
        while (currentWktPos < wkt.length() && Character.isWhitespace(wkt.charAt(currentWktPos))) {
            currentWktPos++;
        }
    }
    
    protected void appendToWKTBuffers(Object o) {
        WKTsb.append(o);
        WKTsbNoZM.append(o);
    }
}


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
}

class Segment {
    private byte segmentType;
    
    Segment(byte segmentType) {
        this.segmentType = segmentType;
    }
    
    public byte getSegmentType() {
        return segmentType;
    }
}

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