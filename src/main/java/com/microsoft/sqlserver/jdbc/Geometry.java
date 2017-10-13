package com.microsoft.sqlserver.jdbc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Geometry {

    private ByteBuffer buffer;
    private InternalSpatialDatatype internalType;
    private String WKT;
    private int srid;
    private byte version;
    private byte serializationProperties;
    private int numberOfPoints;
    private int numberOfFigures;
    private int numberOfShapes;
    private int numberOfSegments;
    private double points[];
    private double zValues[];
    private double mValues[];
    private Figure figures[];
    private Shape shapes[];
    private Segment segments[];
    private StringBuffer WKTsb;
    private int currentPointIndex = 0;
    private int currentFigureIndex = 0;
    private int currentSegmentIndex = 0;
    private int currentShapeIndex = 0;
    
    //serialization properties
    private boolean hasZvalues;
    private boolean hasMvalues;
    private boolean isValid;
    private boolean isSinglePoint;
    private boolean isSingleLineSegment;
    
    private final byte hasZvaluesMask =          0b00000001;
    private final byte hasMvaluesMask =          0b00000010;
    private final byte isValidMask =             0b00000100;
    private final byte isSinglePointMask =       0b00001000;
    private final byte isSingleLineSegmentMask = 0b00010000;
    private final byte isLargerThanHemisphere =  0b00100000;
    
    
    public Geometry(String WellKnownText, int srid) {
        this.WKT = WellKnownText;
        this.srid = srid;
    }
    
    public Geometry(byte[] hexData) {
        buffer = ByteBuffer.wrap(hexData);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        
        parseHexData();
        
        WKTsb = new StringBuffer();
        
        constructWKT(internalType, numberOfPoints, numberOfFigures, numberOfSegments, numberOfShapes);
        
        WKT = WKTsb.toString();
    }
    
    public InternalSpatialDatatype getInternalType() {
        return internalType;
    }
    
    public int getSRID() {
        return srid;
    }
    
    public String toString() {
        return WKT;
    }
    
    private void parseHexData() {
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
        
        if (isSinglePoint || isSingleLineSegment) {

        } else {
            readNumberOfFigures();
            
            readFigures();
            
            readNumberOfShapes();
            
            readShapes();
        }
        
        determineInternalType();

        if (version == 2 && internalType.getTypeCode() != 8) {
            readNumberOfSegments();
            
            readSegments();
        }
    }

    private void interpretSerializationPropBytes() {
        hasZvalues = (serializationProperties & hasZvaluesMask) != 0;
        hasMvalues = (serializationProperties & hasMvaluesMask) != 0;
        isValid = (serializationProperties & isValidMask) != 0;
        isSinglePoint = (serializationProperties & isSinglePointMask) != 0;
        isSingleLineSegment = (serializationProperties & isSingleLineSegmentMask) != 0;
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
    
    private void constructWKT(InternalSpatialDatatype isd, int pointIndexEnd, int figureIndexEnd, int segmentIndexEnd, int shapeIndexEnd) {
        if (null == points || numberOfPoints == 0) {
            WKT = internalType + " EMPTY";
            return;
        }
        
        WKTsb.append(isd.getTypeName());
        WKTsb.append("(");

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
                constructSimpleWKT(currentFigureIndex, figureIndexEnd);
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
            case FULLGLOBE:
                //TODO: return error
                return;
            default:
                break;
        }
        
        WKTsb.append(")");
    }

    private void constructPointWKT(int pointIndex) {
        int firstPointIndex = pointIndex * 2;
        int secondPointIndex = firstPointIndex + 1;
        int zValueIndex = pointIndex;
        int mValueIndex = pointIndex;
        
        
        WKTsb.append(points[firstPointIndex]);
        WKTsb.append(" ");
        
        WKTsb.append(points[secondPointIndex]);
        WKTsb.append(" ");
        
        if (hasZvalues && !Double.isNaN(zValues[zValueIndex])) {
            WKTsb.append(zValues[zValueIndex]);
            WKTsb.append(" ");
            
            if (hasMvalues && !Double.isNaN(mValues[mValueIndex])) {
                WKTsb.append(mValues[mValueIndex]);
                WKTsb.append(" ");
            }
        }
        
        currentPointIndex++;
        WKTsb.setLength(WKTsb.length() - 1); // truncate last space
    }
    
    private void constructLineWKT(int pointStartIndex, int pointEndIndex) {
        for (int i = pointStartIndex; i < pointEndIndex; i++) {
            constructPointWKT(i);
            
            // add ', ' to separate points, except for the last point
            if (i != pointEndIndex - 1) {
                WKTsb.append(", ");
            }
        }
    }
    
    private void constructSimpleWKT(int figureStartIndex, int figureEndIndex) {
        // Method for constructing Simple (i.e. not constructed of other Geometry/Geography objects)
        // Geometry/Geography objects.
        for (int i = figureStartIndex; i < figureEndIndex; i++) {
            WKTsb.append("(");
            if (i != numberOfFigures - 1) { //not the last figure
                constructLineWKT(figures[i].getPointOffset(), figures[i + 1].getPointOffset());
            } else {
                constructLineWKT(figures[i].getPointOffset(), numberOfPoints);
            }
            
            if (i != figureEndIndex - 1) {
                WKTsb.append("), ");
            } else {
                WKTsb.append(")");
            }
        }
    }

    private void constructCompoundcurveWKT(int segmentStartIndex, int segmentEndIndex, int pointEndIndex) {
        for (int i = segmentStartIndex; i < segmentEndIndex; i++) {
            byte segment = segments[i].getSegmentType();
            constructSegmentWKT(i, segment, pointEndIndex);
            
            if (i == segmentEndIndex - 1) {
                WKTsb.append(")");
                break;
            }
            
            switch (segment) {
                case 0:
                case 2:
                    if (segments[i + 1].getSegmentType() != 0) {
                        WKTsb.append("), ");
                    }
                    break;
                case 1:
                case 3:
                    if (segments[i + 1].getSegmentType() != 1) {
                        WKTsb.append("), ");
                    }
                    break;
                default:
                    return;
            }
        }
    }

    private void constructSegmentWKT(int currentSegment, byte segment, int pointEndIndex) {
        switch (segment) {
            case 0:
                WKTsb.append(", ");
                constructLineWKT(currentPointIndex, currentPointIndex + 1);
                
                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 0) { // not being followed by another line, but not the last segment
                    currentPointIndex = currentPointIndex - 1;
                    incrementPointNumStartIfPointNotReused(pointEndIndex);
                }
                break;
                
            case 1:
                WKTsb.append(", ");
                constructLineWKT(currentPointIndex, currentPointIndex + 2);
                
                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 1) { // not being followed by another arc, but not the last segment
                    currentPointIndex = currentPointIndex - 1; // only increment pointNumStart by one less than what we should be, since the last point will be reused
                    incrementPointNumStartIfPointNotReused(pointEndIndex);
                }

                break;
            case 2:
                WKTsb.append("(");
                constructLineWKT(currentPointIndex, currentPointIndex + 2);
                
                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 0) { // not being followed by another line, but not the last segment
                    currentPointIndex = currentPointIndex - 1; // only increment pointNumStart by one less than what we should be, since the last point will be reused
                    incrementPointNumStartIfPointNotReused(pointEndIndex);
                }
                
                break;
            case 3:
                WKTsb.append("CIRCULARSTRING(");
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
    
    private void incrementPointNumStartIfPointNotReused(int pointEndIndex) {
        // We need to increment PointNumStart if the last point was actually not re-used in the points array.
        // 0 for pointNumEnd indicates that this check is not applicable.
        if (currentPointIndex + 1 >= pointEndIndex) {
            currentPointIndex++;
        }
    }

    private void constructMultipolygonWKT(int figureStartIndex, int figureEndIndex) {
        for (int i = figureStartIndex; i < figureEndIndex; i++) {
            if (figures[i].getFiguresAttribute() == 2) { // exterior ring
                WKTsb.append("((");
            } else { // interior ring
                WKTsb.append("(");
            }
            
            if (i == figures.length - 1) { // last figure
                constructLineWKT(figures[i].getPointOffset(), numberOfPoints);
            } else {
                constructLineWKT(figures[i].getPointOffset(), figures[i + 1].getPointOffset());
            }
            
            if (i == figureEndIndex - 1) { // last polygon of this multipolygon, close off the Multipolygon and return
                WKTsb.append("))");
                return;
            } else if (figures[i + 1].getFiguresAttribute() == 2) { // not the last polygon, followed by an exterior ring
                WKTsb.append(")), ");
            } else {  // not the last polygon, followed by an interior ring
                WKTsb.append("), ");
            }
        }
    }
    
    private void constructCurvepolygonWKT(int figureStartIndex, int figureEndIndex, int segmentStartIndex, int segmentEndIndex) {        
        for (int i = figureStartIndex; i < figureEndIndex; i++) {
            switch (figures[i].getFiguresAttribute()) {
                case 1: // line
                    WKTsb.append("(");
                    
                    if (i == figures.length - 1) {
                        constructLineWKT(currentPointIndex, numberOfPoints);
                    } else {
                        constructLineWKT(currentPointIndex, figures[i + 1].getPointOffset());
                        //currentPointIndex = figures[i + 1].getPointOffset();
                    }
                        
                    WKTsb.append(")");
                    break;
                case 2: // arc
                    WKTsb.append("CIRCULARSTRING(");
                    
                    if (i == figures.length - 1) {
                        constructLineWKT(currentPointIndex, numberOfPoints);
                    } else {
                        constructLineWKT(currentPointIndex, figures[i + 1].getPointOffset());
                        //currentPointIndex = figures[i + 1].getPointOffset();
                    }
                        
                    WKTsb.append(")");
                    
                    break;
                case 3: // composite curve
                    WKTsb.append("COMPOUNDCURVE(");
                    
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
                            WKTsb.append(")");
                        // about to exit while loop, but not the last segment = we are closing Compoundcurve.
                        } else if (!(currentPointIndex < pointEndIndex)) {
                            WKTsb.append("))");
                        } else {
                            switch (segment) {
                                case 0:
                                case 2:
                                    if (segments[segmentStartIndex + 1].getSegmentType() != 0) {
                                        WKTsb.append("), ");
                                    }
                                    break;
                                case 1:
                                case 3:
                                    if (segments[segmentStartIndex + 1].getSegmentType() != 1) {
                                        WKTsb.append("), ");
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
                WKTsb.append(")");
            } else {
                WKTsb.append(", ");
            }
            
        }
    }
    
    private void constructGeometryCollectionWKT(int shapeEndIndex) {
        currentShapeIndex++;
        constructGeometryCollectionWKThelper(shapeEndIndex);
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
                case POLYGON:;
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
                    WKTsb.append(isd.getTypeName());
                    WKTsb.append("(");
                    
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
                        WKTsb.append("), ");
                    } else {
                        WKTsb.append(")");
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
                    WKTsb.append("FULLGLOBE");
                    break;
                default:
                    break;
            }
            
            constructWKT(isd, pointIndexEnd, figureIndexEnd, segmentIndexEnd, shapeIndexEnd);
            currentFigureIndex = currentFigureIndex + figureIndexIncrement;
            currentSegmentIndex = currentSegmentIndex + segmentIndexIncrement;
            
            if (currentShapeIndex < shapeEndIndex) {
                WKTsb.append(", ");
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