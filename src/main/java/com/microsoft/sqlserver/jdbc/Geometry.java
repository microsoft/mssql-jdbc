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
    private int pointNumStart = 0;
    private int segmentNumStart = 0;
    private int shapeNumStart = 0;
    
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
        
        constructWKT(internalType, numberOfPoints);
        
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
    
    private void constructWKT(InternalSpatialDatatype it, int pointNumEnd) {
        //Might have to divide into Simple or Compound types, instead of type by type
        //Refer to the PDF for simple vs compound
        WKTsb.append(it.getTypeName());
        
        if (null == points || numberOfPoints == 0) {
            WKT = internalType + " EMPTY";
            return;
        }
        
        WKTsb.append("(");
        
        switch (it) {
            case POINT:
                constructPointWKT(pointNumStart);
                break;
            case LINESTRING:
            case CIRCULARSTRING:
                constructLineWKT(pointNumStart, pointNumEnd);
                break;
            case POLYGON:
            case MULTIPOINT:
            case MULTILINESTRING:
                constructSimpleWKT(0);
                break;
            case COMPOUNDCURVE:
                constructCompoundcurveWKT(segmentNumStart);
                break;
            case MULTIPOLYGON:
                constructMultipolygonWKT();
                break;
            case GEOMETRYCOLLECTION:
                constructGeometryCollectionWKT();
                break;
            case CURVEPOLYGON:
                constructCurvepolygonWKT();
                break;
            case FULLGLOBE:
                WKTsb.append("FULLGLOBE");
                break;
            default:
                break;
        }
        
        WKTsb.append(")");
    }

    private void constructPointWKT(int pointNum) {
        int firstPointIndex = pointNum * 2;
        int secondPointIndex = firstPointIndex + 1;
        int zValueIndex = pointNum;
        int mValueIndex = pointNum;
        
        
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
        
        pointNumStart++;
        WKTsb.setLength(WKTsb.length() - 1); // truncate last space
    }
    
    private void constructLineWKT(int startIndex, int endIndex) {
        for (int i = startIndex; i < endIndex; i++) {
            constructPointWKT(i);
            
            // add ', ' to separate points, except for the last point
            if (i != endIndex - 1) {
                WKTsb.append(", ");
            }
        }
    }
    

    private void constructSimpleWKT(int startIndex) {
        // Method for constructing Simple (i.e. not constructed of other Geometry/Geography objects)
        // Geometry/Geography objects.
        for (int i = startIndex; i < figures.length; i++) {
            WKTsb.append("(");
            if (i != figures.length - 1) { //not the last figure
                constructLineWKT(figures[i].getPointOffset(), figures[i + 1].getPointOffset());
            } else {
                constructLineWKT(figures[i].getPointOffset(), numberOfPoints);
            }
            
            if (i != figures.length - 1) {
                WKTsb.append("), ");
            } else {
                WKTsb.append(")");
            }
        }
    }

    private void constructCompoundcurveWKT(int startIndex) {
        for (int i = startIndex; i < segments.length; i++) {
            byte segment = segments[i].getSegmentType();
            constructSegmentWKT(i, segment, 0);
            
            if (i == segments.length - 1) {
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

    private void constructSegmentWKT(int currentSegment, byte segment, int pointNumEnd) {
        switch (segment) {
            case 0:
                WKTsb.append(", ");
                constructLineWKT(pointNumStart, pointNumStart + 1);
                
                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 0) { // not being followed by another line, but not the last segment
                    pointNumStart = pointNumStart - 1;
                    incrementPointNumStartIfPointNotReused(pointNumEnd);
                }
                break;
            case 1:
                WKTsb.append(", ");
                constructLineWKT(pointNumStart, pointNumStart + 2);
                
                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 1) { // not being followed by another arc, but not the last segment
                    pointNumStart = pointNumStart - 1; // only increment pointNumStart by one less than what we should be, since the last point will be reused
                    incrementPointNumStartIfPointNotReused(pointNumEnd);
                }

                break;
            case 2:
                WKTsb.append("(");
                constructLineWKT(pointNumStart, pointNumStart + 2);
                
                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 0) { // not being followed by another line, but not the last segment
                    pointNumStart = pointNumStart - 1; // only increment pointNumStart by one less than what we should be, since the last point will be reused
                    incrementPointNumStartIfPointNotReused(pointNumEnd);
                }
                
                break;
            case 3:
                WKTsb.append("CIRCULARSTRING(");
                constructLineWKT(pointNumStart, pointNumStart + 3);
                
                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 1) { // not being followed by another arc
                    pointNumStart = pointNumStart - 1; // only increment pointNumStart by one less than what we should be, since the last point will be reused
                    incrementPointNumStartIfPointNotReused(pointNumEnd);
                }

                break;
            default:
                return;
        }
    }
    
    private void incrementPointNumStartIfPointNotReused(int pointNumEnd) {
        // We need to increment PointNumStart if this constructSegmentWKT was called from a CurvePolygon, and the last point was actually not re-used in the
        // points array.
        // 0 for pointNumEnd indicates that this check is not applicable.
        if (0 != pointNumEnd && (pointNumStart + 1 >= pointNumEnd)) {
            pointNumStart++;
        }
    }

    private void constructMultipolygonWKT() {
        for (int i = 0; i < figures.length; i++) {
            if (figures[i].getFiguresAttribute() == 2) { // exterior ring
                WKTsb.append("((");
            } else { // interior ring
                WKTsb.append("(");
            }
            
            if (i == figures.length - 1) { // last polygon 
                constructLineWKT(figures[i].getPointOffset(), numberOfPoints);
            } else {
                constructLineWKT(figures[i].getPointOffset(), figures[i + 1].getPointOffset());
            }
            
            if (i == figures.length - 1) { // last polygon, close off the Multipolygon and return
                WKTsb.append("))");
                return;
            } else if (figures[i + 1].getFiguresAttribute() == 2) { // not the last polygon, followed by an exterior ring
                WKTsb.append(")), ");
            } else {  // not the last polygon, followed by an interior ring
                WKTsb.append("), ");
            }
        }
    }
    
    private void constructCurvepolygonWKT() {
        int currentSegment = 0;
        
        for (int i = 0; i < figures.length; i++) {
            switch (figures[i].getFiguresAttribute()) {
                case 1: // line
                    WKTsb.append("(");
                    
                    if (i == figures.length - 1) {
                        constructLineWKT(pointNumStart, numberOfPoints);
                    } else {
                        constructLineWKT(pointNumStart, figures[i + 1].getPointOffset());
                        pointNumStart = figures[i + 1].getPointOffset();
                    }
                        
                    WKTsb.append(")");
                    break;
                case 2: // arc
                    WKTsb.append("CIRCULARSTRING(");
                    
                    if (i == figures.length - 1) {
                        constructLineWKT(pointNumStart, numberOfPoints);
                    } else {
                        constructLineWKT(pointNumStart, figures[i + 1].getPointOffset());
                        pointNumStart = figures[i + 1].getPointOffset();
                    }
                        
                    WKTsb.append(")");
                    
                    break;
                case 3: // composite curve
                    WKTsb.append("COMPOUNDCURVE(");
                    
                    int pointNumEnd = 0;
                    
                    if (i == figures.length - 1) {
                        pointNumEnd = numberOfPoints;
                    } else {
                        pointNumEnd = figures[i + 1].getPointOffset();
                    }
                    
                    while (pointNumStart < pointNumEnd) {
                        byte segment = segments[currentSegment].getSegmentType();
                        constructSegmentWKT(currentSegment, segment, pointNumEnd);
                        
                        if (currentSegment == segments.length - 1) {
                            WKTsb.append(")");
                         // about to exit while loop, but not the last segment = we are closing Compoundcurve.
                        } else if (!(pointNumStart < pointNumEnd)) {
                            WKTsb.append("))");
                        } else {
                            switch (segment) {
                                case 0:
                                case 2:
                                    if (segments[currentSegment + 1].getSegmentType() != 0) {
                                        WKTsb.append("), ");
                                    }
                                    break;
                                case 1:
                                case 3:
                                    if (segments[currentSegment + 1].getSegmentType() != 1) {
                                        WKTsb.append("), ");
                                    }
                                    break;
                                default:
                                    return;
                            }
                        }

                        currentSegment++;
                    }
                    
                    break;
                default:
                    return;
            }
            
            if (i == figures.length - 1) {
                WKTsb.append(")");
            } else {
                WKTsb.append(", ");
            }
            
        }
    }
    
    private void constructGeometryCollectionWKT() {
        while (shapeNumStart < shapes.length) {
            byte openGISType = shapes[shapeNumStart].getOpenGISType();
            
            if (openGISType == 7) {
                // Another GeometryCollection inside another.
                shapeNumStart++;
                constructGeometryCollectionWKT();
                return;
            } else {
                if (shapeNumStart == shapes.length - 1) {
                    constructWKT(InternalSpatialDatatype.valueOf(openGISType), numberOfPoints);
                    WKTsb.append(")");
                } else {
                    int figureIndex = shapes[shapeNumStart].getFigureOffset();
                    if (figureIndex == figures.length - 1) {
                        constructWKT(InternalSpatialDatatype.valueOf(openGISType), numberOfPoints);
                    } else {
                        constructWKT(InternalSpatialDatatype.valueOf(openGISType), figures[figureIndex + 1].getPointOffset());
                    }
                    WKTsb.append(", ");
                }
            }
            
            shapeNumStart++;
        }
        
        /*
        for (int i = shapeNumStart; i < shapes.length; i++) {
            if (shapes[i].getOpenGISType() == 7) {
                
            } else {
                constructWKT(InternalSpatialDatatype.valueOf(shapes[i].getOpenGISType()));
            }
                
            if (i == figures.length - 1) {
                WKTsb.append(")");
            } else {
                WKTsb.append(", ");
            }
        }
        */
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