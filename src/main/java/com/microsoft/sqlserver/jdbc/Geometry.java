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
    
    //serialization properties
    private boolean hasZvalues;
    private boolean hasMvalues;
    private boolean isValid ;
    private boolean isSinglePoint;
    private boolean isSingleLineSegment;
    
    private final byte hasZvaluesMask = 0b00000001; // 1
    private final byte hasMvaluesMask = 0b00000010; // 2
    private final byte isValidMask = 0b00000100; // 4
    private final byte isSinglePointMask = 0b00001000; // 8
    private final byte isSingleLineSegmentMask = 0b00010000; // 16
    
    
    public Geometry(String WellKnownText, int srid) {
        this.WKT = WellKnownText;
        this.srid = srid;
    }
    
    public Geometry(byte[] hexData) {
        buffer = ByteBuffer.wrap(hexData);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        
        parseHexData();
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
        
        readZvalues();
        
        readMvalues();
        
        readNumberOfFigures();
        
        readFigures();
        
        readNumberOfShapes();
        
        readShapes();
        
        if (version == 2) {
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
        for (int i = 0; i < numberOfSegments; i++) {
            st = buffer.get();
            segments[i] = new Segment(st);
        }
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