/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.microsoft.sqlserver.jdbc.spatialdatatypes.Figure;
import com.microsoft.sqlserver.jdbc.spatialdatatypes.Point;
import com.microsoft.sqlserver.jdbc.spatialdatatypes.Segment;
import com.microsoft.sqlserver.jdbc.spatialdatatypes.Shape;


/**
 * Abstract parent class for Spatial Datatypes that contains common functionalities.
 */
abstract class SQLServerSpatialDatatype {

    /** WKT = Well-Known-Text, WKB = Well-Knwon-Binary, CLR = Client Runtime Language */
    /**
     * As a general rule, the ~IndexEnd variables are non-inclusive (i.e. pointIndexEnd = 8 means the shape using it
     * will only go up to the 7th index of the array)
     */
    ByteBuffer buffer;
    InternalSpatialDatatype internalType;
    String wkt;
    String wktNoZM;
    byte[] clr;
    byte[] clrNoZM;
    int srid;
    byte version = 1;
    int numberOfPoints;
    int numberOfFigures;
    int numberOfShapes;
    int numberOfSegments;
    StringBuffer WKTsb;
    StringBuffer WKTsbNoZM;
    int currentPointIndex = 0;
    int currentFigureIndex = 0;
    int currentSegmentIndex = 0;
    int currentShapeIndex = 0;
    int currentWKBPointIndex = 0;
    int currentWKBFigureIndex = 0;
    int currentWKBSegmentIndex = 0;
    int currentWKBShapeIndex = 0;
    double xValues[];
    double yValues[];
    double zValues[];
    double mValues[];
    Figure figures[] = {};
    Shape shapes[] = {};
    Segment segments[] = {};

    // WKB properties
    byte[] wkb;
    byte endian = 1; // little endian
    int wkbType;
    /*
     * Open Geospatial Consortium specifications Document reference number: OGC 06-103r3
     */
    final private int WKB_POINT_SIZE = 16; // two doubles, x and y, are 16 bytes together
    final private int BYTE_ORDER_SIZE = 1;
    final private int INTERNAL_TYPE_SIZE = 4;
    final private int NUMBER_OF_SHAPES_SIZE = 4;
    final private int LINEAR_RING_HEADER_SIZE = 4;
    final private int WKB_POINT_HEADER_SIZE = BYTE_ORDER_SIZE + INTERNAL_TYPE_SIZE;
    final private int WKB_HEADER_SIZE = BYTE_ORDER_SIZE + INTERNAL_TYPE_SIZE + NUMBER_OF_SHAPES_SIZE;
    final private int WKB_FULLGLOBE_CODE = 126;

    // serialization properties
    boolean hasZvalues = false;
    boolean hasMvalues = false;
    boolean isValid = true;
    boolean isSinglePoint = false;
    boolean isSingleLineSegment = false;
    boolean isLargerThanHemisphere = false;
    boolean isNull = true;

    final byte FA_INTERIOR_RING = 0;
    final byte FA_STROKE = 1;
    final byte FA_EXTERIOR_RING = 2;

    final byte FA_POINT = 0;
    final byte FA_LINE = 1;
    final byte FA_ARC = 2;
    final byte FA_COMPOSITE_CURVE = 3;

    // WKT to CLR properties
    int currentWktPos = 0;
    List<Point> pointList = new ArrayList<Point>();
    List<Figure> figureList = new ArrayList<Figure>();
    List<Shape> shapeList = new ArrayList<Shape>();
    List<Segment> segmentList = new ArrayList<Segment>();
    byte serializationProperties = 0;

    private final byte SEGMENT_LINE = 0;
    private final byte SEGMENT_ARC = 1;
    private final byte SEGMENT_FIRST_LINE = 2;
    private final byte SEGMENT_FIRST_ARC = 3;

    private final byte hasZvaluesMask = 0b00000001;
    private final byte hasMvaluesMask = 0b00000010;
    private final byte isValidMask = 0b00000100;
    private final byte isSinglePointMask = 0b00001000;
    private final byte isSingleLineSegmentMask = 0b00010000;
    private final byte isLargerThanHemisphereMask = 0b00100000;

    private List<Integer> version_one_shape_indexes = new ArrayList<Integer>();

    /**
     * Serializes the Geogemetry/Geography instance to internal SQL Server format (CLR).
     * 
     * @param excludeZMFromCLR
     *        flag to indicate if Z and M coordinates should be excluded from the internal SQL Server format
     * @param type
     *        Type of Spatial Datatype (Geometry/Geography)
     */
    void serializeToClr(boolean excludeZMFromCLR, SQLServerSpatialDatatype type) {
        ByteBuffer buf = ByteBuffer.allocate(determineClrCapacity(excludeZMFromCLR));
        createSerializationProperties();

        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(srid);
        buf.put(version);
        if (excludeZMFromCLR) {
            byte serializationPropertiesNoZM = serializationProperties;
            if (hasZvalues) {
                serializationPropertiesNoZM -= hasZvaluesMask;
            }

            if (hasMvalues) {
                serializationPropertiesNoZM -= hasMvaluesMask;
            }
            buf.put(serializationPropertiesNoZM);
        } else {
            buf.put(serializationProperties);
        }

        if (!isSinglePoint && !isSingleLineSegment) {
            buf.putInt(numberOfPoints);
        }

        if (type instanceof Geometry) {
            for (int i = 0; i < numberOfPoints; i++) {
                buf.putDouble(xValues[i]);
                buf.putDouble(yValues[i]);
            }
        } else { // Geography
            for (int i = 0; i < numberOfPoints; i++) {
                buf.putDouble(yValues[i]);
                buf.putDouble(xValues[i]);
            }
        }

        if (!excludeZMFromCLR) {
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
            if (excludeZMFromCLR) {
                clrNoZM = buf.array();
            } else {
                clr = buf.array();
            }
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

        if (excludeZMFromCLR) {
            clrNoZM = buf.array();
        } else {
            clr = buf.array();
        }
    }

    /**
     * Serializes the Geogemetry/Geography instance to Well-Known binary format.
     * 
     * @param type
     *        Type of Spatial Datatype (Geometry/Geography)
     */
    void serializeToWkb(SQLServerSpatialDatatype type) {
        ByteBuffer buf = ByteBuffer.allocate(determineWkbCapacity());

        /*
         * Page 66 of OGC 06-103r3 (https://portal.ogc.org/files/?artifact_id=18241) Structure of a
         * WKBGeometry/WKBGeography representations The basic building block is the representation for a Point, which
         * consists of an x and y axis. Other Geometry representations are built using the representations for geometric
         * objects that have already been defined. For example, a LINESTRING(1 2, 3 2) shape is represented in WKB as
         * follows in hex: 0x010200000002000000000000000000F03F000000000000004000000000000008400000000000001040 We can
         * break down the above hex like this: 01 - byte order | one byte | currently representing little endian (big
         * endian is 02) 02000000 - Geometry code type 2 | four bytes | currently representing LINESTRING 02000000 - 02
         * | four bytes | currently representing that there are two POINTS in this LINESTRING
         * 000000000000F03F0000000000000040 16 bytes, 2 points with x and y axis (1, 2) 00000000000008400000000000000040
         * 16 bytes, 2 points with x and y axis (3, 2) There are geometric objects that contain other geometric objects,
         * such as MULTIPOINT. The below logic builds WKB from existing Geometry/Geography object and returns a byte
         * array.
         */

        buf.order(ByteOrder.LITTLE_ENDIAN);
        switch (internalType) {
            case POINT:
                addPointToBuffer(buf, numberOfPoints);
                break;
            case LINESTRING:
                addLineStringToBuffer(buf, numberOfPoints);
                break;
            case POLYGON:
                addPolygonToBuffer(buf, numberOfFigures);
                break;
            case MULTIPOINT:
                addMultiPointToBuffer(buf, numberOfFigures);
                break;
            case MULTILINESTRING:
                addMultiLineStringToBuffer(buf, numberOfFigures);
                break;
            case MULTIPOLYGON:
                addMultiPolygonToBuffer(buf, numberOfShapes - 1);
                break;
            case GEOMETRYCOLLECTION:
                addGeometryCollectionToBuffer(buf, calculateNumShapesInThisGeometryCollection());
                break;
            case CIRCULARSTRING:
                addCircularStringToBuffer(buf, numberOfPoints);
                break;
            case COMPOUNDCURVE:
                addCompoundCurveToBuffer(buf, calculateNumCurvesInThisFigure());
                break;
            case CURVEPOLYGON:
                addCurvePolygonToBuffer(buf, numberOfFigures);
                break;
            case FULLGLOBE:
                addFullGlobeToBuffer(buf);
                break;
            default:
                // Shouldn't be possible to come here, existing Geometry/Geography doesn't support other cases
                break;
        }

        wkb = buf.array();
    }

    private void addPointToBuffer(ByteBuffer buf, int numberOfPoints) {
        buf.put(endian);
        // handle special case where POINT EMPTY
        if (numberOfPoints == 0) {
            buf.putInt(InternalSpatialDatatype.MULTIPOINT.getTypeCode());
            buf.putInt(numberOfPoints);
        } else {
            buf.putInt(InternalSpatialDatatype.POINT.getTypeCode());
            addCoordinateToBuffer(buf, numberOfPoints);
            currentWKBFigureIndex++;
        }
    }

    private void addLineStringToBuffer(ByteBuffer buf, int numberOfPoints) {
        buf.put(endian);
        buf.putInt(InternalSpatialDatatype.LINESTRING.getTypeCode());
        buf.putInt(numberOfPoints);
        addCoordinateToBuffer(buf, numberOfPoints);
        if (numberOfPoints > 0) {
            currentWKBFigureIndex++;
        }
    }

    private void addPolygonToBuffer(ByteBuffer buf, int numberOfFigures) {
        buf.put(endian);
        buf.putInt(InternalSpatialDatatype.POLYGON.getTypeCode());
        buf.putInt(numberOfFigures);
        addStructureToBuffer(buf, numberOfFigures, InternalSpatialDatatype.POLYGON);
    }

    private void addMultiPointToBuffer(ByteBuffer buf, int numberOfFigures) {
        buf.put(endian);
        buf.putInt(InternalSpatialDatatype.MULTIPOINT.getTypeCode());
        buf.putInt(numberOfFigures);
        addStructureToBuffer(buf, numberOfFigures, InternalSpatialDatatype.MULTIPOINT);
    }

    private void addMultiLineStringToBuffer(ByteBuffer buf, int numberOfFigures) {
        buf.put(endian);
        buf.putInt(InternalSpatialDatatype.MULTILINESTRING.getTypeCode());
        buf.putInt(numberOfFigures);
        addStructureToBuffer(buf, numberOfFigures, InternalSpatialDatatype.MULTILINESTRING);
    }

    private void addMultiPolygonToBuffer(ByteBuffer buf, int numberOfShapes) {
        buf.put(endian);
        buf.putInt(InternalSpatialDatatype.MULTIPOLYGON.getTypeCode());
        buf.putInt(numberOfShapes);
        // increment shape index by 1 because the first shape is always itself, which we don't need.
        currentWKBShapeIndex++;
        addStructureToBuffer(buf, numberOfShapes, InternalSpatialDatatype.MULTIPOLYGON);
    }

    private void addCircularStringToBuffer(ByteBuffer buf, int numberOfPoints) {
        buf.put(endian);
        buf.putInt(InternalSpatialDatatype.CIRCULARSTRING.getTypeCode());
        buf.putInt(numberOfPoints);
        addCoordinateToBuffer(buf, numberOfPoints);
        if (numberOfPoints > 0) {
            currentWKBFigureIndex++;
        }
    }

    private void addCompoundCurveToBuffer(ByteBuffer buf, int numberOfCurves) {
        buf.put(endian);
        buf.putInt(InternalSpatialDatatype.COMPOUNDCURVE.getTypeCode());
        buf.putInt(numberOfCurves);
        addStructureToBuffer(buf, numberOfCurves, InternalSpatialDatatype.COMPOUNDCURVE);
        if (numberOfCurves > 0) {
            currentWKBFigureIndex++;
        }
    }

    private void addCurvePolygonToBuffer(ByteBuffer buf, int numberOfFigures) {
        buf.put(endian);
        buf.putInt(InternalSpatialDatatype.CURVEPOLYGON.getTypeCode());
        buf.putInt(numberOfFigures);
        for (int i = 0; i < numberOfFigures; i++) {
            switch (figures[currentWKBFigureIndex].getFiguresAttribute()) {
                case FA_LINE:
                    addStructureToBuffer(buf, 1, InternalSpatialDatatype.LINESTRING);
                    break;
                case FA_ARC:
                    addStructureToBuffer(buf, 1, InternalSpatialDatatype.CIRCULARSTRING);
                    break;
                case FA_COMPOSITE_CURVE:
                    int numCurvesInThisFigure = calculateNumCurvesInThisFigure();
                    buf.put(endian);
                    buf.putInt(InternalSpatialDatatype.COMPOUNDCURVE.getTypeCode());
                    buf.putInt(numCurvesInThisFigure);
                    addStructureToBuffer(buf, numCurvesInThisFigure, InternalSpatialDatatype.COMPOUNDCURVE);
                    currentWKBFigureIndex++;
                    break;
                default:
                    // Shouldn't be possible to come here, existing Geometry/Geography doesn't support other cases
                    break;
            }
        }
    }

    private void addGeometryCollectionToBuffer(ByteBuffer buf, int numberOfRemainingGeometries) {
        buf.put(endian);
        buf.putInt(internalType.getTypeCode());
        buf.putInt(numberOfRemainingGeometries);
        // increment shape index by 1 because the first shape is always itself, which we don't need.
        currentWKBShapeIndex++;
        while (numberOfRemainingGeometries > 0) {
            switch (InternalSpatialDatatype.valueOf(shapes[currentWKBShapeIndex].getOpenGISType())) {
                case POINT:
                    if (shapes[currentWKBShapeIndex].getFigureOffset() == -1) {
                        addPointToBuffer(buf, 0);
                    } else {
                        addPointToBuffer(buf, calculateNumPointsInThisFigure());
                    }
                    currentWKBShapeIndex++;
                    break;
                case LINESTRING:
                    if (shapes[currentWKBShapeIndex].getFigureOffset() == -1) {
                        addLineStringToBuffer(buf, 0);
                    } else {
                        addLineStringToBuffer(buf, calculateNumPointsInThisFigure());
                    }
                    currentWKBShapeIndex++;
                    break;
                case POLYGON:
                    if (shapes[currentWKBShapeIndex].getFigureOffset() == -1) {
                        addPolygonToBuffer(buf, 0);
                    } else {
                        addPolygonToBuffer(buf, calculateNumFiguresInThisShape(false));
                    }
                    currentWKBShapeIndex++;
                    break;
                case MULTIPOINT:
                    if (shapes[currentWKBShapeIndex].getFigureOffset() == -1) {
                        addMultiPointToBuffer(buf, 0);
                    } else {
                        addMultiPointToBuffer(buf, calculateNumFiguresInThisShape(true));
                    }
                    currentWKBShapeIndex++;
                    break;
                case MULTILINESTRING:
                    if (shapes[currentWKBShapeIndex].getFigureOffset() == -1) {
                        addMultiLineStringToBuffer(buf, 0);
                    } else {
                        addMultiLineStringToBuffer(buf, calculateNumFiguresInThisShape(true));
                    }
                    currentWKBShapeIndex++;
                    break;
                case MULTIPOLYGON:
                    /*
                     * increment WKBShapeIndex for all shapes except for GeometryCollection and Multipolygon, since
                     * their shape index was incremented earlier to be used for calculation.
                     */
                    if (shapes[currentWKBShapeIndex].getFigureOffset() == -1) {
                        addMultiPolygonToBuffer(buf, 0);
                    } else {
                        addMultiPolygonToBuffer(buf, calculateNumShapesInThisMultiPolygon());
                    }
                    break;
                case GEOMETRYCOLLECTION:
                    if (shapes[currentWKBShapeIndex].getFigureOffset() == -1) {
                        addGeometryCollectionToBuffer(buf, 0);
                    } else {
                        addGeometryCollectionToBuffer(buf, calculateNumShapesInThisGeometryCollection());
                    }
                    break;
                case CIRCULARSTRING:
                    if (shapes[currentWKBShapeIndex].getFigureOffset() == -1) {
                        addCircularStringToBuffer(buf, 0);
                    } else {
                        addCircularStringToBuffer(buf, calculateNumPointsInThisFigure());
                    }
                    currentWKBShapeIndex++;
                    break;
                case COMPOUNDCURVE:
                    if (shapes[currentWKBShapeIndex].getFigureOffset() == -1) {
                        addCompoundCurveToBuffer(buf, 0);
                    } else {
                        addCompoundCurveToBuffer(buf, calculateNumCurvesInThisFigure());
                    }
                    currentWKBShapeIndex++;
                    break;
                case CURVEPOLYGON:
                    if (shapes[currentWKBShapeIndex].getFigureOffset() == -1) {
                        addCurvePolygonToBuffer(buf, 0);
                    } else {
                        addCurvePolygonToBuffer(buf, calculateNumFiguresInThisShape(false));
                    }
                    currentWKBShapeIndex++;
                    break;
                default:
                    // Shouldn't be possible to come here, existing Geometry/Geography doesn't support other cases
                    break;
            }
            numberOfRemainingGeometries--;
        }
    }

    private void addFullGlobeToBuffer(ByteBuffer buf) {
        buf.put(endian);
        buf.putInt(WKB_FULLGLOBE_CODE);
    }

    private void addCoordinateToBuffer(ByteBuffer buf, int numPoint) {
        while (numPoint > 0) {
            buf.putDouble(xValues[currentWKBPointIndex]);
            buf.putDouble(yValues[currentWKBPointIndex]);
            currentWKBPointIndex++;
            numPoint--;
        }
    }

    private void addStructureToBuffer(ByteBuffer buf, int remainingStructureCount,
            InternalSpatialDatatype internalParentType) {
        int originalRemainingStructureCount = remainingStructureCount;
        while (remainingStructureCount > 0) {
            int numPointsInThisFigure = calculateNumPointsInThisFigure();
            switch (internalParentType) {
                case LINESTRING:
                    buf.put(endian);
                    buf.putInt(InternalSpatialDatatype.LINESTRING.getTypeCode());
                    buf.putInt(numPointsInThisFigure);
                    addCoordinateToBuffer(buf, numPointsInThisFigure);
                    currentWKBFigureIndex++;
                    break;
                case POLYGON:
                    buf.putInt(numPointsInThisFigure);
                    addCoordinateToBuffer(buf, numPointsInThisFigure);
                    currentWKBFigureIndex++;
                    break;
                case MULTIPOINT:
                    buf.put(endian);
                    buf.putInt(InternalSpatialDatatype.POINT.getTypeCode());
                    addCoordinateToBuffer(buf, 1);
                    currentWKBFigureIndex++;
                    currentWKBShapeIndex++;
                    break;
                case MULTILINESTRING:
                    buf.put(endian);
                    buf.putInt(InternalSpatialDatatype.LINESTRING.getTypeCode());
                    buf.putInt(numPointsInThisFigure);
                    addCoordinateToBuffer(buf, numPointsInThisFigure);
                    currentWKBFigureIndex++;
                    currentWKBShapeIndex++;
                    break;
                case MULTIPOLYGON:
                    int numFiguresInThisShape = calculateNumFiguresInThisShape(false);
                    buf.put(endian);
                    buf.putInt(InternalSpatialDatatype.POLYGON.getTypeCode());
                    buf.putInt(numFiguresInThisShape);
                    addStructureToBuffer(buf, numFiguresInThisShape, InternalSpatialDatatype.POLYGON);
                    currentWKBShapeIndex++;
                    break;
                case CIRCULARSTRING:
                    buf.put(endian);
                    buf.putInt(InternalSpatialDatatype.CIRCULARSTRING.getTypeCode());
                    buf.putInt(numPointsInThisFigure);
                    addCoordinateToBuffer(buf, numPointsInThisFigure);
                    currentWKBFigureIndex++;
                    break;
                case COMPOUNDCURVE:
                    /*
                     * COMPOUNDCURVEs are made of these four types of segments: SEGMENT_FIRST_ARC - It's a
                     * circularstring. It has 3 points and signals the beginning of an arc. SEGMENT_FIRST_LINE - It's a
                     * linestring. It has 2 points and signals the beginning of a line. SEGMENT_ARC - It only comes
                     * after SEGMENT_FIRST_ARC or other SEGMENT_ARCs. Adds 2 points each. SEGMENT_LINE - It only comes
                     * after SEGMENT_FIRST_LINE or other SEGMENT_LINE. Adds 1 point each. The FIRST_ARCs and FIRST_LINEs
                     * are considered as a full geometric object by WKB and it takes up a header spot, so we need to
                     * calculate how many of these occur in a compoundcurve and handle them individually. On the other
                     * hand, SEGMENT_ARCs and SEGMENT_LINEs only add up additional points.
                     */
                    if (segments[currentWKBSegmentIndex].getSegmentType() == SEGMENT_FIRST_ARC) {
                        int numberOfPointsInStructure = 3;
                        currentWKBSegmentIndex++;
                        while (currentWKBSegmentIndex < segments.length
                                && segments[currentWKBSegmentIndex].getSegmentType() != SEGMENT_FIRST_ARC
                                && segments[currentWKBSegmentIndex].getSegmentType() != SEGMENT_FIRST_LINE) {
                            numberOfPointsInStructure = numberOfPointsInStructure + 2;
                            currentWKBSegmentIndex++;
                        }
                        buf.put(endian);
                        buf.putInt(InternalSpatialDatatype.CIRCULARSTRING.getTypeCode());
                        buf.putInt(numberOfPointsInStructure);
                        if (originalRemainingStructureCount != remainingStructureCount) {
                            currentWKBPointIndex--;
                        }
                        addCoordinateToBuffer(buf, numberOfPointsInStructure);
                    } else if (segments[currentWKBSegmentIndex].getSegmentType() == SEGMENT_FIRST_LINE) {
                        int numberOfPointsInStructure = 2;
                        currentWKBSegmentIndex++;
                        while (currentWKBSegmentIndex < segments.length
                                && segments[currentWKBSegmentIndex].getSegmentType() != SEGMENT_FIRST_ARC
                                && segments[currentWKBSegmentIndex].getSegmentType() != SEGMENT_FIRST_LINE) {
                            numberOfPointsInStructure++;
                            currentWKBSegmentIndex++;
                        }
                        buf.put(endian);
                        buf.putInt(InternalSpatialDatatype.LINESTRING.getTypeCode());
                        buf.putInt(numberOfPointsInStructure);
                        if (originalRemainingStructureCount != remainingStructureCount) {
                            currentWKBPointIndex--;
                        }
                        addCoordinateToBuffer(buf, numberOfPointsInStructure);
                    } else {
                        // CompoundCurves should start with first arc or first line, it should not come here
                        break;
                    }
                    break;
                default:
                    // Shouldn't be possible to come here, existing Geometry/Geography doesn't support other cases
                    break;
            }
            remainingStructureCount--;
        }
    }

    private int calculateNumPointsInThisFigure() {
        if (figures.length == 0) {
            return 0;
        }

        /*
         * Calculates the number of points in this figure. Whenever this method is used, it's because this shape of
         * geometry/geography uses figures to keep track of which internal shape holds how many points. For example, we
         * have a curvepolygon has 3 shapes inside it, where the first shape has 3 points, the second shape has 5 points
         * and the third shape has 2 points. In order to determine that the first shape has 3 points, we look ahead one
         * figure, and see that the figure that comes after this figure has 3 as its point offset. Since current point
         * offset is 0 (since we haven't counted any points yet), we know that this figure has (3 - 0) = 3 points
         * inside. If we were to call this method one more time, we repeat the same process - we look ahead one figure,
         * find out that the figure has 8 as its point offset, which means that this figure has (8 - 3) = 5 points. The
         * 3rd call to this method sees that this is the last figure, so it does (10 - 8) = 2 points, where 10 is the
         * total number of points in the curvepolygon.
         */
        return (currentWKBFigureIndex == figures.length - 1)
                                                             ? (numberOfPoints
                                                                     - figures[currentWKBFigureIndex].getPointOffset())
                                                             : (figures[currentWKBFigureIndex + 1].getPointOffset()
                                                                     - figures[currentWKBFigureIndex].getPointOffset());
    }

    private int calculateNumCurvesInThisFigure() {
        int numPointsInThisFigure = calculateNumPointsInThisFigure();
        int numCurvesInThisFigure = 0;
        int tempCurrentWKBSegmentIndex = currentWKBSegmentIndex;
        boolean isFirstSegment = true;
        while (numPointsInThisFigure > 0) {
            switch (segments[tempCurrentWKBSegmentIndex].getSegmentType()) {
                case SEGMENT_LINE:
                    numPointsInThisFigure--;
                    break;
                case SEGMENT_ARC:
                    numPointsInThisFigure -= 2;
                    break;
                case SEGMENT_FIRST_LINE:
                    if (isFirstSegment) {
                        numPointsInThisFigure -= 2;
                    } else {
                        numPointsInThisFigure -= 1;
                    }
                    numCurvesInThisFigure++;
                    break;
                case SEGMENT_FIRST_ARC:
                    if (isFirstSegment) {
                        numPointsInThisFigure -= 3;
                    } else {
                        numPointsInThisFigure -= 2;
                    }
                    numCurvesInThisFigure++;
                    break;
                default:
                    // Shouldn't be possible to come here, existing Geometry/Geography doesn't support other cases
                    break;
            }
            isFirstSegment = false;
            tempCurrentWKBSegmentIndex++;
        }
        return numCurvesInThisFigure;
    }

    private int calculateNumFiguresInThisShape(boolean containsInnerStructures) {
        if (shapes.length == 0) {
            return 0;
        }

        if (containsInnerStructures) {
            int nextNonInnerShapeIndex = currentWKBShapeIndex + 1;
            while (nextNonInnerShapeIndex < shapes.length
                    && shapes[nextNonInnerShapeIndex].getParentOffset() == currentWKBShapeIndex) {
                nextNonInnerShapeIndex++;
            }

            if (nextNonInnerShapeIndex == shapes.length) {
                return (numberOfFigures - shapes[currentWKBShapeIndex].getFigureOffset());
            } else {
                int figureIndexEnd = -1;
                int localCurrentShapeIndex = nextNonInnerShapeIndex;
                while (figureIndexEnd == -1 && localCurrentShapeIndex < shapes.length - 1) {
                    figureIndexEnd = shapes[localCurrentShapeIndex + 1].getFigureOffset();
                    localCurrentShapeIndex++;
                }

                if (figureIndexEnd == -1) {
                    figureIndexEnd = numberOfFigures;
                }

                return figureIndexEnd - shapes[currentWKBShapeIndex].getFigureOffset();
            }
        } else {
            if (currentWKBShapeIndex == shapes.length - 1) {
                return numberOfFigures - shapes[currentWKBShapeIndex].getFigureOffset();
            } else {
                int figureIndexEnd = -1;
                int localCurrentShapeIndex = currentWKBShapeIndex;
                while (figureIndexEnd == -1 && localCurrentShapeIndex < shapes.length - 1) {
                    figureIndexEnd = shapes[localCurrentShapeIndex + 1].getFigureOffset();
                    localCurrentShapeIndex++;
                }

                if (figureIndexEnd == -1) {
                    figureIndexEnd = numberOfFigures;
                }

                return figureIndexEnd - shapes[currentWKBShapeIndex].getFigureOffset();
            }
        }
    }

    private int calculateNumShapesInThisMultiPolygon() {
        if (shapes.length == 0) {
            return 0;
        }

        int nextNonInnerShapeIndex = currentWKBShapeIndex + 1;
        while (nextNonInnerShapeIndex < shapes.length
                && shapes[nextNonInnerShapeIndex].getParentOffset() == currentWKBShapeIndex) {
            nextNonInnerShapeIndex++;
        }

        return (nextNonInnerShapeIndex - currentWKBShapeIndex - 1); // subtract 1 for Multipolygon itself
    }

    private int calculateNumShapesInThisGeometryCollection() {
        int numberOfGeometries = 0;
        for (int i = 0; i < shapes.length; i++) {
            if (shapes[i].getParentOffset() == currentWKBShapeIndex) {
                numberOfGeometries++;
            }
        }
        return numberOfGeometries;
    }

    /**
     * Deserializes the buffer (that contains CLR representation of Geometry/Geography data), and stores it into
     * multiple corresponding data structures.
     * 
     * @param type
     *        Type of Spatial Datatype (Geography/Geometry)
     * @throws SQLServerException
     *         if an Exception occurs
     */
    void parseClr(SQLServerSpatialDatatype type) throws SQLServerException {
        srid = readInt();
        version = readByte();
        serializationProperties = readByte();

        interpretSerializationPropBytes();
        readNumberOfPoints();
        readPoints(type);

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

    /**
     * Constructs the WKT representation of Geometry/Geography from the deserialized data.
     * 
     * @param sd
     *        the Geometry/Geography instance.
     * @param isd
     *        internal spatial datatype object
     * @param pointIndexEnd
     *        upper bound for reading points
     * @param figureIndexEnd
     *        upper bound for reading figures
     * @param segmentIndexEnd
     *        upper bound for reading segments
     * @param shapeIndexEnd
     *        upper bound for reading shapes
     * @throws SQLServerException
     *         if an exception occurs
     */
    void constructWKT(SQLServerSpatialDatatype sd, InternalSpatialDatatype isd, int pointIndexEnd, int figureIndexEnd,
            int segmentIndexEnd, int shapeIndexEnd) throws SQLServerException {
        if (numberOfPoints == 0) {
            if (isd.getTypeCode() == 11) { // FULLGLOBE
                if (sd instanceof Geometry) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_illegalTypeForGeometry"));
                    throw new SQLServerException(form.format(new Object[] {"Fullglobe"}), null, 0, null);
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
        } else if (figureIndexEnd == -1) {
            // figureIndexEnd can be -1 if a shape inside a GeometryCollection is EMPTY, handle accordingly.
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
                throwIllegalWKTPosition();
        }

        appendToWKTBuffers(")");
    }

    /**
     * Parses WKT and populates the data structures of the Geometry/Geography instance.
     * 
     * @param sd
     *        the Geometry/Geography instance.
     * @param startPos
     *        The index to start from from the WKT.
     * @param parentShapeIndex
     *        The index of the parent's Shape in the shapes array. Used to determine this shape's parent.
     * @param isGeoCollection
     *        flag to indicate if this is part of a GeometryCollection.
     * @throws SQLServerException
     *         if an exception occurs
     */
    void parseWKTForSerialization(SQLServerSpatialDatatype sd, int startPos, int parentShapeIndex,
            boolean isGeoCollection) throws SQLServerException {
        // after every iteration of this while loop, the currentWktPosition will be set to the
        // end of the geometry/geography shape, except for the very first iteration of it.
        // This means that there has to be comma (that separates the previous shape with the next shape),
        // or we expect a ')' that will close the entire shape and exit the method.

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
            } catch (Exception e) {
                throwIllegalWKTPosition();
            }
            byte fa = 0;

            if (version == 1 && ("CIRCULARSTRING".equals(nextToken) || "COMPOUNDCURVE".equals(nextToken)
                    || "CURVEPOLYGON".equals(nextToken))) {
                version = 2;
            }

            // check for FULLGLOBE before reading the first open bracket, since FULLGLOBE doesn't have one.
            if ("FULLGLOBE".equals(nextToken)) {
                if (sd instanceof Geometry) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_illegalTypeForGeometry"));
                    throw new SQLServerException(form.format(new Object[] {"Fullglobe"}), null, 0, null);
                }

                if (startPos != 0) {
                    throwIllegalWKTPosition();
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
                    if (startPos == 0 && "POINT".equals(nextToken.toUpperCase())) {
                        isSinglePoint = true;
                        internalType = InternalSpatialDatatype.POINT;
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
                    fa = isd.getTypeCode() == InternalSpatialDatatype.LINESTRING.getTypeCode() ? FA_STROKE
                                                                                               : FA_EXTERIOR_RING;
                    figureList.add(new Figure(fa, pointList.size()));

                    readLineWkt();

                    if (startPos == 0 && "LINESTRING".equals(nextToken.toUpperCase()) && pointList.size() == 2) {
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
                    throwIllegalWKTPosition();
            }
            readCloseBracket();
        }

        populateStructures();
    }

    /**
     * Constructs and appends a Point type in WKT form to the stringbuffer. There are two stringbuffers - WKTsb and
     * WKTsbNoZM. WKTsb contains the X, Y, Z and M coordinates, whereas WKTsbNoZM contains only X and Y coordinates.
     * 
     * @param pointIndex
     *        indicates which point to append to the stringbuffer.
     * 
     */
    void constructPointWKT(int pointIndex) {
        if (xValues[pointIndex] % 1 == 0) {
            appendToWKTBuffers((int) xValues[pointIndex]);
        } else {
            appendToWKTBuffers(xValues[pointIndex]);
        }
        appendToWKTBuffers(" ");

        if (yValues[pointIndex] % 1 == 0) {
            appendToWKTBuffers((int) yValues[pointIndex]);
        } else {
            appendToWKTBuffers(yValues[pointIndex]);
        }
        appendToWKTBuffers(" ");

        if (hasZvalues && !Double.isNaN(zValues[pointIndex])) {
            if (zValues[pointIndex] % 1 == 0) {
                WKTsb.append((long) zValues[pointIndex]);
            } else {
                WKTsb.append(zValues[pointIndex]);
            }
            WKTsb.append(" ");
        } else if (hasMvalues && !Double.isNaN(mValues[pointIndex])) {
            // Handle the case where the user has POINT (1 2 NULL M) value.
            WKTsb.append("NULL ");
        }

        if (hasMvalues && !Double.isNaN(mValues[pointIndex])) {
            if (mValues[pointIndex] % 1 == 0) {
                WKTsb.append((long) mValues[pointIndex]);
            } else {
                WKTsb.append(mValues[pointIndex]);
            }
            WKTsb.append(" ");
        }

        currentPointIndex++;
        // truncate last space
        WKTsb.setLength(WKTsb.length() - 1);
        WKTsbNoZM.setLength(WKTsbNoZM.length() - 1);
    }

    /**
     * Constructs a line in WKT form.
     * 
     * @param pointStartIndex
     *        .
     * @param pointEndIndex
     *        .
     */
    void constructLineWKT(int pointStartIndex, int pointEndIndex) {
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
     * @param figureStartIndex
     *        .
     * @param figureEndIndex
     *        .
     */
    void constructShapeWKT(int figureStartIndex, int figureEndIndex) {
        for (int i = figureStartIndex; i < figureEndIndex; i++) {
            appendToWKTBuffers("(");
            if (i != numberOfFigures - 1) { // not the last figure
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
     * @param shapeStartIndex
     *        .
     * @param shapeEndIndex
     *        .
     */
    void constructMultiShapeWKT(int shapeStartIndex, int shapeEndIndex) {
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
     * @param segmentStartIndex
     *        .
     * @param segmentEndIndex
     *        .
     * @param pointEndIndex
     *        .
     */
    void constructCompoundcurveWKT(int segmentStartIndex, int segmentEndIndex, int pointEndIndex) {
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
     * @param shapeStartIndex
     *        .
     * @param shapeEndIndex
     *        .
     */
    void constructMultipolygonWKT(int shapeStartIndex, int shapeEndIndex) {
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
                } else { // not the last polygon, followed by an interior ring
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
     * @param figureStartIndex
     *        .
     * @param figureEndIndex
     *        .
     * @param segmentStartIndex
     *        .
     * @param segmentEndIndex
     *        .
     */
    void constructCurvepolygonWKT(int figureStartIndex, int figureEndIndex, int segmentStartIndex,
            int segmentEndIndex) {
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

            // Append a comma if this is not the last figure of the shape.
            if (i != figureEndIndex - 1) {
                appendToWKTBuffers(", ");
            }
        }
    }

    /**
     * Constructs a Segment in WKT form. SQL Server re-uses the last point of a segment if the following segment is of
     * type 3 (first arc) or type 2 (first line). This makes sense because the last point of a segment and the first
     * point of the next segment have to match for a valid curve. This means that the code has to look ahead and decide
     * to decrement the currentPointIndex depending on what segment comes next, since it may have been reused (and it's
     * reflected in the array of points)
     * 
     * @param currentSegment
     *        .
     * @param segment
     *        .
     * @param pointEndIndex
     *        .
     */
    void constructSegmentWKT(int currentSegment, byte segment, int pointEndIndex) {
        switch (segment) {
            case 0:
                appendToWKTBuffers(", ");
                constructLineWKT(currentPointIndex, currentPointIndex + 1);

                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 0) { // not being followed by another line,
                                                                                 // but not the last segment
                    currentPointIndex = currentPointIndex - 1;
                    incrementPointNumStartIfPointNotReused(pointEndIndex);
                }
                break;

            case 1:
                appendToWKTBuffers(", ");
                constructLineWKT(currentPointIndex, currentPointIndex + 2);

                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 1) { // not being followed by another arc,
                                                                                 // but not the last segment
                    currentPointIndex = currentPointIndex - 1; // only increment pointNumStart by one less than what we
                                                               // should be, since the last
                                                               // point will be reused
                    incrementPointNumStartIfPointNotReused(pointEndIndex);
                }

                break;
            case 2:
                appendToWKTBuffers("(");
                constructLineWKT(currentPointIndex, currentPointIndex + 2);

                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 0) { // not being followed by another line,
                                                                                 // but not the last segment
                    currentPointIndex = currentPointIndex - 1; // only increment pointNumStart by one less than what we
                                                               // should be, since the last
                                                               // point will be reused
                    incrementPointNumStartIfPointNotReused(pointEndIndex);
                }

                break;
            case 3:
                appendToWKTBuffers("CIRCULARSTRING(");
                constructLineWKT(currentPointIndex, currentPointIndex + 3);

                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 1) { // not being followed by another arc
                    currentPointIndex = currentPointIndex - 1; // only increment pointNumStart by one less than what we
                                                               // should be, since the last
                                                               // point will be reused
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
     * @param shapeEndIndex
     *        .
     * @throws SQLServerException
     *         if an exception occurs
     */
    void constructGeometryCollectionWKT(int shapeEndIndex) throws SQLServerException {
        currentShapeIndex++;
        constructGeometryCollectionWKThelper(shapeEndIndex);
    }

    /**
     * Reads Point WKT and adds it to the list of points. This method will read up until and including the comma that
     * may come at the end of the Point WKT.
     * 
     * @throws SQLServerException
     *         if an exception occurs
     */
    void readPointWkt() throws SQLServerException {
        int numOfCoordinates = 0;
        double sign;
        double coords[] = new double[4];
        for (int i = 0; i < coords.length; i++) {
            coords[i] = Double.NaN;
        }

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

            while (currentWktPos < wkt.length()
                    && (Character.isDigit(wkt.charAt(currentWktPos)) || wkt.charAt(currentWktPos) == '.'
                            || wkt.charAt(currentWktPos) == 'E' || wkt.charAt(currentWktPos) == 'e')) {
                currentWktPos++;
            }

            try {
                coords[numOfCoordinates] = sign * new BigDecimal(wkt.substring(startPos, currentWktPos)).doubleValue();

                if (numOfCoordinates == 2) {
                    hasZvalues = true;
                } else if (numOfCoordinates == 3) {
                    hasMvalues = true;
                }
            } catch (Exception e) { // modify to conversion exception
                // handle NULL case
                // the first check ensures that there is enough space for the wkt to have NULL
                if (wkt.length() > currentWktPos + 3
                        && "null".equalsIgnoreCase(wkt.substring(currentWktPos, currentWktPos + 4))) {
                    coords[numOfCoordinates] = Double.NaN;
                    currentWktPos = currentWktPos + 4;
                } else {
                    throwIllegalWKTPosition();
                }
            }

            numOfCoordinates++;

            skipWhiteSpaces();

            // After skipping white space after the 4th coordinate has been read, the next
            // character has to be either a , or ), or the WKT is invalid.
            if (numOfCoordinates == 4) {
                if (checkSQLLength(currentWktPos + 1) && wkt.charAt(currentWktPos) != ','
                        && wkt.charAt(currentWktPos) != ')') {
                    throwIllegalWKTPosition();
                }
            }

            if (checkSQLLength(currentWktPos + 1) && wkt.charAt(currentWktPos) == ',') {
                // need at least 2 coordinates
                if (numOfCoordinates == 1) {
                    throwIllegalWKTPosition();
                }
                currentWktPos++;
                skipWhiteSpaces();
                break;
            }
            skipWhiteSpaces();
        }

        pointList.add(new Point(coords[0], coords[1], coords[2], coords[3]));
    }

    /**
     * Reads a series of Point types.
     * 
     * @throws SQLServerException
     *         if an exception occurs
     */
    void readLineWkt() throws SQLServerException {
        while (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
            readPointWkt();
        }
    }

    /**
     * Reads a shape (simple Geometry/Geography entities that are contained within a single bracket) WKT.
     * 
     * @param parentShapeIndex
     *        shape index of the parent shape that called this method
     * @param nextToken
     *        next string token
     * @throws SQLServerException
     *         if an exception occurs
     */
    void readShapeWkt(int parentShapeIndex, String nextToken) throws SQLServerException {
        byte fa = FA_POINT;
        while (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {

            // if next keyword is empty, continue the loop.
            // Do not check this for polygon.
            if (!"POLYGON".equals(nextToken)
                    && checkEmptyKeyword(parentShapeIndex, InternalSpatialDatatype.valueOf(nextToken), true)) {
                continue;
            }

            if ("MULTIPOINT".equals(nextToken)) {
                shapeList.add(
                        new Shape(parentShapeIndex, figureList.size(), InternalSpatialDatatype.POINT.getTypeCode()));
            } else if ("MULTILINESTRING".equals(nextToken)) {
                shapeList.add(new Shape(parentShapeIndex, figureList.size(),
                        InternalSpatialDatatype.LINESTRING.getTypeCode()));
            }

            if (version == 1) {
                if ("MULTIPOINT".equals(nextToken)) {
                    fa = FA_STROKE;
                } else if ("MULTILINESTRING".equals(nextToken) || "POLYGON".equals(nextToken)) {
                    fa = FA_EXTERIOR_RING;
                }
                version_one_shape_indexes.add(figureList.size());
            } else if (version == 2) {
                if ("MULTIPOINT".equals(nextToken) || "MULTILINESTRING".equals(nextToken) || "POLYGON".equals(nextToken)
                        || "MULTIPOLYGON".equals(nextToken)) {
                    fa = FA_LINE;
                }
            }

            figureList.add(new Figure(fa, pointList.size()));
            readOpenBracket();
            readLineWkt();
            readCloseBracket();

            skipWhiteSpaces();

            if (checkSQLLength(currentWktPos + 1) && wkt.charAt(currentWktPos) == ',') { // more rings to follow
                readComma();
            } else if (wkt.charAt(currentWktPos) == ')') { // about to exit while loop
                continue;
            } else { // unexpected input
                throwIllegalWKTPosition();
            }
        }
    }

    /**
     * Reads a CurvePolygon WKT.
     * 
     * @throws SQLServerException
     *         if an exception occurs
     */
    void readCurvePolygon() throws SQLServerException {
        while (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
            String nextPotentialToken = getNextStringToken().toUpperCase(Locale.US);
            if ("CIRCULARSTRING".equals(nextPotentialToken)) {
                figureList.add(new Figure(FA_ARC, pointList.size()));
                readOpenBracket();
                readLineWkt();
                readCloseBracket();
            } else if ("COMPOUNDCURVE".equals(nextPotentialToken)) {
                figureList.add(new Figure(FA_COMPOSITE_CURVE, pointList.size()));
                readOpenBracket();
                readCompoundCurveWkt(true);
                readCloseBracket();
            } else if (wkt.charAt(currentWktPos) == '(') { // LineString
                figureList.add(new Figure(FA_LINE, pointList.size()));
                readOpenBracket();
                readLineWkt();
                readCloseBracket();
            } else {
                throwIllegalWKTPosition();
            }

            if (checkSQLLength(currentWktPos + 1) && wkt.charAt(currentWktPos) == ',') { // more polygons to follow
                readComma();
            } else if (wkt.charAt(currentWktPos) == ')') { // about to exit while loop
                continue;
            } else { // unexpected input
                throwIllegalWKTPosition();
            }
        }
    }

    /**
     * Reads a MultiPolygon WKT.
     * 
     * @param thisShapeIndex
     *        shape index of current shape
     * @param nextToken
     *        next string token
     * @throws SQLServerException
     *         if an exception occurs
     */
    void readMultiPolygonWkt(int thisShapeIndex, String nextToken) throws SQLServerException {
        while (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
            if (checkEmptyKeyword(thisShapeIndex, InternalSpatialDatatype.valueOf(nextToken), true)) {
                continue;
            }
            shapeList.add(new Shape(thisShapeIndex, figureList.size(), InternalSpatialDatatype.POLYGON.getTypeCode())); // exterior
                                                                                                                        // polygon
            readOpenBracket();
            readShapeWkt(thisShapeIndex, nextToken);
            readCloseBracket();

            if (checkSQLLength(currentWktPos + 1) && wkt.charAt(currentWktPos) == ',') { // more polygons to follow
                readComma();
            } else if (wkt.charAt(currentWktPos) == ')') { // about to exit while loop
                continue;
            } else { // unexpected input
                throwIllegalWKTPosition();
            }
        }
    }

    /**
     * Reads a Segment WKT.
     * 
     * @param segmentType
     *        segment type
     * @param isFirstIteration
     *        flag that indicates if this is the first iteration from the loop outside
     * @throws SQLServerException
     *         if an exception occurs
     */
    void readSegmentWkt(int segmentType, boolean isFirstIteration) throws SQLServerException {
        segmentList.add(new Segment((byte) segmentType));

        int segmentLength = segmentType;

        // under 2 means 0 or 1 (possible values). 0 (line) has 1 point, and 1 (arc) has 2 points, so increment by one
        if (segmentLength < 2) {
            segmentLength++;
        }

        for (int i = 0; i < segmentLength; i++) {
            // If a segment type of 2 (first line) or 3 (first arc) is not from the very first iteration of the while
            // loop,
            // then the first point has to be a duplicate point from the previous segment, so skip the first point.
            if (i == 0 && !isFirstIteration && segmentType >= 2) {
                skipFirstPointWkt();
            } else {
                readPointWkt();
            }
        }

        if (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
            if (segmentType == SEGMENT_FIRST_ARC || segmentType == SEGMENT_ARC) {
                readSegmentWkt(SEGMENT_ARC, false);
            } else if (segmentType == SEGMENT_FIRST_LINE || segmentType == SEGMENT_LINE) {
                readSegmentWkt(SEGMENT_LINE, false);
            }
        }
    }

    /**
     * Reads a CompoundCurve WKT.
     * 
     * @param isFirstIteration
     *        flag that indicates if this is the first iteration from the loop outside
     * @throws SQLServerException
     *         if an exception occurs
     */
    void readCompoundCurveWkt(boolean isFirstIteration) throws SQLServerException {
        while (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
            String nextPotentialToken = getNextStringToken().toUpperCase(Locale.US);
            if ("CIRCULARSTRING".equals(nextPotentialToken)) {
                readOpenBracket();
                readSegmentWkt(SEGMENT_FIRST_ARC, isFirstIteration);
                readCloseBracket();
            } else if (wkt.charAt(currentWktPos) == '(') {// LineString
                readOpenBracket();
                readSegmentWkt(SEGMENT_FIRST_LINE, isFirstIteration);
                readCloseBracket();
            } else {
                throwIllegalWKTPosition();
            }

            isFirstIteration = false;

            if (checkSQLLength(currentWktPos + 1) && wkt.charAt(currentWktPos) == ',') { // more polygons to follow
                readComma();
            } else if (wkt.charAt(currentWktPos) == ')') { // about to exit while loop
                continue;
            } else { // unexpected input
                throwIllegalWKTPosition();
            }
        }
    }

    /**
     * Reads the next string token (usually POINT, LINESTRING, etc.). Then increments currentWktPos to the end of the
     * string token.
     * 
     * @return the next string token
     */
    String getNextStringToken() {
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
     * Populates the various data structures contained within the Geometry/Geography instance.
     */
    void populateStructures() {
        if (pointList.size() > 0) {
            xValues = new double[pointList.size()];
            yValues = new double[pointList.size()];

            for (int i = 0; i < pointList.size(); i++) {
                xValues[i] = pointList.get(i).getX();
                yValues[i] = pointList.get(i).getY();
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
        // the figure offset of the very first shape (GeometryCollections) has to be -1, but this is not possible to
        // know until
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

    void readOpenBracket() throws SQLServerException {
        skipWhiteSpaces();
        if (wkt.charAt(currentWktPos) == '(') {
            currentWktPos++;
            skipWhiteSpaces();
        } else {
            throwIllegalWKTPosition();
        }
    }

    void readCloseBracket() throws SQLServerException {
        skipWhiteSpaces();
        if (wkt.charAt(currentWktPos) == ')') {
            currentWktPos++;
            skipWhiteSpaces();
        } else {
            throwIllegalWKTPosition();
        }
    }

    boolean hasMoreToken() {
        skipWhiteSpaces();
        return currentWktPos < wkt.length();
    }

    void createSerializationProperties() {
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

    int determineClrCapacity(boolean excludeZMFromCLR) {
        int totalSize = 0;

        totalSize += 6; // SRID + version + SerializationPropertiesByte

        if (isSinglePoint || isSingleLineSegment) {
            totalSize += 16 * numberOfPoints;

            if (!excludeZMFromCLR) {
                if (hasZvalues) {
                    totalSize += 8 * numberOfPoints;
                }

                if (hasMvalues) {
                    totalSize += 8 * numberOfPoints;
                }
            }

            return totalSize;
        }

        int pointSize = 16;
        if (!excludeZMFromCLR) {
            if (hasZvalues) {
                pointSize += 8;
            }

            if (hasMvalues) {
                pointSize += 8;
            }
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

    int determineWkbCapacity() {
        int totalSize = 0;

        totalSize += BYTE_ORDER_SIZE; // byte order
        totalSize += INTERNAL_TYPE_SIZE; // internal type size

        switch (internalType) {
            case POINT:
                // Handle special case where POINT EMPTY
                if (numberOfPoints == 0) {
                    totalSize += NUMBER_OF_SHAPES_SIZE; // number of points
                }
                totalSize += numberOfPoints * WKB_POINT_SIZE;
                break;
            case LINESTRING:
                totalSize += NUMBER_OF_SHAPES_SIZE; // number of points
                totalSize += numberOfPoints * WKB_POINT_SIZE;
                break;
            case POLYGON:
                totalSize += NUMBER_OF_SHAPES_SIZE; // number of rings
                totalSize += figures.length * 4 + (numberOfPoints * WKB_POINT_SIZE);
                break;
            case MULTIPOINT:
                totalSize += NUMBER_OF_SHAPES_SIZE; // number of points
                totalSize += numberOfFigures * WKB_POINT_HEADER_SIZE;
                totalSize += numberOfPoints * WKB_POINT_SIZE;
                break;
            case MULTILINESTRING:
                totalSize += NUMBER_OF_SHAPES_SIZE; // number of LineStrings
                totalSize += numberOfFigures * WKB_HEADER_SIZE;
                totalSize += numberOfPoints * WKB_POINT_SIZE;
                break;
            case MULTIPOLYGON:
                totalSize += NUMBER_OF_SHAPES_SIZE; // number of polygons
                totalSize += (numberOfShapes - 1) * WKB_HEADER_SIZE;
                for (int i = 1; i < shapes.length; i++) {
                    if (i == shapes.length - 1) { // last element
                        totalSize += LINEAR_RING_HEADER_SIZE * (figures.length - shapes[i].getFigureOffset());
                    } else {
                        int nextFigureOffset = shapes[i + 1].getFigureOffset();
                        totalSize += LINEAR_RING_HEADER_SIZE * (nextFigureOffset - shapes[i].getFigureOffset());
                    }
                }
                totalSize += numberOfPoints * WKB_POINT_SIZE;
                break;
            case GEOMETRYCOLLECTION:
                totalSize += NUMBER_OF_SHAPES_SIZE; // number of shapes
                int actualNumberOfPoints = numberOfPoints;
                for (Segment s : segments) {
                    if (s.getSegmentType() == SEGMENT_FIRST_ARC || s.getSegmentType() == SEGMENT_FIRST_LINE) {
                        totalSize += WKB_HEADER_SIZE;
                        actualNumberOfPoints++;
                    }
                }
                int numberOfCompositeCurves = 0;
                for (Figure f : figures) {
                    if (f.getFiguresAttribute() == FA_COMPOSITE_CURVE) {
                        numberOfCompositeCurves++;
                    }
                }
                if (numberOfCompositeCurves > 1) {
                    actualNumberOfPoints = actualNumberOfPoints - (numberOfCompositeCurves - 1);
                }
                if (numberOfSegments > 0) {
                    actualNumberOfPoints--;
                }
                // start from 1
                for (int i = 1; i < shapes.length; i++) {
                    if (shapes[i].getOpenGISType() == InternalSpatialDatatype.POINT.getTypeCode()) {
                        /*
                         * empty points are actually considered an empty multipoint in WKB. In this case, the figure
                         * offset will be -1, so adjust accordingly.
                         */
                        if (shapes[i].getFigureOffset() == -1) {
                            totalSize += WKB_HEADER_SIZE;
                        } else {
                            totalSize += WKB_POINT_HEADER_SIZE;
                        }
                    } else if (shapes[i].getOpenGISType() == InternalSpatialDatatype.POLYGON.getTypeCode()) {
                        if (shapes[i].getFigureOffset() != -1) {
                            if (i == shapes.length - 1) { // last element
                                totalSize += LINEAR_RING_HEADER_SIZE * (figures.length - shapes[i].getFigureOffset());
                            } else {

                                /*
                                 * the logic below handles cases where we have something like:
                                 * GEOMETRYCOLLECTION(POLYGON(0 1, 0 2, 0 3), POLYGON EMPTY, POLYGON EMPTY) In this
                                 * case, in order to find out how many figures there are in the first polygon, we need
                                 * to look ahead to the next shape (POLYGON EMPTY) to find out how many figures are in
                                 * this polygon. However, if the shape is empty, the figure index is going to be -1. If
                                 * that happens, we need to keep looking ahead until we find a shape that isn't empty.
                                 * If the last shape is also empty, then we can calculate the number of figures in the
                                 * current polygon by doing (number of all the figures) - (current figure index).
                                 */

                                int figureIndexEnd = -1;
                                int localCurrentShapeIndex = i;
                                while (figureIndexEnd == -1 && localCurrentShapeIndex < shapes.length - 1) {
                                    figureIndexEnd = shapes[localCurrentShapeIndex + 1].getFigureOffset();
                                    localCurrentShapeIndex++;
                                }

                                if (figureIndexEnd == -1) {
                                    figureIndexEnd = numberOfFigures;
                                }

                                totalSize += LINEAR_RING_HEADER_SIZE * (figureIndexEnd - shapes[i].getFigureOffset());
                            }
                        }
                        totalSize += WKB_HEADER_SIZE;
                    } else if (shapes[i].getOpenGISType() == InternalSpatialDatatype.CURVEPOLYGON.getTypeCode()) {
                        if (shapes[i].getFigureOffset() != -1) {
                            if (i == shapes.length - 1) { // last element
                                totalSize += WKB_HEADER_SIZE * (figures.length - shapes[i].getFigureOffset());
                            } else {
                                int figureIndexEnd = -1;
                                int localCurrentShapeIndex = i;
                                while (figureIndexEnd == -1 && localCurrentShapeIndex < shapes.length - 1) {
                                    figureIndexEnd = shapes[localCurrentShapeIndex + 1].getFigureOffset();
                                    localCurrentShapeIndex++;
                                }

                                if (figureIndexEnd == -1) {
                                    figureIndexEnd = numberOfFigures;
                                }
                                totalSize += WKB_HEADER_SIZE * (figureIndexEnd - shapes[i].getFigureOffset());
                            }
                        }
                        totalSize += WKB_HEADER_SIZE;
                    } else {
                        totalSize += WKB_HEADER_SIZE;
                    }
                }
                totalSize += actualNumberOfPoints * WKB_POINT_SIZE;
                break;
            case CIRCULARSTRING:
                totalSize += NUMBER_OF_SHAPES_SIZE; // number of points
                totalSize += numberOfPoints * WKB_POINT_SIZE;
                break;
            case COMPOUNDCURVE:
                totalSize += NUMBER_OF_SHAPES_SIZE; // number of curves
                actualNumberOfPoints = numberOfPoints;

                /*
                 * Segments are exclusively used by COMPOUNDCURVE. However, by extension GEOMETRYCOLLECTION or
                 * CURVEPOLYGON can have them beacuse they can contain COMPOUNDCURVE inside. If a geometric shape
                 * contains any number of COMPOUNDCURVEs, we need to calculate the actual number of points involved in
                 * the geometric shape. This is because some points in the compound curve shapes are being used across
                 * two segments. For example, let's say we have a COMPOUNDCURVE(CIRCULARSTRING(1 0, 0 1, 9 6, 8 7, -1
                 * 0), CIRCULARSTRING(-1 0, 7 9, -10 2)). This compoundcurve contains a CIRCULARSTRING that spans across
                 * 5 points, and another CIRCULARSTRING that spans across 3 points. In WKB format this would be
                 * considered as 8 points. However, the existing Geometry/Geography object will have their CLR sent from
                 * SQL Server as only having 7 points. This is because compoundcurves have a rule that all the segments
                 * have to be connected to each other (if the user tried to create a compoundcurve that doesn't link
                 * from one segment to the next, it will throw an error). Therefore, in order to convert from CLR to
                 * WKB, we need to find out the "true" number of points that exist in this geometric object, and the
                 * below logic handles the calculation.
                 */

                for (Segment s : segments) {
                    if (s.getSegmentType() == SEGMENT_FIRST_ARC || s.getSegmentType() == SEGMENT_FIRST_LINE) {
                        totalSize += WKB_HEADER_SIZE;
                        actualNumberOfPoints++;
                    }
                }
                if (numberOfSegments > 0) {
                    actualNumberOfPoints--;
                }
                totalSize += actualNumberOfPoints * WKB_POINT_SIZE;
                break;
            case CURVEPOLYGON:
                totalSize += NUMBER_OF_SHAPES_SIZE; // number of shapes
                actualNumberOfPoints = numberOfPoints;
                for (Segment s : segments) {
                    if (s.getSegmentType() == SEGMENT_FIRST_ARC || s.getSegmentType() == SEGMENT_FIRST_LINE) {
                        totalSize += WKB_HEADER_SIZE;
                        actualNumberOfPoints++;
                    }
                }
                numberOfCompositeCurves = 0;

                /*
                 * Since CURVEPOLYGONs can have COMPOUNDCURVEs inside, we need to account for them as well. However,
                 * COMPOUNDCURVEs inside CURVEPOLYGONs are treated differently than when they are on their own, because
                 * each compound curve (represented by f.getFiguresAttribute() == FA_COMPOSITE_CURVE) needs to wrap
                 * around to itself, which means that for every composite curve, the actual number of pointsdecreases by
                 * one. The below logic accounts for this.
                 */

                for (Figure f : figures) {
                    totalSize += WKB_HEADER_SIZE;
                    if (f.getFiguresAttribute() == FA_COMPOSITE_CURVE) {
                        numberOfCompositeCurves++;
                    }
                }

                if (numberOfCompositeCurves > 1) {
                    actualNumberOfPoints = actualNumberOfPoints - (numberOfCompositeCurves - 1);
                }
                if (numberOfSegments > 0) {
                    actualNumberOfPoints--;
                }
                totalSize += actualNumberOfPoints * WKB_POINT_SIZE;
                break;
            case FULLGLOBE:
                totalSize = 5; // create a variable for this later?
                break;
            default:
                break;
        }

        return totalSize;
    }

    /**
     * Append the data to both stringbuffers.
     * 
     * @param o
     *        data to append to the stringbuffers.
     */
    void appendToWKTBuffers(Object o) {
        WKTsb.append(o);
        WKTsbNoZM.append(o);
    }

    void interpretSerializationPropBytes() {
        hasZvalues = (serializationProperties & hasZvaluesMask) != 0;
        hasMvalues = (serializationProperties & hasMvaluesMask) != 0;
        isValid = (serializationProperties & isValidMask) != 0;
        isSinglePoint = (serializationProperties & isSinglePointMask) != 0;
        isSingleLineSegment = (serializationProperties & isSingleLineSegmentMask) != 0;
        isLargerThanHemisphere = (serializationProperties & isLargerThanHemisphereMask) != 0;
    }

    void readNumberOfPoints() throws SQLServerException {
        if (isSinglePoint) {
            numberOfPoints = 1;
        } else if (isSingleLineSegment) {
            numberOfPoints = 2;
        } else {
            numberOfPoints = readInt();
            checkNegSize(numberOfPoints);
        }
    }

    void readZvalues() throws SQLServerException {
        zValues = new double[numberOfPoints];
        for (int i = 0; i < numberOfPoints; i++) {
            zValues[i] = readDouble();
        }
    }

    void readMvalues() throws SQLServerException {
        mValues = new double[numberOfPoints];
        for (int i = 0; i < numberOfPoints; i++) {
            mValues[i] = readDouble();
        }
    }

    void readNumberOfFigures() throws SQLServerException {
        numberOfFigures = readInt();
        checkNegSize(numberOfFigures);
    }

    void readFigures() throws SQLServerException {
        byte fa;
        int po;
        figures = new Figure[numberOfFigures];
        for (int i = 0; i < numberOfFigures; i++) {
            fa = readByte();
            po = readInt();
            figures[i] = new Figure(fa, po);
        }
    }

    void readNumberOfShapes() throws SQLServerException {
        numberOfShapes = readInt();
        checkNegSize(numberOfShapes);
    }

    void readShapes() throws SQLServerException {
        int po;
        int fo;
        byte ogt;
        shapes = new Shape[numberOfShapes];
        for (int i = 0; i < numberOfShapes; i++) {
            po = readInt();
            fo = readInt();
            ogt = readByte();
            shapes[i] = new Shape(po, fo, ogt);
        }
    }

    void readNumberOfSegments() throws SQLServerException {
        numberOfSegments = readInt();
        checkNegSize(numberOfSegments);
    }

    void readSegments() throws SQLServerException {
        byte st;
        segments = new Segment[numberOfSegments];
        for (int i = 0; i < numberOfSegments; i++) {
            st = readByte();
            segments[i] = new Segment(st);
        }
    }

    void determineInternalType() {
        if (isSinglePoint) {
            internalType = InternalSpatialDatatype.POINT;
        } else if (isSingleLineSegment) {
            internalType = InternalSpatialDatatype.LINESTRING;
        } else {
            internalType = InternalSpatialDatatype.valueOf(shapes[0].getOpenGISType());
        }
    }

    boolean checkEmptyKeyword(int parentShapeIndex, InternalSpatialDatatype isd,
            boolean isInsideAnotherShape) throws SQLServerException {
        String potentialEmptyKeyword = getNextStringToken().toUpperCase(Locale.US);
        if ("EMPTY".equals(potentialEmptyKeyword)) {

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
                    String strError = SQLServerException.getErrString("R_illegalWKT");
                    throw new SQLServerException(strError, null, 0, null);
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

        if (!"".equals(potentialEmptyKeyword)) {
            throwIllegalWKTPosition();
        }
        return false;
    }

    void throwIllegalWKT() throws SQLServerException {
        String strError = SQLServerException.getErrString("R_illegalWKT");
        throw new SQLServerException(strError, null, 0, null);
    }

    void throwIllegalByteArray() throws SQLServerException {
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ParsingError"));
        Object[] msgArgs = {JDBCType.VARBINARY};
        throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
    }

    private void incrementPointNumStartIfPointNotReused(int pointEndIndex) {
        // We need to increment PointNumStart if the last point was actually not re-used in the points array.
        // 0 for pointNumEnd indicates that this check is not applicable.
        if (currentPointIndex + 1 >= pointEndIndex) {
            currentPointIndex++;
        }
    }

    /**
     * Helper used for resursive iteration for constructing GeometryCollection in WKT form.
     * 
     * @param shapeEndIndex
     *        .
     * @throws SQLServerException
     *         if an exception occurs
     */
    private void constructGeometryCollectionWKThelper(int shapeEndIndex) throws SQLServerException {
        // phase 1: assume that there is no multi - stuff and no geometrycollection
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

            // check if the figureoffset of current shape is -1, which means it should be empty.
            if (shapes[currentShapeIndex].getFigureOffset() == -1) {
                currentShapeIndex++; // empty shapes still take up a shape space
                figureIndexEnd = -1; // this will signal constructWKT to put an EMPTY for current shape.
            } else {
                switch (isd) {
                    case POINT:
                        figureIndexIncrement++;
                        currentShapeIndex++;
                        break;
                    case LINESTRING:
                    case CIRCULARSTRING:
                        figureIndexIncrement++;
                        currentShapeIndex++;
                        if (figureIndex + 1 < figures.length) {
                            pointIndexEnd = figures[figureIndex + 1].getPointOffset();
                        } else {
                            pointIndexEnd = numberOfPoints;
                        }
                        break;
                    case POLYGON:
                    case CURVEPOLYGON:
                        figureIndexEnd = -1;
                        localCurrentShapeIndex = currentShapeIndex;
                        while (figureIndexEnd == -1 && localCurrentShapeIndex < shapes.length - 1) {
                            figureIndexEnd = shapes[localCurrentShapeIndex + 1].getFigureOffset();
                            localCurrentShapeIndex++;
                        }

                        if (figureIndexEnd == -1) {
                            figureIndexEnd = numberOfFigures;
                        }

                        currentShapeIndex++;
                        figureIndexIncrement = figureIndexEnd - currentFigureIndex;

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

                                    int increment = calculateSegmentIncrement(localCurrentSegmentIndex,
                                            pointOffsetEnd - figures[i].getPointOffset());

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
                        // Multipoint and MultiLineString can go on for multiple Shapes, but eventually
                        // the parentOffset will signal the end of the object, or it's reached the end of the
                        // shapes array.
                        // There is also no possibility that a MultiPoint or MultiLineString would branch
                        // into another parent.

                        int thisShapesParentOffset = shapes[currentShapeIndex].getParentOffset();

                        int tempShapeIndex = currentShapeIndex;

                        // Increment shapeStartIndex to account for the shape index that either Multipoint,
                        // MultiLineString
                        // or MultiPolygon takes up
                        tempShapeIndex++;
                        while (tempShapeIndex < shapes.length
                                && shapes[tempShapeIndex].getParentOffset() != thisShapesParentOffset) {
                            if (!(tempShapeIndex == shapes.length - 1) && // last iteration, don't check for
                                                                          // shapes[tempShapeIndex + 1]
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

                        while (localCurrentShapeIndex < shapes.length - 1 && shapes[localCurrentShapeIndex + 1]
                                .getParentOffset() > geometryCollectionParentIndex) {
                            localCurrentShapeIndex++;
                        }
                        // increment localCurrentShapeIndex one more time since it will be used as a shapeEndIndex
                        // parameter
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

                        int increment = calculateSegmentIncrement(currentSegmentIndex,
                                pointIndexEnd - figures[currentFigureIndex].getPointOffset());

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
     * Calculates how many segments will be used by this shape. Needed to determine when the shape that uses segments
     * (e.g. CompoundCurve) needs to stop reading in cases where the CompoundCurve is included as part of
     * GeometryCollection.
     * 
     * @param segmentStart
     *        .
     * @param pointDifference
     *        number of points that were assigned to this segment to be used.
     * @return the number of segments that will be used by this shape.
     */
    private int calculateSegmentIncrement(int segmentStart, int pointDifference) {

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

            while (currentWktPos < wkt.length()
                    && (Character.isDigit(wkt.charAt(currentWktPos)) || wkt.charAt(currentWktPos) == '.'
                            || wkt.charAt(currentWktPos) == 'E' || wkt.charAt(currentWktPos) == 'e')) {
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

    private void readComma() throws SQLServerException {
        skipWhiteSpaces();
        if (wkt.charAt(currentWktPos) == ',') {
            currentWktPos++;
            skipWhiteSpaces();
        } else {
            throwIllegalWKTPosition();
        }
    }

    private void skipWhiteSpaces() {
        while (currentWktPos < wkt.length() && Character.isWhitespace(wkt.charAt(currentWktPos))) {
            currentWktPos++;
        }
    }

    private void checkNegSize(int num) throws SQLServerException {
        if (num < 0) {
            throwIllegalByteArray();
        }
    }

    private void readPoints(SQLServerSpatialDatatype type) throws SQLServerException {
        xValues = new double[numberOfPoints];
        yValues = new double[numberOfPoints];

        if (type instanceof Geometry) {
            for (int i = 0; i < numberOfPoints; i++) {
                xValues[i] = readDouble();
                yValues[i] = readDouble();
            }
        } else { // Geography
            for (int i = 0; i < numberOfPoints; i++) {
                yValues[i] = readDouble();
                xValues[i] = readDouble();
            }
        }
    }

    private void checkBuffer(int i) throws SQLServerException {
        if (buffer.remaining() < i) {
            throwIllegalByteArray();
        }
    }

    private boolean checkSQLLength(int length) throws SQLServerException {
        if (null == wkt || wkt.length() < length) {
            throwIllegalWKTPosition();
        }
        return true;
    }

    private void throwIllegalWKTPosition() throws SQLServerException {
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_illegalWKTposition"));
        throw new SQLServerException(form.format(new Object[] {currentWktPos}), null, 0, null);
    }

    byte readByte() throws SQLServerException {
        checkBuffer(1);
        return buffer.get();
    }

    int readInt() throws SQLServerException {
        checkBuffer(4);
        return buffer.getInt();
    }

    double readDouble() throws SQLServerException {
        checkBuffer(8);
        return buffer.getDouble();
    }

    // Allow retrieval of internal structures
    /**
     * Get point list
     * 
     * @return point list
     */
    public List<Point> getPointList() {
        return pointList;
    }

    /**
     * Get figure list
     * 
     * @return figure list
     */
    public List<Figure> getFigureList() {
        return figureList;
    }

    /**
     * Get shape list
     * 
     * @return shape list
     */
    public List<Shape> getShapeList() {
        return shapeList;
    }

    /**
     * Get segment list
     * 
     * @return segment list
     */
    public List<Segment> getSegmentList() {
        return segmentList;
    }
}
