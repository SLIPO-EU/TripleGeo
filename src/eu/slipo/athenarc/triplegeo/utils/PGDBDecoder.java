/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2012, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package eu.slipo.athenarc.triplegeo.utils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
//import java.util.logging.Level;
//import java.util.logging.Logger;

import org.geotools.data.shapefile.shp.MultiLineHandler;
import org.geotools.data.shapefile.shp.MultiPointHandler;
import org.geotools.data.shapefile.shp.PolygonHandler;
import org.geotools.data.shapefile.shp.ShapeType;
import org.geotools.factory.GeoTools;
import org.geotools.geometry.jts.JTSFactoryFinder;
//import org.geotools.util.logging.Logging;

import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * Geometry Decoder for ESRI Personal Geodatabase
 * 
 * @author MapPlus, mapplus@gmail.com, http://onspatial.com
 * @since 2012-10-30
 * Modified by: Kostas Patroumpas, 28/2/2018; removed logging for efficiency
 */
public class PGDBDecoder {
//    protected static final Logger LOGGER = Logging.getLogger(PGDBDecoder.class);

    static GeometryFactory gf = JTSFactoryFinder.getGeometryFactory(GeoTools.getDefaultHints());

    static final int SHPT_NULL = 0;

    static final int SHPT_POINT = 1;

    static final int SHPT_POINTM = 21;

    static final int SHPT_POINTZM = 11;

    static final int SHPT_POINTZ = 9;

    static final int SHPT_MULTIPOINT = 8;

    static final int SHPT_MULTIPOINTM = 28;

    static final int SHPT_MULTIPOINTZM = 18;

    static final int SHPT_MULTIPOINTZ = 20;

    static final int SHPT_ARC = 3;

    static final int SHPT_ARCM = 23;

    static final int SHPT_ARCZM = 13;

    static final int SHPT_ARCZ = 10;

    static final int SHPT_POLYGON = 5;

    static final int SHPT_POLYGONM = 25;

    static final int SHPT_POLYGONZM = 15;

    static final int SHPT_POLYGONZ = 19;

    static final int SHPT_MULTIPATCHM = 31;

    static final int SHPT_MULTIPATCH = 32;

    static final int SHPT_GENERALPOLYLINE = 50;

    static final int SHPT_GENERALPOLYGON = 51;

    static final int SHPT_GENERALPOINT = 52;

    static final int SHPT_GENERALMULTIPOINT = 53;

    static final int SHPT_GENERALMULTIPATCH = 54;

    public static PGDBDecoder newInstance() {
        return new PGDBDecoder();
    }

    public int getGeneralShapeType(int shape) {
        switch (shape) {
        case SHPT_GENERALPOLYLINE:
            shape = SHPT_ARC;
            break;
        case SHPT_GENERALPOLYGON:
            shape = SHPT_POLYGON;
            break;
        case SHPT_GENERALPOINT:
            shape = SHPT_POINT;
            break;
        case SHPT_GENERALMULTIPOINT:
            shape = SHPT_MULTIPOINT;
            break;
        case SHPT_GENERALMULTIPATCH:
            shape = SHPT_MULTIPATCH;
        }
        return shape;
    }

    public boolean isPoint(int shape) {
        if (shape == SHPT_POINT || shape == SHPT_POINTM || shape == SHPT_POINTZ
                || shape == SHPT_POINTZM) {
            return true;
        }
        return false;
    }

    public boolean isMultiPoint(int shape) {
        if (shape == SHPT_MULTIPOINT || shape == SHPT_MULTIPOINTM || shape == SHPT_MULTIPOINTZ
                || shape == SHPT_MULTIPOINTZM) {
            return true;
        }
        return false;
    }

    public boolean isLineString(int shape) {
        if (shape == SHPT_ARC || shape == SHPT_ARCZ || shape == SHPT_ARCM || shape == SHPT_ARCZM) {
            return true;
        }
        return false;
    }

    public boolean isPolygon(int shape) {
        if (shape == SHPT_POLYGON || shape == SHPT_POLYGONZ || shape == SHPT_POLYGONM
                || shape == SHPT_POLYGONZM || shape == SHPT_MULTIPATCH || shape == SHPT_MULTIPATCHM) {
            return true;
        }
        return false;
    }

    public Geometry decodeGeometry(byte[] bytes) {
        int size = bytes.length;
        if (size < 4) {
//            LOGGER.log(Level.WARNING, "Shape buffer size (%d) too small");
            return null;
        }

        /* -------------------------------------------------------------------- */
        /* Detect zlib compressed shapes and uncompress buffer if necessary */
        /* NOTE: this seems to be an undocumented feature, even in the */
        /* extended_shapefile_format.pdf found in the FileGDB API documentation */
        /* -------------------------------------------------------------------- */
        if (size >= 14 && bytes[12] == 0x78 && bytes[13] == 0xDA /* zlib marker */) {
//            LOGGER.log(Level.WARNING, "zlib compressed shapes");
            // TODO: ============================================
            return null;
        }

        int shape = bytes[0];
        if (0 == shape) {
//            LOGGER.log(Level.WARNING, "null shape");
            return null;
        }

        /* -------------------------------------------------------------------- */
        /* shape record is all little endian */
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        /* read shape type (not needed) */
        buffer.position(buffer.position() + 4);
        /* -------------------------------------------------------------------- */

        shape = getGeneralShapeType(shape);
        if (isPoint(shape)) {
            boolean hasZ = (shape == SHPT_POINTZ || shape == SHPT_POINTZM);
            if (size < 4 + 8 + 8 + ((hasZ) ? 8 : 0)) {
//                LOGGER.log(Level.WARNING, "Corrupted Shape");
                return null;
            }

            return readPoint(buffer, hasZ);
        } else if (isMultiPoint(shape)) {
            boolean hasZ = (shape == SHPT_MULTIPOINTZ || shape == SHPT_MULTIPOINTZM);
            if (size < 4 + 8 + 8 + ((hasZ) ? 8 : 0)) {
//                LOGGER.log(Level.WARNING, "Corrupted Shape");
                return null;
            }

            return readMultiPoint(buffer, hasZ);
        } else if (isLineString(shape)) {
            if (size < 44) {
//                LOGGER.log(Level.WARNING, "Corrupted Shape");
                return null;
            }

            boolean hasZ = (shape == SHPT_ARCZ || shape == SHPT_ARCZM || shape == SHPT_MULTIPATCH || shape == SHPT_MULTIPATCHM);
            boolean isMultiPatch = (shape == SHPT_MULTIPATCH || shape == SHPT_MULTIPATCHM);
            return readMultiLineString(buffer, hasZ, isMultiPatch);
        } else if (isPolygon(shape)) {
            if (size < 44) {
//                LOGGER.log(Level.WARNING, "Corrupted Shape");
                return null;
            }

            boolean hasZ = (shape == SHPT_POLYGONZ || shape == SHPT_POLYGONZM
                    || shape == SHPT_MULTIPATCH || shape == SHPT_MULTIPATCHM);
            boolean isMultiPatch = (shape == SHPT_MULTIPATCH || shape == SHPT_MULTIPATCHM);
            return readPolygon(buffer, hasZ, isMultiPatch);
        }

        return null;
    }

    private Geometry readPolygon(ByteBuffer buffer, boolean hasZ, boolean isMultiPatch) {
//        boolean flatFeature = false;
        ShapeType shapeType = hasZ ? ShapeType.POLYGONZ : ShapeType.POLYGON;
        PolygonHandler handler = new PolygonHandler(gf);
        return (Geometry) handler.read(buffer, shapeType, true);
    }

    private Geometry readMultiLineString(ByteBuffer buffer, boolean hasZ, boolean isMultiPatch) {
//        boolean flatFeature = false;
        ShapeType shapeType = hasZ ? ShapeType.ARCZ : ShapeType.ARC;
        MultiLineHandler handler = new MultiLineHandler(gf);
        return (Geometry) handler.read(buffer, shapeType, true);
    }

    private Geometry readMultiPoint(ByteBuffer buffer, boolean hasZ) {
//        boolean flatFeature = false;
        ShapeType shapeType = hasZ ? ShapeType.MULTIPOINTZ : ShapeType.MULTIPOINT;
        MultiPointHandler handler = new MultiPointHandler(gf);
        return (Geometry) handler.read(buffer, shapeType, true);
    }

    private Geometry readPoint(ByteBuffer buffer, boolean hasZ) {
        final int dimension = hasZ ? 3 : 2;

        CoordinateSequence cs = gf.getCoordinateSequenceFactory().create(1, dimension);
        cs.setOrdinate(0, 0, buffer.getDouble());
        cs.setOrdinate(0, 1, buffer.getDouble());

        if (dimension > 2) {
            cs.setOrdinate(0, 2, buffer.getDouble());
        }

        return gf.createPoint(cs);
    }

}
