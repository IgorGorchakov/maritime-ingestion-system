package com.maritime.common.geo;

import com.uber.h3core.H3Core;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.io.IOException;
import java.util.List;

public class GeoUtils {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private static final H3Core H3;
    static {
        try { H3 = H3Core.newInstance(); }
        catch (IOException e) { throw new ExceptionInInitializerError(e); }
    }

    public static final int HEX_RESOLUTION = 7;

    /** Returns the H3 resolution-7 cell address containing this position. */
    public static String latLonToH3Cell(double lat, double lon) {
        return H3.latLngToCellAddress(lat, lon, HEX_RESOLUTION);
    }

    /** Returns {lat, lon} of the centroid of the given H3 cell address. */
    public static double[] h3CellCentroid(String cellAddress) {
        com.uber.h3core.util.LatLng c = H3.cellToLatLng(cellAddress);
        return new double[]{c.lat, c.lng};
    }

    /**
     * Checks if a point (lat, lon) is inside a polygon defined by a list of coordinates.
     * Note: JTS uses (x, y) which corresponds to (lon, lat).
     */
    public static boolean isPointInPolygon(double lat, double lon, List<double[]> polygonCoords) {
        if (polygonCoords == null || polygonCoords.isEmpty()) {
            return false;
        }

        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(lon, lat));
        Polygon polygon = createPolygonFromCoords(polygonCoords);

        return polygon.contains(point);
    }

    /**
     * Calculates the distance between two points in meters using the Haversine formula.
     */
    public static double calculateDistanceInMeters(double lat1, double lon1, double lat2, double lon2) {
        final double R = 6_371_000; // Earth radius in meters
        double lat1Rad = Math.toRadians(lat1);
        double lat2Rad = Math.toRadians(lat2);
        double deltaLat = Math.toRadians(lat2 - lat1);
        double deltaLon = Math.toRadians(lon2 - lon1);

        double a = Math.sin(deltaLat / 2) * Math.sin(deltaLat / 2)
                + Math.cos(lat1Rad) * Math.cos(lat2Rad)
                * Math.sin(deltaLon / 2) * Math.sin(deltaLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return R * c;
    }

    private static Polygon createPolygonFromCoords(List<double[]> coords) {
        Coordinate[] coordinates = coords.stream()
                .map(c -> new Coordinate(c[0], c[1]))
                .toArray(Coordinate[]::new);
        return GEOMETRY_FACTORY.createPolygon(coordinates);
    }
}