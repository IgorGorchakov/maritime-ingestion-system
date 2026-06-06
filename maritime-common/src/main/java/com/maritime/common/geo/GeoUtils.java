package com.maritime.common.geo;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.util.List;

public class GeoUtils {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

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