# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""
Geospatial utility functions for resource search.
"""
from geoalchemy2 import WKBElement, WKTElement
from shapely import Point, wkb  # type: ignore


def location_wkb_to_point(location: WKBElement) -> Point:
    """Convert a location to a Point"""
    return wkb.loads(location.desc)


def coordinates_to_point(lat: float, lon: float) -> Point:
    """Convert coordinates to a Point"""
    return Point(lat, lon)


def point_to_location_wkt(point: Point) -> WKTElement:
    """Convert a Point to a location"""
    return WKTElement(point.wkt)


def point_to_location_wkb(point: Point) -> WKBElement:
    """Convert a Point to a location"""
    return WKBElement(point.wkb)


def coordinates_to_location_wkt(lat: float, lon: float) -> WKTElement:
    """Convert coordinates to a location"""
    return point_to_location_wkt(coordinates_to_point(lat, lon))


def coordinates_to_location_wkb(lat: float, lon: float) -> WKBElement:
    """Convert coordinates to a location"""
    return point_to_location_wkb(coordinates_to_point(lat, lon))
