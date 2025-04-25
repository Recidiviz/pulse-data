# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""An enum designating allowed types for locations that a justice impacted individual or
state staff member may be associated with at a given point in time.
"""
from enum import Enum


class StateLocationType(Enum):
    """An enum designating allowed types for locations that a justice impacted
    individual or state staff member may be associated with at a given point in time.
    """

    # Incarceration facilities
    COUNTY_JAIL = "COUNTY_JAIL"
    STATE_PRISON = "STATE_PRISON"
    FEDERAL_PRISON = "FEDERAL_PRISON"

    # Incarceration facilities where parole violators are incarcerated. Some states make
    # the distinction between parole violator facilities and other jails / prisons (e.g.
    # PA's Parole Violator Centers) and this type should be used in those instances.
    PAROLE_VIOLATOR_FACILITY = "PAROLE_VIOLATOR_FACILITY"

    # Any sort of residential program where a person may live for an extended period of
    # time.
    RESIDENTIAL_PROGRAM = "RESIDENTIAL_PROGRAM"

    # Used for any location where supervision is administered (e.g. a supervision unit
    # or office).
    SUPERVISION_LOCATION = "SUPERVISION_LOCATION"

    # A hospital or other medical facility where someone might be located for an
    # extended period of time.
    MEDICAL_FACILITY = "MEDICAL_FACILITY"

    # Any court or location associated with the judicial branch.
    COURT = "COURT"

    # A location associated with any law enforcement organization (e.g. police,
    # sheriffs, etc)
    LAW_ENFORCEMENT = "LAW_ENFORCEMENT"

    # Any location / office that has purely administrative purposes. May be associated
    # with higher-level state staff (e.g. directors).
    ADMINISTRATIVE = "ADMINISTRATIVE"

    # Locations that are literally a city or a county
    CITY_COUNTY = "CITY_COUNTY"

    # Any location that is not in state. This may be another state or country. Often
    # in the context of out-of-state supervision.
    OUT_OF_STATE = "OUT_OF_STATE"

    # Used when states have location codes designating that a person is not at any
    # particular location but are instead in transit.
    IN_TRANSIT = "IN_TRANSIT"

    # Used when we do not not the location type.
    INTERNAL_UNKNOWN = "INTERNAL_UNKNOWN"
