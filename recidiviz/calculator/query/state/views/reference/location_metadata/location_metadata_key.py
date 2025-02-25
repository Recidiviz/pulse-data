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
"""Enum which enumerates all keys allowed in the location_metadata JSON column in the 
location_metadata view.
"""

from enum import Enum


class LocationMetadataKey(Enum):
    """Enumerates all keys allowed in the location_metadata JSON column in the
    location_metadata view. See documentation in the location_metadata view for
    descriptions of each key.
    """

    COUNTY_ID = "county_id"
    COUNTY_NAME = "county_name"
    IS_ACTIVE_LOCATION = "is_active_location"
    LOCATION_SUBTYPE = "location_subtype"
    LOCATION_ACRONYM = "location_acronym"
    FACILITY_GROUP_EXTERNAL_ID = "facility_group_external_id"
    FACILITY_GROUP_NAME = "facility_group_name"
    FACILITY_SECURITY_LEVEL = "facility_security_level"
    SUPERVISION_UNIT_ID = "supervision_unit_id"
    SUPERVISION_UNIT_NAME = "supervision_unit_name"
    SUPERVISION_OFFICE_ID = "supervision_office_id"
    SUPERVISION_OFFICE_NAME = "supervision_office_name"
    SUPERVISION_DISTRICT_ID = "supervision_district_id"
    SUPERVISION_DISTRICT_NAME = "supervision_district_name"
    SUPERVISION_REGION_ID = "supervision_region_id"
    SUPERVISION_REGION_NAME = "supervision_region_name"
