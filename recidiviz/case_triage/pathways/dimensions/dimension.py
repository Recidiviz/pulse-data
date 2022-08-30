# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Dimension enumeration for Pathways metrics"""

import enum


class Dimension(enum.Enum):
    ADMISSION_REASON = "admission_reason"
    AGE_GROUP = "age_group"
    FACILITY = "facility"
    GENDER = "gender"
    JUDICIAL_DISTRICT = "judicial_district"
    LEGAL_STATUS = "legal_status"
    LENGTH_OF_STAY = "length_of_stay"
    PRIOR_LENGTH_OF_INCARCERATION = "prior_length_of_incarceration"
    RACE = "race"
    SUPERVISING_OFFICER = "supervising_officer"
    SUPERVISION_DISTRICT = "supervision_district"
    # TODO(#13552): Remove this once FE uses supervision_district
    DISTRICT = "district"
    SUPERVISION_LEVEL = "supervision_level"
    SUPERVISION_START_DATE = "supervision_start_date"
    SUPERVISION_TYPE = "supervision_type"
    TIME_PERIOD = "time_period"
    YEAR_MONTH = "year_month"
