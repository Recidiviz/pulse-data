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
    """Dimension enumeration for Pathways metrics"""

    ADMISSION_REASON = "admission_reason"
    AGE_GROUP = "age_group"
    FACILITY = "facility"
    GENDER = "gender"
    SEX = "sex"
    JUDICIAL_DISTRICT = "judicial_district"
    LEGAL_STATUS = "legal_status"
    LENGTH_OF_STAY = "length_of_stay"
    PRIOR_LENGTH_OF_INCARCERATION = "prior_length_of_incarceration"
    RACE = "race"
    ETHNICITY = "ethnicity"
    SUPERVISING_OFFICER = "supervising_officer"
    SUPERVISION_DISTRICT = "supervision_district"
    # TODO(#13552): Remove this once FE uses supervision_district
    DISTRICT = "district"
    SUPERVISION_LEVEL = "supervision_level"
    SUPERVISION_START_DATE = "supervision_start_date"
    SUPERVISION_TYPE = "supervision_type"
    TIME_PERIOD = "time_period"
    YEAR_MONTH = "year_month"
    # Impact Dashboard Dimensions
    START_DATE = "start_date"
    END_DATE = "end_date"
    AVG_DAILY_POPULATION = "avg_daily_population"
    AVG_POPULATION_LIMITED_SUPERVISION_LEVEL = (
        "avg_population_limited_supervision_level"
    )
    MONTHS_SINCE_TREATMENT = "months_since_treatment"
    SENTENCE_LENGTH_MIN = "sentence_length_min"
    SENTENCE_LENGTH_MAX = "sentence_length_max"
