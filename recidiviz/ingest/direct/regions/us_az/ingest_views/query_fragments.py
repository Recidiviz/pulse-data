# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Shared helper fragments for the US_AZ ingest view queries."""

# Query fragments that rely only on raw data tables

ADC_NUMBER_TO_PERSON_ID_CTE = """
-- This CTE returns one row per individual containing their ADC Number and Person ID. 
adc_number_to_person_id_cte AS (
    SELECT DISTINCT
        PERSON_ID,
        ADC_NUMBER
    FROM {PERSON}
)"""
