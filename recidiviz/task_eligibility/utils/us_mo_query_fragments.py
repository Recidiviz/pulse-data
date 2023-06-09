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
"""
Helper SQL queries for Missouri
"""


def current_bed_stay_cte() -> str:
    """Helper method that returns a CTE getting
    a single ongoing bed stay for each person incarcerated in MO.
    Ideally this should show the person's de facto unit and cell.
    """

    return """current_bed_stay AS (
        SELECT DISTINCT
            person_id,
            state_code,
            FIRST_VALUE(bed_number) OVER w as bed_number,
            FIRST_VALUE(room_number) OVER w as room_number,
            FIRST_VALUE(complex_number) OVER w as complex_number,
            FIRST_VALUE(building_number) OVER w as building_number,
            FIRST_VALUE(confinement_type_raw_text) OVER w as housing_use_code,
        FROM `{project_id}.{sessions_dataset}.us_mo_housing_stays_preprocessed`
        WHERE end_date_exclusive IS NULL
        -- TODO(#18852): Refine this confinement_type_raw_text dedup logic
        WINDOW w AS (
            PARTITION BY person_id, state_code
            ORDER BY
                CASE confinement_type_raw_text
                    WHEN "HOS" THEN 1
                    WHEN "ADS" THEN 2
                    WHEN "TAS" THEN 3
                    WHEN "DIS" THEN 4
                    WHEN "NOC" THEN 5
                    WHEN "PRC" THEN 6
                    WHEN "GNP" THEN 7
                    ELSE 8 END,
                CASE stay_type
                    WHEN "TEMPORARY-TASC" THEN 1
                    WHEN "TEMPORARY-OTHER" THEN 2
                    WHEN "PERMANENT" THEN 3
                    ELSE 4 END,
                start_date DESC
        )
    )"""
