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


def hearings_dedup_cte() -> str:
    """Helper method that returns a CTE getting
    a single hearing for each person-date in MO.

    TODO(#18850): Create a preprocessed view to replace this fragment
    """

    return """hearings_dedup_type AS (
        SELECT DISTINCT
            p.state_code, 
            p.person_id, 
            FIRST_VALUE(h.JU_CSQ) OVER same_hearing_dates AS hearing_id,
            -- JU_BA: Classification Hearing Date
            SAFE.PARSE_DATE("%Y%m%d", h.JU_BA) AS hearing_date,
            -- JU_AY: Classification Hearing Next Review Date ("0" is null, handled by SAFE.PARSE_DATE)
            SAFE.PARSE_DATE("%Y%m%d", FIRST_VALUE(h.JU_AY) OVER same_hearing_dates) AS next_review_date,
            h.JU_HRT AS hearing_type,
            h.JU_PLN AS hearing_facility,
        FROM `{project_id}.{us_mo_raw_data_up_to_date_dataset}.LBAKRDTA_TAK293_latest` h
        LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` p
        ON
            h.JU_DOC = p.external_id
            AND p.state_code = "US_MO"
        WHERE
            -- Subset to Classification Hearings
            h.JU_FOR = "CLA"
        -- Dedup hearings on same day of same type
        WINDOW same_hearing_dates AS (
            PARTITION BY p.person_id, h.JU_BA, h.JU_HRT
            ORDER BY CAST(h.JU_SEQ AS INT64) DESC
        )
    )
    ,
    -- Dedup hearings on the same date. NOTE: Multiple hearings of different types on the same date are likely valid and
    -- intended in some cases, and without this dedup would result in ~150 cases of overlapping spans. This 
    -- deduplication bypasses the issue of multiple "threads" of hearings for the same person by assuming some hearing
    -- types have priority over others, which will have their "next review date" ignored.
    -- TODO(#18536): Verify this approach with MODOC staff
    hearings AS (
        SELECT DISTINCT
            state_code,
            person_id,
            hearing_date,
            FIRST_VALUE(hearing_id) OVER w as hearing_id,
            FIRST_VALUE(next_review_date) OVER w AS next_review_date,
            FIRST_VALUE(hearing_type) OVER w AS hearing_type,
            FIRST_VALUE(hearing_facility) OVER w AS hearing_facility,
        FROM hearings_dedup_type
        
        -- TODO(#18774) Determine priority order for hearing types
        WINDOW w AS (
            PARTITION by person_id, state_code, hearing_date
            ORDER BY CASE hearing_type
                WHEN "ADS30" THEN 1
                WHEN "ADS60" THEN 2
                WHEN "ADS90" THEN 3
                WHEN "TASC" THEN 4
                WHEN "INITIA" THEN 5
                WHEN "PC" THEN 6
                WHEN "OTHER" THEN 7
                ELSE 8 END
        )
    )"""


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
                    ELSE 8 END
        )
    )"""
