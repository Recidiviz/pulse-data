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
"""Create constants and helper SQL query fragments for Missouri."""

# The following charge codes can be used to identify first-degree-assault offenses in
# Missouri and were identified by looking at offenses in `LBAKRCOD_TAK112` in NCIC
# category (`BE_NC2`) 13 (assault) and reviewing their respective
# descriptions.
# The codes come from the field `BE_CD1` in that view, which is a five-digit field with
# two digits that correspond with the NCIC major category followed by three digits that
# are the "unique identifier" for the charge in MO.
MO_CHARGE_CODES_ASSAULT_FIRST_DEGREE = [
    # These codes are for offenses corresponding with RSMo 565.050, which covers first-
    # degree assault in general.
    "13020",
    "13ACJ",
    "13011",
    "13010",
    "13ACH",
    # These codes are for offenses corresponding with RSMo 565.081 (repealed by S.B.
    # 491, 2014), which covered first-degree assault of emergency personnel, public-
    # safety officials, highway workers, etc. (although that statute evolved over time).
    "13105",
    "13100",
]

# The following charge codes can be used to identify first-degree-murder offenses in
# Missouri and were identified by looking at offenses in `LBAKRCOD_TAK112` in NCIC
# category (`BE_NC2`) 09 (homicide).
MO_CHARGE_CODES_MURDER_FIRST_DEGREE = [
    # These codes correspond with now-repealed/-modified statutes that, when they
    # existed, defined first-degree murder.
    "10022",
    "10020",
    # These codes are for offenses covered by RSMo 565.020, which (as of June 2025) now
    # defines first-degree murder.
    "09AAG",
    "10021",
]

# The following charge codes can be used to identify second-degree-murder offenses in
# Missouri and were identified by looking at offenses in `LBAKRCOD_TAK112` in NCIC
# category (`BE_NC2`) 09 (homicide).
MO_CHARGE_CODES_MURDER_SECOND_DEGREE = [
    # This code corresponds with a now-repealed/-modified statute that, when it existed,
    # defined second-degree murder.
    "10030",
    # These codes are for offenses covered by RSMo 565.021, which (as of June 2025) now
    # defines second-degree murder.
    "09AAH",
    "09AAI",
    "09AAJ",
    "10031",
    "10034",
    "10035",
    "10036",
]

# The following charge codes can be used to identify first degree arson offenses in
# Missouri and were identified by looking at offenses in `LBAKRCOD_TAK112` in NCIC
# category (`BE_NC2`) 20 (arson) and (`BE_CLA`) A or B (first degree arson can be class A or B felonies).
MO_CHARGE_CODES_ARSON_FIRST_DEGREE = [
    "20AAM",
    # Not sure why there are some NCI categories of 17, since these
    # were surfaced by ensuring BC_NC2 = 20
    "17012",
    "17010",
    "20AAK",
    "17015",
    "20AAL",
]

# The following charge codes can be used to identify first degree arson offenses in
# Missouri and were identified by looking at offenses in `LBAKRCOD_TAK112` in NCIC
# category (`BE_NC2`) 12 (robbery) and (`BE_CLA`) A (first degree robbery is a class A felony).
MO_CHARGE_CODES_ROBBERY_FIRST_DEGREE = [
    "12030",
    "12AAE",
    "12AAA",
    "12AAC",
    "12035",
    "12010",
    # These have `BE_CLA` = U (Unclassified)
    "12011",
    "12012",
]


def classes_cte() -> str:
    """Helper method that returns a CTE getting
    information on classes in MO.

    TODO(#21441): Deprecate once ingested
    """

    return """classes AS (
        SELECT
            pei.person_id,
            se.OFNDR_CYCLE_REF_ID,
            se.EXIT_TYPE_CD,
            -- The 'is_referral' flag is used to indicate whether or not a class entry
            -- represents a pending referral. This is currently being used in MOSOP-related
            -- logic to prevent people from being marked as completed if they have a pending referral.
            se.ENROLLMENT_STATUS_CD = 'PND' AS is_referral,
            DOC_ID,
            CYCLE_NO,
            ACTUAL_START_DT,
            ACTUAL_EXIT_DT,
            classes.CLASS_TITLE,
            exit.CLASS_EXIT_REASON_DESC
        FROM `{project_id}.us_mo_raw_data_up_to_date_views.OFNDR_PDB_CLASS_SCHEDULE_ENROLLMENTS_latest` se
        LEFT JOIN `{project_id}.us_mo_raw_data_up_to_date_views.OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF_latest` xref
        USING (OFNDR_CYCLE_REF_ID)
        LEFT JOIN `{project_id}.us_mo_raw_data_up_to_date_views.MASTER_PDB_CLASSES_latest` classes
        USING (CLASS_REF_ID)
        LEFT JOIN `{project_id}.us_mo_raw_data_up_to_date_views.CODE_PDB_CLASS_EXIT_REASON_CODES_latest` exit
        USING (CLASS_EXIT_REASON_CD)
        LEFT JOIN `{project_id}.us_mo_normalized_state.state_person_external_id` pei
        ON
            DOC_ID = pei.external_id
            AND id_type = 'US_MO_DOC'
        WHERE 
            se.DELETE_IND = "N" 
            AND xref.DELETE_IND = "N" 
            AND classes.DELETE_IND = "N"
    )"""


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

    # TODO(#45647): Once housing units are ingested in MO, can we use ingested data here
    # instead of the `us_mo_housing_stays_preprocessed` view? If we still need to pull
    # data that can't be ingested right now (e.g., bed number, room number), can we at
    # least use ingested data as much as possible so that we can ensure that what we're
    # pulling here draws upon / mimics the logic used in ingest?

    return """current_bed_stay AS (
        SELECT DISTINCT
            person_id,
            state_code,
            FIRST_VALUE(bed_number) OVER w as bed_number,
            FIRST_VALUE(room_number) OVER w as room_number,
            FIRST_VALUE(complex_number) OVER w as complex_number,
            FIRST_VALUE(building_number) OVER w as building_number,
            FIRST_VALUE(confinement_type_raw_text) OVER w as housing_use_code,
            FIRST_VALUE(facility_code) OVER w as facility,
        FROM `{project_id}.sessions.us_mo_housing_stays_preprocessed`
        WHERE end_date_exclusive IS NULL
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


def latest_d1_sanction_spans_cte() -> str:
    return """d1_sanctions_dedup AS (
            -- Multiple D1 sanctions incurred on the same day may be distinct, but we only
            -- need to count them once in determining when someone's last sanction began.
            SELECT DISTINCT
                state_code,
                person_id,
                date_effective
            FROM `{project_id}.normalized_state.state_incarceration_incident_outcome`
            WHERE outcome_type_raw_text = 'D1'
        )
        ,
        latest_d1_sanction_spans AS (
            SELECT
                state_code,
                person_id,
                -- When someone begins an active sanction, their "latest sanction start date" 
                -- will be that sanction's start date until the next time they incur a sanction.
                date_effective AS start_date,
                LEAD(date_effective) OVER w AS end_date,
                date_effective AS latest_d1_sanction_start_date,
            FROM d1_sanctions_dedup
            WINDOW w AS (
                PARTITION BY state_code, person_id
                ORDER BY date_effective ASC
            )
        )
    """
