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

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)

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


def us_mo_close_enough_to_earliest_established_release_date_criterion_builder(
    *,
    criteria_name: str,
    description: str,
    date_leading_interval: int,
    date_part: str,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a state-specific criterion view builder identifying when someone in MO is
    within some minimum period of time (e.g., 60 months) of their earliest established
    release date from incarceration.

    Args:
        criteria_name (str): The name of the criterion view.
        description (str): A brief description of the criterion view.
        date_leading_interval (int): Number of <date_part> representing the amount of
            time in advance of the earliest established release date that a person will
            become eligible.
        date_part (str): Supports any of the BigQuery date_part values:
            "DAY", "WEEK", "MONTH", "QUARTER", "YEAR". Defaults to "MONTH".
    Returns:
        StateSpecificTaskCriteriaBigQueryViewBuilder: View builder for spans of time
            when someone sufficiently close to their earliest established release date.
    """

    # TODO(#46222): Can we make this historically accurate and not just a snapshot of
    # current eligibility?
    criterion_query = f"""
        WITH most_recent_cycle AS (
            /* TODO(#46134): Should we set up a pre-processed view for cycle data so
            we're not having to pull directly from the raw data here in addition to in
            several other places? */
            /* Pull most recent cycle for each person in MO. Note that this will not
            necessarily be an active cycle for all people in MO data, but for residents
            currently incarcerated, the most recent cycle should be the current one. */
            SELECT
                pei.state_code,
                pei.person_id,
                tak040.DQ_CYC AS cycle_number,
                SAFE.PARSE_DATE('%Y%m%d', tak040.DQ_CD) AS cycle_start_date,
                SAFE.PARSE_DATE('%Y%m%d', tak040.DQ_FD) AS cycle_end_date,
            FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.LBAKRDTA_TAK040_latest` tak040
            INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
                ON pei.state_code = 'US_MO'
                AND pei.id_type = 'US_MO_DOC'
                AND tak040.DQ_DOC = pei.external_id
            -- keep cycle with highest cycle number (which should be the most recent)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY pei.state_code, pei.person_id
                ORDER BY tak040.DQ_CYC DESC
            ) = 1
        ),
        release_dates AS (
            -- pull release dates for currently active, most recent cycles
            SELECT
                rdp.state_code,
                rdp.person_id,
                mrc.cycle_start_date,
                mrc.cycle_end_date,
                rdp.maximum_release_date,
                rdp.mandatory_release_date,
                rdp.conditional_release_date,
                /* Null out PPDs from the past (which seem to have often been from
                legitimate releases to parole that ended in revocation, such that the
                PPD was a true PPD at one point but is no longer. */
                /* TODO(#45890): Can we handle this upstream anywhere, particularly if
                we're going to be using this date for eligibility logic? Do we still
                want to null it out here if so? Is this the right logic for identifying
                when a date is no longer valid? */
                IF(
                    rdp.presumptive_parole_date < CURRENT_DATE('US/Eastern'),
                    CAST(NULL AS DATE),
                    rdp.presumptive_parole_date
                ) AS presumptive_parole_date,
            FROM `{{project_id}}.{{analyst_dataset}}.us_mo_release_dates_preprocessed_materialized` rdp
            INNER JOIN most_recent_cycle mrc
                ON rdp.state_code = mrc.state_code
                AND rdp.person_id = mrc.person_id
                AND rdp.cycle_number = mrc.cycle_number
                -- restrict to cycles that are currently active
                AND CURRENT_DATE('US/Eastern') BETWEEN mrc.cycle_start_date AND {nonnull_end_date_clause('mrc.cycle_end_date')}
        ),
        earliest_release_dates AS (
            /* Note that if a person has no non-null release dates coming out of the above
            CTE, then there will be no rows coming from this CTE for them. */
            SELECT
                state_code,
                person_id,
                cycle_start_date AS start_datetime,
                cycle_end_date AS end_datetime,
                MIN(release_date) AS critical_date,
            FROM release_dates
            UNPIVOT(release_date FOR release_date_type IN (maximum_release_date, mandatory_release_date, conditional_release_date, presumptive_parole_date))
            GROUP BY 1, 2, 3, 4
        ),
        {critical_date_has_passed_spans_cte(
            meets_criteria_leading_window_time=date_leading_interval,
            date_part=date_part,
            table_name='earliest_release_dates',
        )}
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            critical_date_has_passed AS meets_criteria,
            TO_JSON(STRUCT(
                critical_date AS earliest_release_date
            )) AS reason,
            critical_date AS earliest_release_date,
        FROM critical_date_has_passed_spans
    """

    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_MO,
        criteria_name=criteria_name,
        description=description,
        criteria_spans_query_template=criterion_query,
        meets_criteria_default=False,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_MO,
            instance=DirectIngestInstance.PRIMARY,
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        analyst_dataset=ANALYST_VIEWS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="earliest_release_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Earliest established release date",
            ),
        ],
    )
