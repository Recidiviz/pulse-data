# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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

from typing import List, Optional

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.calculator.query.state.views.tasks.tasks_criteria_utils import (
    calc_contact_period_end_date,
    create_contact_cadence_reason,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)

# The following lists of charge codes come from the field `BE_CD1` in `LBAKRCOD_TAK112`.
# `BE_CD1` is a five-digit field, with two digits that correspond with the NCIC major
# category followed by three digits that are the "unique identifier" for the charge in
# MO.

# The following charge codes can be used to identify rape offenses in Missouri that are
# not captured in NCIC category 11 (according to the NCIC category MO attaches to the
# charge code). These codes were identified by looking at offenses in `LBAKRCOD_TAK112`
# with `BE_SHD` (short description) containing 'RAPE' that are not in NCIC category
# (`BE_NC2`) 11.
MO_CHARGE_CODES_RAPE_NOT_NCIC_11 = [
    "36ABM",
]

# The following charge codes can be used to identify first-degree-assault offenses in
# Missouri and were identified by looking at offenses in `LBAKRCOD_TAK112` in NCIC
# category (`BE_NC2`) 13 (assault) and reviewing their respective
# descriptions.
MO_CHARGE_CODES_ASSAULT_FIRST_DEGREE = [
    # These codes are for offenses corresponding with RSMo 565.050, which covers first-
    # degree assault in general.
    "13010",
    "13011",
    "13020",
    "13ACH",
    "13ACJ",
    # These codes are for offenses corresponding with RSMo 565.081 (repealed by S.B.
    # 491, 2014), which covered first-degree assault of emergency personnel, public-
    # safety officials, highway workers, etc. (although that statute evolved over time).
    "13100",
    "13105",
    # TODO(#43387): Should domestic assault offenses be considered disqualifying "first-
    # degree assault" offenses? Confirm with MO and then add them in if so.
    # These codes are for first-degree domestic assault.
    # "13009",
    # "13015",
    # "13016",
    # "13018",
    # "13021",
    # "13025",
    # "13027",
    # "13AAU",
    # "13AAV",
    # "13ACG",
    # "13ACI",
]

# The following charge codes can be used to identify first-degree-murder offenses in
# Missouri and were identified by looking at offenses in `LBAKRCOD_TAK112` in NCIC
# category (`BE_NC2`) 09 (homicide).
MO_CHARGE_CODES_MURDER_FIRST_DEGREE = [
    # These codes correspond with now-repealed/-modified statutes that, when they
    # existed, defined first-degree murder.
    "10020",
    "10022",
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

# The following charge codes can be used to identify first-degree-arson offenses in
# Missouri and were identified by looking at offenses in `LBAKRCOD_TAK112` in NCIC
# category (`BE_NC2`) 20 (arson).
MO_CHARGE_CODES_ARSON_FIRST_DEGREE = [
    "17010",
    "17012",
    "17015",
    "20AAK",
    "20AAL",
    "20AAM",
]

# The following charge codes can be used to identify first-degree-robbery offenses in
# Missouri and were identified by looking at offenses in `LBAKRCOD_TAK112` in NCIC
# category (`BE_NC2`) 12 (robbery).
MO_CHARGE_CODES_ROBBERY_FIRST_DEGREE = [
    "12010",
    "12011",
    "12012",
    "12030",
    "12035",
    "12AAA",
    "12AAC",
]

# The following sanction codes are for progressive-discipline sanctions. MO is
# transitioning from using D1 to using D12, D13, D14, and D15 in early 2026.
MO_PROGRESSIVE_DISCIPLINE_SANCTIONS = [
    "D1",
    "D12",
    "D13",
    "D14",
    "D15",
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


def latest_progressive_discipline_sanction_spans_cte() -> str:
    return f"""progressive_discipline_sanctions_dedup AS (
            -- Multiple progressive-discipline sanctions incurred on the same day may
            -- be distinct, but we only need to count them once in determining when
            -- someone's last sanction began.
            SELECT DISTINCT
                state_code,
                person_id,
                date_effective
            FROM `{{project_id}}.us_mo_normalized_state.state_incarceration_incident_outcome`
            WHERE outcome_type_raw_text IN ({list_to_query_string(MO_PROGRESSIVE_DISCIPLINE_SANCTIONS, quoted=True)})
        )
        ,
        latest_progressive_discipline_sanction_spans AS (
            SELECT
                state_code,
                person_id,
                -- When someone begins an active sanction, their "latest sanction start date" 
                -- will be that sanction's start date until the next time they incur a sanction.
                date_effective AS start_date,
                LEAD(date_effective) OVER w AS end_date,
                date_effective AS latest_progressive_discipline_sanction_start_date,
            FROM progressive_discipline_sanctions_dedup
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
        date_part (str): Supports any of the BigQuery date_part values: "DAY", "WEEK",
            "MONTH", "QUARTER", "YEAR".
    Returns:
        StateSpecificTaskCriteriaBigQueryViewBuilder: View builder for spans of time
            when someone is sufficiently close to their earliest established release
            date.
    """

    # TODO(#46222): Can we make this historically accurate and not just a snapshot of
    # current eligibility?
    criterion_query = f"""
        WITH most_recent_cycle AS (
            /* Pull most recent cycle for each person in MO. Note that this will not
            necessarily be an active cycle for all people in MO data, but for residents
            currently incarcerated, the most recent cycle should be the current one. */
            SELECT
                state_code,
                person_id,
                cycle_number,
                cycle_start_date,
                cycle_end_date,
            FROM `{{project_id}}.{{analyst_dataset}}.us_mo_cycles_preprocessed_materialized`
            WHERE is_most_recent_cycle
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
                PPD was a true PPD at one point but is no longer). */
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
            /* Note that if a person has no non-null release dates coming out of the
            above CTE, then there will be no rows coming from this CTE for them. */
            SELECT
                state_code,
                person_id,
                cycle_start_date,
                cycle_end_date,
                release_date,
                release_date_type,
            FROM release_dates
            UNPIVOT(release_date FOR release_date_type IN (maximum_release_date, mandatory_release_date, conditional_release_date, presumptive_parole_date))
            /* Filter to row(s) with a date matching the earliest date for a person-
            cycle. If a person has multiple dates that are the same and the earliest of
            their dates, we'll keep those multiple rows here via RANK. */
            QUALIFY RANK() OVER (
                PARTITION BY state_code, person_id, cycle_start_date, cycle_end_date
                ORDER BY release_date
            ) = 1
        ),
        earliest_release_dates_aggregated AS (
            SELECT
                state_code,
                person_id,
                cycle_start_date AS start_datetime,
                cycle_end_date AS end_datetime,
                /* We already filtered to the earliest release date(s) for a person-
                cycle, so regardless of which date we take here, it will be the earliest
                date from their set of dates. */
                ANY_VALUE(release_date) AS critical_date,
                /* For all the date types that match the earliest date, aggregate them
                into an array. */
                ARRAY_AGG(release_date_type ORDER BY release_date_type) AS earliest_release_date_types,
            FROM earliest_release_dates
            GROUP BY 1, 2, 3, 4
        ),
        {critical_date_has_passed_spans_cte(
            meets_criteria_leading_window_time=date_leading_interval,
            date_part=date_part,
            table_name="earliest_release_dates_aggregated",
            attributes=["earliest_release_date_types"],
        )}
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            critical_date_has_passed AS meets_criteria,
            TO_JSON(STRUCT(
                critical_date AS earliest_release_date,
                earliest_release_date_types
            )) AS reason,
            critical_date AS earliest_release_date,
            earliest_release_date_types,
        FROM critical_date_has_passed_spans
    """

    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_MO,
        criteria_name=criteria_name,
        description=description,
        criteria_spans_query_template=criterion_query,
        meets_criteria_default=False,
        analyst_dataset=ANALYST_VIEWS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="earliest_release_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Earliest established release date",
            ),
            ReasonsField(
                name="earliest_release_date_types",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="Type(s) of earliest release date(s)",
            ),
        ],
    )


# TODO(#54133): Validate logic, especially for "2+ per 1 period" contacts and "2+ per 2+
# period" contacts.
def us_mo_contact_compliance_builder(
    *,
    criteria_name: str,
    description: str,
    contact_category: str,
    reasons_field_suffix: str = "",
    supplementary_contact_types: Optional[List[str]] = None,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a state-specific criterion view builder identifying when someone in MO
    is due (i.e., has not met requirements) for a category of contact that can encompass
    multiple contact types.

    Args:
        criteria_name (str): The name of the criterion view.
        description (str): A brief description of the criterion view.
        contact_category (str): The category of contact being considered, as specified
            in `us_mo_contact_standards.csv`.
        reasons_field_suffix (str, optional): Suffix to append to names of reasons
            fields. Can be used to make it easier to group criteria by ensuring reasons
            fields for sub-criteria have different names. Default: "" (adds no suffix).
        supplementary_contact_types (List[str], optional): Types for any supplementary
            contacts to include in the reasons blob. Can be used to include, for
            example, recent attempted contacts or contacts of a type complementary to
            the type(s) specified in the contact requirement. Should correspond with the
            types from `us_mo_contact_events_preprocessed`.
    Returns:
        StateSpecificTaskCriteriaBigQueryViewBuilder: View builder for spans of time
            when someone is due for a contact within the specified category.
    """

    reasons_field_name_category_of_contact = (
        f"category_of_contact{reasons_field_suffix}"
    )
    reasons_field_name_contact_types_accepted = (
        f"contact_types_accepted{reasons_field_suffix}"
    )
    reasons_field_name_contact_cadence = f"contact_cadence{reasons_field_suffix}"
    reasons_field_name_contact_count = f"contact_count{reasons_field_suffix}"
    reasons_field_name_contact_due_date = f"contact_due_date{reasons_field_suffix}"
    reasons_field_name_last_contact_date = f"last_contact_date{reasons_field_suffix}"
    reasons_field_name_overdue_flag = f"overdue_flag{reasons_field_suffix}"
    reasons_field_name_supplementary_contacts = (
        f"supplementary_contacts{reasons_field_suffix}"
    )

    if supplementary_contact_types is not None:
        supplementary_contact_inclusion_condition = f"OR ce.contact_type IN ({list_to_query_string(supplementary_contact_types, quoted=True)})"
    else:
        supplementary_contact_inclusion_condition = ""

    criterion_query = f"""
    WITH contact_events AS (
        SELECT 
            state_code,
            person_id,
            contact_external_id,
            contact_date,
            contact_type,
        FROM `{{project_id}}.tasks_views.us_mo_contact_events_preprocessed_materialized` 
        WHERE status = 'COMPLETED'
    ),
    contact_cadence_spans AS (
        SELECT *
        FROM `{{project_id}}.tasks_views.us_mo_contact_requirement_spans_materialized`
        WHERE contact_category = '{contact_category}'
    ),
    /* Link contact events to the contact-requirement spans during which they occur. The
    following CTE returns a set of rows unique at the contact-event level, where a
    single contact-requirement span may be linked to multiple contact events occurring
    within that span. */
    contact_events_with_cadence_spans AS (
        SELECT
            ccs.state_code,
            ccs.person_id,
            ccs.contact_standard_period_start,
            ccs.contact_standard_period_end,
            ccs.contact_category,
            ccs.contact_types_accepted,
            ccs.frequency,
            ccs.frequency_date_part,
            ccs.quantity,
            ce.contact_external_id,
            ce.contact_date,
            /* Flag "qualifying" contacts (that count toward contact requirements). If a
            contact is not in the list of `contact_types_accepted`, then we'll treat it
            as a supplementary contact and incorporate it only as additional information
            in the reasons blobs of criterion spans. */
            (ce.contact_type IN UNNEST(SPLIT(ccs.contact_types_accepted, ','))) AS is_qualifying_contact,
        FROM contact_events ce
        /* Use INNER JOIN because we're only interested in contacts that happen during a
        span of time for which contact requirements are defined. Also, we'll create
        baseline spans separately for contact periods prior to considering any contacts,
        so we only need to keep contact periods here that overlap with contacts. */
        INNER JOIN contact_cadence_spans ccs
            ON ce.state_code = ccs.state_code
            AND ce.person_id = ccs.person_id
            AND (
                ce.contact_type IN UNNEST(SPLIT(ccs.contact_types_accepted, ','))
                {supplementary_contact_inclusion_condition}
            )
            AND ce.contact_date BETWEEN ccs.contact_standard_period_start AND {nonnull_end_date_exclusive_clause('ccs.contact_standard_period_end')}
        /* Sometimes, a single contact (with one `contact_external_id`) may include
        multiple types of contacts that count toward a requirement, and those types may
        be split out into different rows. However, even though that single contact may
        have multiple qualifying types, we only want to count that contact once toward
        meeting requirements. Here, in cases where there are multiple rows coming from
        the preprocessed contacts view with the same `contact_external_id`, we'll reduce
        it to one row and treat it as one contact. If all that differs between the two
        rows is the `contact_type`, it doesn't matter which row we choose, since we're
        not keeping the `contact_type` as output from this CTE anyways. */
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ce.state_code, ce.person_id, ce.contact_external_id
            ORDER BY IF(is_qualifying_contact, 1, 2)
        ) = 1
    ),
    /* Create spans of time when someone has an upcoming contact due date. For month- or
    week-based contact requirements, we extend due dates to the end of the relevant
    calendar month or week. (For example, for a "once every four months" contact
    requirement, if the latest contact occurred on January 15, the due date of the next
    contact would be May 30.) */
    due_date_spans_initial_compliance AS (
        /* Create spans based on when someone starts on a supervision level / case type.
        Treat each level/type change as a fresh start, without looking back to previous
        contacts but allowing for a full contact interval to come into compliance. (For
        example, for a "once every four months" contact requirement, we allow four
        months from the change before the next contact is due.)
        TODO(#54133): Do we need to adjust this logic to align with MO's practices? */
        SELECT
            state_code,
            person_id,
            contact_standard_period_start AS start_date,
            contact_standard_period_end AS end_date,
            contact_standard_period_start,
            contact_category,
            contact_types_accepted,
            frequency,
            frequency_date_part,
            quantity,
            {calc_contact_period_end_date(period_start_date="contact_standard_period_start")}
                AS initial_compliance_due_date,
        FROM contact_cadence_spans
    ),
    due_date_spans_from_contacts AS (
        /* Create spans based on when someone has a qualifying contact. Each contact
        acts as a fresh start, such that the next contact won't be due for another full
        contact interval. (For example, for a "once every four months" requirement, the
        due date for the subsequent contact will be four months after the latest
        contact.) */
        WITH critical_date_spans_contact_expiration AS (
            SELECT
                state_code,
                person_id,
                contact_date AS start_datetime,
                contact_standard_period_end AS end_datetime,
                /* Create a critical date that indicates when each contact will "expire"
                (i.e., no longer count toward meeting current requirements, because it's
                too old). The expiration date of one contact determines the due date of
                the next. */
                DATE_ADD(
                    ({calc_contact_period_end_date(period_start_date="contact_date")}),
                    INTERVAL 1 DAY
                ) AS critical_date,
                contact_standard_period_start,
                contact_category,
                contact_types_accepted,
                frequency,
                frequency_date_part,
                quantity,
                contact_date AS last_contact_date,
            FROM contact_events_with_cadence_spans
            WHERE is_qualifying_contact
        ),
        {critical_date_has_passed_spans_cte(
            table_name="critical_date_spans_contact_expiration",
            cte_suffix="_contact_expiration",
            attributes=[
                "contact_standard_period_start",
                "contact_category",
                "contact_types_accepted",
                "frequency",
                "frequency_date_part",
                "quantity",
                "last_contact_date",
            ],
        )}
        SELECT 
            state_code,
            person_id,
            start_date,
            end_date,
            contact_standard_period_start,
            contact_category,
            contact_types_accepted,
            frequency,
            frequency_date_part,
            quantity,
            -- if the critical date has passed, the contact has expired
            IF(critical_date_has_passed, 0, 1) AS contact_count,
            critical_date AS contact_expiration_date,
            last_contact_date,
        FROM critical_date_has_passed_spans_contact_expiration
    ),
    due_date_spans_all AS (
        -- spans with due dates for initial compliance
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            contact_standard_period_start,
            contact_category,
            contact_types_accepted,
            frequency,
            frequency_date_part,
            quantity,
            0 AS contact_count,
            CAST(NULL AS DATE) AS contact_expiration_date,
            initial_compliance_due_date,
            CAST(NULL AS DATE) AS last_contact_date,
            CAST(NULL AS DATE) AS supplementary_contact_date,
            CAST(NULL AS STRING) AS supplementary_contact_types,
        FROM due_date_spans_initial_compliance
        UNION ALL
        -- spans with due dates driven by contact events
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            contact_standard_period_start,
            contact_category,
            contact_types_accepted,
            frequency,
            frequency_date_part,
            quantity,
            contact_count,
            contact_expiration_date,
            CAST(NULL AS DATE) AS initial_compliance_due_date,
            last_contact_date,
            CAST(NULL AS DATE) AS supplementary_contact_date,
            CAST(NULL AS STRING) AS supplementary_contact_types,
        FROM due_date_spans_from_contacts
        UNION ALL
        /* Create spans based on when someone has a supplementary contact. These
        contacts don't affect compliance but are relevant contextual information we want
        to surface. We incorporate them at this point so we can break up spans according
        to when these contacts occur, in addition to when other pieces of compliance-
        relevant information change. */
        SELECT
            cewcs.state_code,
            cewcs.person_id,
            cewcs.contact_date AS start_date,
            cewcs.contact_standard_period_end AS end_date,
            cewcs.contact_standard_period_start,
            cewcs.contact_category,
            cewcs.contact_types_accepted,
            cewcs.frequency,
            cewcs.frequency_date_part,
            cewcs.quantity,
            0 AS contact_count,
            CAST(NULL AS DATE) AS contact_expiration_date,
            CAST(NULL AS DATE) AS initial_compliance_due_date,
            CAST(NULL AS DATE) AS last_contact_date,
            cewcs.contact_date AS supplementary_contact_date,
            JSON_VALUE(ssc.supervision_contact_metadata, '$.CONTACT_TYPES') AS supplementary_contact_types,
        FROM contact_events_with_cadence_spans cewcs
        /* Pull in additional info so we can show all types for a given contact
        instance. Note that we're pulling in `StateSupervisionContact` entities for
        supplementary contacts, which are unique by external ID and may include multiple
        contact types in the metadata for a given `StateSupervisionContact`. We do so
        because we want each supplementary contact in the reasons blob to match up with
        a single contact as conducted on supervision. This means that, for example, if
        we include both 'HV' and 'PLN' as supplementary contact types, a single
        `StateSupervisionContact` on 12/31 with both of those types will be a single
        supplementary contact in the reasons blob, rather than having two supplementary
        contacts (an 'HV' contact on 12/31 and a 'PLN' contact on 12/31), since that
        should align better with how staff enter and understand contact data. */ 
        LEFT JOIN `{{project_id}}.us_mo_normalized_state.state_supervision_contact` ssc
            ON cewcs.state_code = ssc.state_code
            AND cewcs.person_id = ssc.person_id
            AND cewcs.contact_external_id = ssc.external_id
        WHERE NOT cewcs.is_qualifying_contact
    ),
    -- sub-sessionize to combine due dates from all of the spans
    {create_sub_sessions_with_attributes(table_name="due_date_spans_all")},
    sub_sessions_with_attributes_aggregated AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            -- these should all be the same across all rows anyways
            ANY_VALUE(contact_standard_period_start) AS contact_standard_period_start,
            ANY_VALUE(contact_category) AS contact_category,
            ANY_VALUE(contact_types_accepted) AS contact_types_accepted,
            ANY_VALUE(frequency) AS frequency,
            ANY_VALUE(frequency_date_part) AS frequency_date_part,
            ANY_VALUE(quantity) AS quantity,
            SUM(contact_count) AS contact_count,
            -- all expiration dates for contacts that have occurred by start date
            ARRAY_AGG(contact_expiration_date IGNORE NULLS ORDER BY contact_expiration_date DESC) AS contact_expiration_date_array,
            -- should pick up the one non-null date
            ANY_VALUE(initial_compliance_due_date) AS initial_compliance_due_date,
            -- pull most recent contact date
            MAX(last_contact_date) AS last_contact_date,
            -- assemble array of supplementary contacts
            ARRAY_AGG(
                IF(
                    supplementary_contact_date IS NOT NULL,
                    STRUCT(
                        supplementary_contact_date AS contact_date,
                        supplementary_contact_types AS contact_types
                    ),
                    CAST(NULL AS STRUCT<contact_date DATE, contact_types STRING>)
                )
                IGNORE NULLS
                ORDER BY supplementary_contact_date DESC, supplementary_contact_types
            ) AS supplementary_contacts,
        FROM sub_sessions_with_attributes
        GROUP BY 1, 2, 3, 4
    ),
    sub_sessions_with_attributes_aggregated_with_supplementary_contacts_filtered AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            contact_category,
            contact_types_accepted,
            frequency,
            frequency_date_part,
            quantity,
            contact_count,
            contact_expiration_date_array,
            initial_compliance_due_date,
            last_contact_date,
            /* Re-assemble array of supplementary contacts after filtering out contacts
            that happened before the `last_contact_date` (if available). We convert each
            STRUCT to a JSON-formatted string before ARRAY_AGG-ing them back together so
            that we end up with an array of STRINGs rather than an array of STRUCTs.
            This enables the data in those STRUCTs to be correctly extracted downstream
            for tasks details in our current infra. */
            ARRAY_AGG(
                IF(
                    single_supplementary_contact.contact_date > COALESCE(last_contact_date, contact_standard_period_start),
                    TO_JSON_STRING(STRUCT(
                        single_supplementary_contact.contact_date AS contact_date,
                        single_supplementary_contact.contact_types AS contact_types
                    )),
                    CAST(NULL AS STRING)
                )
                IGNORE NULLS
                ORDER BY contact_date DESC, contact_types
            ) AS supplementary_contacts,
        FROM sub_sessions_with_attributes_aggregated
        LEFT JOIN UNNEST(supplementary_contacts) AS single_supplementary_contact
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    ),
    /* Next, we'll use "critical date has passed" logic to split spans by contact due
    dates, enabling us to identify when a contact will be overdue. */
    critical_date_spans_due_date AS (
        SELECT
            state_code,
            person_id,
            start_date AS start_datetime,
            end_date AS end_datetime,
            /* The due date for the next contact should be the last day on which the
            last relevant contact expires. If that contact's expiration date comes, then
            the next contact is overdue. We therefore set the critical date to the date
            on which that contact expires. If there is no relevant contact yet to set
            the due date for the next, we look back to the initial compliance due date.
            We add one day to that date to reflect the fact that the contact would not
            become overdue until the due date passes. */
            COALESCE(
                contact_expiration_date_array[SAFE_ORDINAL(quantity)],
                DATE_ADD(initial_compliance_due_date, INTERVAL 1 DAY)
            ) AS critical_date,
            contact_category,
            contact_types_accepted,
            frequency,
            frequency_date_part,
            quantity,
            contact_count,
            last_contact_date,
            supplementary_contacts,
        FROM sub_sessions_with_attributes_aggregated_with_supplementary_contacts_filtered
    ),
    {critical_date_has_passed_spans_cte(
        table_name="critical_date_spans_due_date",
        cte_suffix="_due_date",
        attributes=[
            "contact_category",
            "contact_types_accepted",
            "frequency",
            "frequency_date_part",
            "quantity",
            "contact_count",
            "last_contact_date",
            "supplementary_contacts",
        ],
    )}
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        /* Clients will always be eligible, since the only thing that will change when
        an officer completes a contact is that the due date for the next contact gets
        bumped out. There will never be a time period where the client has no upcoming
        due dates. */
        TRUE AS meets_criteria,
        contact_category AS {reasons_field_name_category_of_contact},
        contact_types_accepted AS {reasons_field_name_contact_types_accepted},
        {create_contact_cadence_reason()} AS {reasons_field_name_contact_cadence},
        contact_count AS {reasons_field_name_contact_count},
        -- next contact's due date is 1 day before the day when it would become overdue
        DATE_SUB(critical_date, INTERVAL 1 DAY) AS {reasons_field_name_contact_due_date},
        last_contact_date AS {reasons_field_name_last_contact_date},
        -- if the critical date has passed, the contact is overdue
        critical_date_has_passed AS {reasons_field_name_overdue_flag},
        supplementary_contacts AS {reasons_field_name_supplementary_contacts},
        TO_JSON(STRUCT(
            contact_category AS {reasons_field_name_category_of_contact},
            contact_types_accepted AS {reasons_field_name_contact_types_accepted},
            {create_contact_cadence_reason()} AS {reasons_field_name_contact_cadence},
            contact_count AS {reasons_field_name_contact_count},
            DATE_SUB(critical_date, INTERVAL 1 DAY) AS {reasons_field_name_contact_due_date},
            last_contact_date AS {reasons_field_name_last_contact_date},
            critical_date_has_passed AS {reasons_field_name_overdue_flag},
            supplementary_contacts AS {reasons_field_name_supplementary_contacts}
        )) AS reason,
    FROM critical_date_has_passed_spans_due_date
    """

    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_MO,
        criteria_name=criteria_name,
        criteria_spans_query_template=criterion_query,
        description=description,
        meets_criteria_default=False,
        reasons_fields=[
            ReasonsField(
                name=reasons_field_name_category_of_contact,
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Category of contact due",
            ),
            ReasonsField(
                name=reasons_field_name_contact_types_accepted,
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Types of contacts included in category",
            ),
            ReasonsField(
                name=reasons_field_name_contact_cadence,
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Contact cadence requirement",
            ),
            ReasonsField(
                name=reasons_field_name_contact_count,
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Number of currently applicable contacts",
            ),
            ReasonsField(
                name=reasons_field_name_contact_due_date,
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Due date of the contact",
            ),
            ReasonsField(
                name=reasons_field_name_last_contact_date,
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of the last contact",
            ),
            ReasonsField(
                name=reasons_field_name_overdue_flag,
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Flag that indicates whether contact was missed",
            ),
            ReasonsField(
                name=reasons_field_name_supplementary_contacts,
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="Supplementary contacts to include in reasons blob",
            ),
        ],
    )


def us_mo_create_initial_contact_period_reason() -> str:
    return """
        CASE
            WHEN period = 1
                THEN CONCAT(quantity, " WITHIN FIRST ", period_date_part) 
            ELSE
                CONCAT(quantity, " WITHIN FIRST ", period, " ", period_date_part, "S") 
        END
    """


def us_mo_non_recurring_contact_compliance_builder(
    *,
    criteria_name: str,
    description: str,
    contact_category: str,
    contact_types_accepted: List[str],
    contact_period_spans_query: str,
    quantity: str,
    time_period: str,
    time_period_date_part: str,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a state-specific criterion view builder identifying when someone in MO
    is due (i.e., has not met requirements) for some category of contact with one or
    more qualifying contact types, where such requirements are one-time/non-recurring
    and due within some certain initial period.

    Args:
        criteria_name (str): The name of the criterion view.
        description (str): A brief description of the criterion view.
        contact_category (str): The category of contact being considered.
        contact_types_accepted (List[str]): The types of contacts that count toward
            contact requirements for that category of contact.
        contact_period_spans_query (str): Query to pull contact periods relevant for the
            criterion. Must return `state_code`, `person_id`, `start_date` (when the
            non-recurring contact requirement begins to apply), and `end_date` (an
            exclusive end date, when the requirement ceases to be relevant).
        quantity (str): The number of contacts required in the period (e.g., 2).
        time_period (str): Number of <date_part> within which the requirement must be
            met.
        time_period_date_part (str): Supports any of the BigQuery date_part values:
            "DAY", "WEEK", "MONTH", "QUARTER", or "YEAR".
    Returns:
        StateSpecificTaskCriteriaBigQueryViewBuilder: View builder for spans of time
            when someone is due for a contact.
    """

    criterion_query = f"""
    WITH contact_events AS (
        SELECT 
            state_code,
            person_id,
            contact_external_id,
            contact_date,
        FROM `{{project_id}}.tasks_views.us_mo_contact_events_preprocessed_materialized` 
        WHERE status = 'COMPLETED'
            AND contact_type IN ({list_to_query_string(contact_types_accepted, quoted=True)})
        /* Sometimes, a single contact (with one `contact_external_id`) may include
        multiple types of contacts that count toward a requirement, and those types may
        be split out into different rows. However, even though that single contact may
        have multiple qualifying types, we only want to count that contact once toward
        meeting requirements. Here, in cases where there are multiple rows coming from
        the preprocessed contacts view with the same `contact_external_id`, we'll reduce
        it to one row and treat it as one contact. If all that differs between the two
        rows is the `contact_type`, it doesn't matter which row we choose, since we're
        not keeping the `contact_type` as output from this CTE anyways. */
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY state_code, person_id, contact_external_id
            ORDER BY contact_type DESC
        ) = 1
    ),
    contact_period_spans AS (
        {contact_period_spans_query}
    ),
    due_date_spans AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            {quantity} AS quantity,
            {time_period} AS period,
            '{time_period_date_part}' AS period_date_part,
            start_date AS contact_period_start_date,
            DATE_ADD(start_date, INTERVAL {time_period} {time_period_date_part}) AS compliance_due_date,
            0 AS contact_count,
            CAST(NULL AS DATE) AS last_contact_date,
        FROM contact_period_spans
        UNION ALL
        SELECT
            cps.state_code,
            cps.person_id,
            ce.contact_date AS start_date,
            cps.end_date,
            {quantity} AS quantity,
            {time_period} AS period,
            '{time_period_date_part}' AS period_date_part,
            cps.start_date AS contact_period_start_date,
            CAST(NULL AS DATE) AS compliance_due_date,
            1 AS contact_count,
            ce.contact_date AS last_contact_date,
        FROM contact_events ce
        INNER JOIN contact_period_spans cps
            ON ce.state_code = cps.state_code
            AND ce.person_id = cps.person_id
            AND ce.contact_date BETWEEN cps.start_date AND {nonnull_end_date_exclusive_clause('cps.end_date')}
    ),
    -- sub-sessionize to combine due dates from all of the spans
    {create_sub_sessions_with_attributes(table_name="due_date_spans")},
    -- aggregate sub-sessions to deduplicate between overlapping spans
    sub_sessions_with_attributes_aggregated AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            -- these should all be the same across all rows anyways
            ANY_VALUE(quantity) AS quantity,
            ANY_VALUE(period) AS period,
            ANY_VALUE(period_date_part) AS period_date_part,
            ANY_VALUE(contact_period_start_date) AS contact_period_start_date,
            -- should pick up the one non-null date
            ANY_VALUE(compliance_due_date) AS compliance_due_date,
            SUM(contact_count) AS contact_count,
            -- pull most recent contact date
            MAX(last_contact_date) AS last_contact_date,
        FROM sub_sessions_with_attributes
        GROUP BY 1, 2, 3, 4
    ),
    /* Next, we'll use "critical date has passed" logic to split spans by contact due
    dates, enabling us to identify when a contact will be overdue. */
    critical_date_spans AS (
        SELECT
            state_code,
            person_id,
            start_date AS start_datetime,
            end_date AS end_datetime,
            /* We add one day to the due date to get our critical date to reflect the
            fact that the contact would not become overdue until the due date passes. */
            DATE_ADD(compliance_due_date, INTERVAL 1 DAY) AS critical_date,
            quantity,
            period,
            period_date_part,
            contact_period_start_date,
            compliance_due_date,
            contact_count,
            last_contact_date,
        FROM sub_sessions_with_attributes_aggregated
    ),
    {critical_date_has_passed_spans_cte(
        table_name="critical_date_spans",
        attributes=[
            "quantity",
            "period",
            "period_date_part",
            "contact_period_start_date",
            "compliance_due_date",
            "contact_count",
            "last_contact_date",
        ],
    )}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        (contact_count < quantity) AS meets_criteria,
        '{contact_category}' AS contact_category,
        '{list_to_query_string(contact_types_accepted)}' AS contact_types_accepted,
        {us_mo_create_initial_contact_period_reason()} AS contact_cadence,
        contact_count,
        compliance_due_date AS contact_due_date,
        last_contact_date,
        -- if contact count is too low & critical date has passed, contact is overdue
        ((contact_count < quantity) AND critical_date_has_passed) AS overdue_flag,
        contact_period_start_date,
        TO_JSON(STRUCT(
            '{contact_category}' AS contact_category,
            '{list_to_query_string(contact_types_accepted)}' AS contact_types_accepted,
            {us_mo_create_initial_contact_period_reason()} AS contact_cadence,
            contact_count,
            compliance_due_date AS contact_due_date,
            last_contact_date,
            -- if contact count is too low & critical date has passed, contact is overdue
            ((contact_count < quantity) AND critical_date_has_passed) AS overdue_flag,
            contact_period_start_date
        )) AS reason,
        FROM critical_date_has_passed_spans
    """
    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_MO,
        criteria_name=criteria_name,
        criteria_spans_query_template=criterion_query,
        description=description,
        meets_criteria_default=False,
        reasons_fields=[
            ReasonsField(
                name="contact_category",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Category of contact due",
            ),
            ReasonsField(
                name="contact_types_accepted",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Types of contacts included in category",
            ),
            ReasonsField(
                name="contact_cadence",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Initial contact period requirement",
            ),
            ReasonsField(
                name="contact_count",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Number of contacts done within the initial contact period",
            ),
            ReasonsField(
                name="contact_due_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Due date of the contact",
            ),
            ReasonsField(
                name="last_contact_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of the last contact",
            ),
            ReasonsField(
                name="overdue_flag",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Flag that indicates whether contact was missed",
            ),
            ReasonsField(
                name="contact_period_start_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date when the relevant contact period began",
            ),
        ],
    )
