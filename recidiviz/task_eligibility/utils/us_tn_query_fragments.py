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
"""Create helper SQL queries for Tennessee for use in task eligibility spans."""

from typing import List, Union

from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    num_events_within_time_interval_spans,
)

# TODO(#20870): Rather than having to repeatedly exclude these levels from
# opportunities, should they be remapped differently so that they won't get grouped in
# with the supervision levels for clients not in the PSU or DRC programs?
# These are the raw-text supervision levels that correspond with participation in Day
# Reporting Center (DRC) programs in TN. They're often excluded from various
# supervision-side opportunities.
DRC_SUPERVISION_LEVELS_RAW_TEXT = ["1D1", "2D2", "3D3"]
# These are the raw-text supervision levels that correspond with the Programmed
# Supervision Unit (PSU) in TN. They're often excluded from various supervision-side
# opportunities.
PSU_SUPERVISION_LEVELS_RAW_TEXT = ["6P1", "6P2", "6P3", "6P4"]

# These are the raw-text supervision levels that correspond with Suspension of Direct
# Supervision (SDS) in TN.
SDS_SUPERVISION_LEVELS_RAW_TEXT = [
    "SDS",  # old code
    "9SD",  # current code
]

FACE_TO_FACE_CONTACTS = ["FAC1", "FAC2", "FACA", "FACF", "FACI", "FACO"]

# The following list contains contact-note codes that indicate when a warrant has been
# issued for a client in TN.
# The 'PWAR' code is also relevant for warrants and is used for probation clients;
# however, this code means that the violation has been submitted and is awaiting
# judicial approval, not necessarily that a warrant has been issued. For this reason, we
# do not include the 'PWAR' code in the following list.
WARRANT_CONTACTS = [
    # Absconder Warrant Issued [according to one of our TN TTs (as of 03/2025), this is
    # an old code that doesn't get used any more]
    "ABSW",
    # CSL [Community Supervision for Life] Warrant Issued
    "CSLW",
    # Master Tamper Warrant Issued [related to GPS monitoring; note that code looks
    # misspelled, but this is the code we see in the contact-notes data]
    "GSPW",
    # Sex Offender Registry: Warrant Issued
    "SORW",
    # Violation Warrant and Report Issued [indicates when a warrant has been issued;
    # primarily used for parole clients and is inconsistently used for probation
    # clients]
    "VWAR",
]

# TODO(#38066): Add a unittest to make sure this list matches domains in ingest mappings
STRONG_R_ASSESSMENT_METADATA_KEYS = [
    "FRIENDS_NEED_LEVEL",
    "ATTITUDE_BEHAVIOR_NEED_LEVEL",
    "AGGRESSION_NEED_LEVEL",
    "MENTAL_HEALTH_NEED_LEVEL",
    "ALCOHOL_DRUG_NEED_LEVEL",
    "RESIDENT_NEED_LEVEL",
    "FAMILY_NEED_LEVEL",
    "EMPLOYMENT_NEED_LEVEL",
    "EDUCATION_NEED_LEVEL",
]
# TODO(#38066): Add a unittest to make sure this list matches domains in ingest mappings
STRONG_R2_ASSESSMENT_METADATA_KEYS = [
    "V2_FRIENDS_RESIDENTIAL_NEED_LEVEL",
    "V2_ATTITUDE_BEHAVIOR_NEED_LEVEL",
    "V2_AGGRESSION_NEED_LEVEL",
    "V2_MENTAL_HEALTH_NEED_LEVEL",
    "V2_ALCOHOL_DRUG_NEED_LEVEL",
    "V2_FAMILY_NEED_LEVEL",
    "V2_EDUCATION_EMPLOYMENT_NEED_LEVEL",
]
STRONGR_ASSESSMENT_METADATA_DICT = {
    "FRIENDS_NEED_LEVEL": {
        "STRONG_R": "FRIENDS_NEED_LEVEL",
        "STRONG_R2": "V2_FRIENDS_RESIDENTIAL_NEED_LEVEL",
    },
    "ATTITUDE_BEHAVIOR_NEED_LEVEL": {
        "STRONG_R": "ATTITUDE_BEHAVIOR_NEED_LEVEL",
        "STRONG_R2": "V2_ATTITUDE_BEHAVIOR_NEED_LEVEL",
    },
    "AGGRESSION_NEED_LEVEL": {
        "STRONG_R": "AGGRESSION_NEED_LEVEL",
        "STRONG_R2": "V2_AGGRESSION_NEED_LEVEL",
    },
    "MENTAL_HEALTH_NEED_LEVEL": {
        "STRONG_R": "MENTAL_HEALTH_NEED_LEVEL",
        "STRONG_R2": "V2_MENTAL_HEALTH_NEED_LEVEL",
    },
    "ALC_DRUG_NEED_LEVEL": {
        "STRONG_R": "ALCOHOL_DRUG_NEED_LEVEL",
        "STRONG_R2": "V2_ALCOHOL_DRUG_NEED_LEVEL",
    },
    "RESIDENT_NEED_LEVEL": {
        "STRONG_R": "RESIDENT_NEED_LEVEL",
        "STRONG_R2": "V2_FRIENDS_RESIDENTIAL_NEED_LEVEL",
    },
    "FAMILY_NEED_LEVEL": {
        "STRONG_R": "FAMILY_NEED_LEVEL",
        "STRONG_R2": "V2_FAMILY_NEED_LEVEL",
    },
    "EMPLOYMENT_NEED_LEVEL": {
        "STRONG_R": "EMPLOYMENT_NEED_LEVEL",
        "STRONG_R2": "V2_EDUCATION_EMPLOYMENT_NEED_LEVEL",
    },
    "EDUCATION_NEED_LEVEL": {
        "STRONG_R": "EDUCATION_NEED_LEVEL",
        "STRONG_R2": "V2_EDUCATION_EMPLOYMENT_NEED_LEVEL",
    },
}


# Combines state_supervision_violation_response with state_supervision_violation_response_decision_entry
# to keep person-date level sanctions. We filter out certain types of decisions that don't result in an impact
# on a client's supervision.
supervision_sanctions_cte = """
    SELECT
        state_code,
        person_id,
        vr.response_date AS sanction_date,
    FROM `{project_id}.normalized_state.state_supervision_violation_response` vr
    /* NB: while (as of the time of writing this) in some states there are violation
    responses with multiple decision entries, in TN there are not instances where a
    single violation response is associated with multiple decision entries. As a
    result, even though we are joining in the response-decision data here, because
    we are only considering TN data in this query, we won't end up introducing any
    excess rows (where the same response is joined to multiple decisions) via this
    LEFT JOIN. */
    LEFT JOIN
        `{project_id}.normalized_state.state_supervision_violation_response_decision_entry` vrde
    USING (state_code, person_id, supervision_violation_response_id)
    WHERE state_code='US_TN'
        /* Here, we want to exclude violation responses that did not result in
        changes to a client's supervision. Again, because there is currently a 1:1
        relationship between responses and decision entries in TN, we can simply
        filter by decision here without needing to worry about aggregating across
        decisions. */
        AND COALESCE(vrde.decision, 'NO_DECISION') NOT IN ('CONTINUANCE', 'DELAYED_ACTION', 'VIOLATION_UNFOUNDED')
"""


def negative_arrest_check_within_time_interval(
    *,
    date_interval: int,
    date_part: str,
) -> str:
    """Identify spans of time during which individuals in TN have had a NEGATIVE
    arrest check within a specified time interval (e.g., within the past 2 years).

    Args:
        date_interval (int): Number of <date_part> when a negative arrest check will
        remain valid/relevant.
        date_part (str): Supports any of the BigQuery `date_part` values: "DAY", "WEEK",
            "MONTH", "QUARTER", or "YEAR".
    Returns:
        str: SQL query as a string.
    """

    return f"""
    WITH eligibility_spans AS (
        /* Identify negative arrest checks and the spans of eligibility in which they
        result. */
        SELECT
            pei.state_code,
            pei.person_id,
            -- use date of relevant contact note as date of the negative arrest check
            CAST(CAST(contact.ContactNoteDateTime AS DATETIME) AS DATE) AS negative_arrest_check_date,
            CAST(CAST(contact.ContactNoteDateTime AS DATETIME) AS DATE) AS start_date,
            DATE_ADD(CAST(CAST(contact.ContactNoteDateTime AS DATETIME) AS DATE), INTERVAL {date_interval} {date_part}) AS end_date,
            TRUE AS meets_criteria,
        FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.ContactNoteType_latest` contact
        INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
            ON contact.OffenderID = pei.external_id
            AND pei.id_type = 'US_TN_DOC'
        WHERE contact.ContactNoteType = 'ARRN'
    ),
    /* Sub-sessionize in case there are overlapping spans (i.e., if someone has multiple
    still-relevant ARRNs at once). */
    {create_sub_sessions_with_attributes("eligibility_spans")},
    eligibility_spans_aggregated AS (
        /* Aggregate across sub-sessions to get attributes for each span of time for
        each person. */
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            LOGICAL_OR(meets_criteria) AS meets_criteria,
            MAX(negative_arrest_check_date) AS latest_negative_arrest_check_date,
        FROM sub_sessions_with_attributes
        GROUP BY 1, 2, 3, 4
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(
            latest_negative_arrest_check_date AS latest_negative_arrest_check_date
        )) AS reason,
        latest_negative_arrest_check_date,
    FROM eligibility_spans_aggregated
    """


def no_positive_arrest_check_within_time_interval(
    *,
    date_interval: int,
    date_part: str,
) -> str:
    """Identify spans of time during which individuals in TN have NOT had a POSITIVE
    arrest check within a specified time interval (e.g., within the past 2 years).

    Args:
        date_interval (int): Number of <date_part> when a positive arrest check will
        remain valid/relevant.
        date_part (str): Supports any of the BigQuery `date_part` values: "DAY", "WEEK",
            "MONTH", "QUARTER", or "YEAR".
    Returns:
        str: SQL query as a string.
    """

    return f"""
    WITH arrp_sessions_cte AS
    (
        SELECT  
            DISTINCT
            pei.state_code,
            pei.person_id, 
            CAST(CAST(contact.ContactNoteDateTime AS datetime) AS DATE) AS start_date,
            DATE_ADD(CAST(CAST(contact.ContactNoteDateTime AS DATETIME) AS DATE), INTERVAL {date_interval} {date_part}) AS end_date,
            /* Create this field to keep track of the actual positive arrest check date
            even after we sub-sessionize to handle overlapping periods (cases when a
            person has more than 1 positive check in the lookback period). */
            CAST(CAST(contact.ContactNoteDateTime AS DATETIME) AS DATE) AS latest_positive_arrest_check_date,
            FALSE AS meets_criteria,
        FROM
            `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.ContactNoteType_latest` contact
        INNER JOIN 
            `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON
            contact.OffenderID = pei.external_id
        AND
            pei.state_code = 'US_TN'
        WHERE
            contact.ContactNoteType = 'ARRP'
    )
    ,
    /*
    If a person has more than 1 positive arrest check in the lookback period, they will
    have overlapping sessions created in the above CTE. Therefore, we use
    `create_sub_sessions_with_attributes` to break these up.
    */
    {create_sub_sessions_with_attributes('arrp_sessions_cte')}
    ,
    dedup_cte AS
    /*
    If a person has more than 1 positive arrest check in the lookback period, they will
    have duplicate sub-sessions for the period of time where there was more than 1
    relevant positive check. For example, if a person has a positive check on January 1
    and another on March 1, there would be duplicate sessions for the period from March
    1 to December 31 because both positive checks are relevant at that time. We
    deduplicate below so that we surface the most recent positive check that is relevant
    at each time.
    */
    (
        SELECT
            *,
        FROM sub_sessions_with_attributes
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY person_id, state_code, start_date, end_date 
            ORDER BY latest_positive_arrest_check_date DESC
        ) = 1
    )
    ,
    sessionized_cte AS 
    /*
    Sessionize so that we have continuous periods of time for which a person is not
    eligible due to a positive check. A new session starts either when a person becomes
    eligible or when a person has an additional positive check within the specified time
    period, which changes the `latest_positive_arrest_check_date` value.
    */
    (
        {aggregate_adjacent_spans(
            table_name='dedup_cte',
            attribute=['latest_positive_arrest_check_date', 'meets_criteria'],
            end_date_field_name='end_date',
        )}
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(
            latest_positive_arrest_check_date AS latest_positive_arrest_check
        )) AS reason,
        latest_positive_arrest_check_date,
    FROM sessionized_cte
    """


def detainers_cte() -> str:
    """Helper method that returns a CTE getting detainer information in TN"""

    return f"""
    -- As discussed with TTs in TN, a detainer is "relevant" until it has been lifted, so we use that as
    -- our end date
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        detainer_felony_flag,
        detainer_misdemeanor_flag,
        CASE
            WHEN detainer_felony_flag = 'X' THEN 5
            WHEN detainer_misdemeanor_flag = 'X' THEN 3
            END
            AS detainer_score,
        jurisdiction,
        description,
        charge_pending,
    FROM (
        SELECT
            OffenderID,
            DATE(DetainerReceivedDate) AS start_date,
            DATE(DetainerLiftDate) AS end_date,
            -- According to TN counselors, if a detainer is missing a felony/misdemeanor flag but is from a federal
            -- agency, it's always a felony
            CASE
                WHEN DetainerFelonyFlag IS NULL
                    AND DetainerMisdemeanorFlag IS NULL
                    AND Jurisdiction IN ("FED","INS") THEN 'X'
                ELSE DetainerFelonyFlag
                END
                AS detainer_felony_flag,
            DetainerMisdemeanorFlag AS detainer_misdemeanor_flag,
            Jurisdiction AS jurisdiction,
            OffenseDescription AS description,
            ChargePendingFlag AS charge_pending,
        FROM
            `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.Detainer_latest`
        ) dis
    INNER JOIN
        `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
    ON
        dis.OffenderID = pei.external_id
    AND
        pei.state_code = 'US_TN'
    WHERE
        {nonnull_end_date_exclusive_clause('end_date')} > {nonnull_end_date_exclusive_clause('start_date')}
    """


# List of violent felony charges as defined in TN's Classification V2 CAF form
US_TN_CAF_V2_VIOLENT_FELONY_CHARGES = [
    "ADULTERATION FOOD, DRUGS",
    "AGGRAVATED ARSON",
    "AGGRAVATED ASSAULT",
    "AGGRAVATED KIDNAPPING",
    "AGGRAVATED PROSTITUTION (WITH HIV)",
    "AGGRAVATED RAPE",
    "AGGRAVATED RIOT",
    "AGGRAVATED SEXUAL ASSAULT",
    "AGGRAVATED SEXUAL BATTERY",
    "AGGRAVATED ROBBERY",
    "AGGRAVATED SPOUSE RAPE",
    "AIDING AND ABETTING BANK ROBBERY",
    "ASSAULT FROM AMBUSH",
    "ASSAULT W/DEADLY WEAPON",
    "ASSAULT W/I AGGRAVATED KIDNAPPING",
    "ASSAULT W/INTENT MANSLAUGHTER",
    "ASSAULT W/INTENT TO COMMIT FELONY",
    "ASSAULT W/INTENT TO MURDER",
    "ASSAULT W/INTENT TO RAPE",
    "ASSAULT W/INTENT TO ROB",
    "ASSAULT & BATTERY (felony only)",
    "ASSAULT & BATTERY W/INTENT CARNAL KNOWLEDGE",
    "CARJACKING",
    "CIVIL RIGHTS INTIMIDATION - WITH VIOLENCE",
    "CRIMINAL EXPOSURE TO HIV",
    "ESCAPE - TAKE HOSTAGE(S)",
    "ESP. AGGRAVATED ROBBERY",
    "ESP. AGGRAVATED BURGLARY",
    "EXPLOSIVE – THREAT",
    "GIFTS ADULTERATED CANDY/FOOD",
    "HOME INVASION",
    "INTIMIDATION – VIOLENCE",
    "KIDNAPPING",
    "KIDNAP - SEXUAL ASSAULT, RANSOM, HOSTAGE,",
    "MAYHEM",
    "MURDER 1",
    "MURDER 2",
    "MURDER OF PERSON 70+",
    "PARTICIPATING IN A RIOT",
    "RAPE - WITH VIOLENCE/FORCE/COERCION",
    "RECKLESS AGGRAVATED ASSAULT",
    "RETALIATE FOR PAST ACTION - WITH VIOLENCE",
    "ROBBERY",
    "ROBBERY - ARMED WITH DEADLY WEAPON",
    "SECOND DEGREE MURDER",
    "SEXUAL BATTERY - FORCE/COERCION",
    "STALKING",
    "THREAT TO BOMB",
    "THREATENING A WITNESS",
    "VOLUNTARY MANSLAUGHTER",
    "WILLFUL INJURY W/EXPLOSIVES",
]


def caf_v2_possible_felony_charges_cte() -> str:
    """
    Returns a CTE that gets charges imposed in TN,
    with an is_violent flag indicating whether the charge is violent,
    and classification_type to distinguish confirmed felonies from charges with unknown classification.
    Unknown classifications are related to either diversion sentences or interstate compact cases.

    Returns:
        str: SQL CTE as a string
    """
    violent_felony_charges_list = list_to_query_string(
        US_TN_CAF_V2_VIOLENT_FELONY_CHARGES, quoted=True
    )
    return f"""
    possible_felony_charges AS (
        SELECT
            person_id,
            state_code,
            -- Use (original) sentence imposed date, rather than Alternative Sentence Imposed Date, which is ingested as imposed_date in v1 sentences
            DATE(SAFE_CAST(JSON_EXTRACT_SCALAR(sentence_metadata, '$.SENTENCE_IMPOSED_DATE') AS DATETIME)) AS imposed_date,
            description,
            classification_type,
            -- Use list of violent felony charges only for TN felony sentences
            -- Diversion and interstate compact cases matching these descriptions may or may
            -- not be classified as violent, so we only flag confirmed violent felonies here
            description IN ({violent_felony_charges_list}) AND classification_type = "FELONY" AS is_violent_felony,
        FROM `{{project_id}}.sentence_sessions.sentences_and_charges_materialized`
        WHERE 
            state_code = "US_TN"
            -- Exclude misdemeanors, include interstate compact and diversion cases
            AND classification_type IN ("FELONY", "EXTERNAL_UNKNOWN")
    )
    """


def compliant_reporting_offense_type_condition(
    offense_flags: Union[str, List[str]],
) -> str:
    """
    Function that generates the syntax to query charge description for 4 TN-specific offense flag used in compliant
    reporting.

    Params:
    ------
    offense_flags : Union[str, List[str]]
    Name (or list of name) of offense flags. Must be one of "is_violent_domestic", "is_dui", "is_victim_under_18",
    "is_homicide"
    """
    if not isinstance(offense_flags, List):
        offense_flags = [offense_flags]

    lk = {
        "is_violent_domestic": "description LIKE '%DOMESTIC%'",
        "is_dui": "REGEXP_CONTAINS(description, 'DUI|INFLUENCE|DWI')",
        "is_victim_under_18": "((description LIKE '%13%' AND description LIKE '%VICT%') OR description LIKE '%CHILD%')",
        "is_homicide": "REGEXP_CONTAINS(description, 'HOMICIDE|MURD')",
    }
    if len([x for x in offense_flags if x not in lk]) > 0:
        raise ValueError(f"Offense flag must be one of {list(lk)}")

    return " OR ".join([lk[offense_flag] for offense_flag in offense_flags])


# Incident types classified as violent for TN Classification V2 CAF scoring.
# Used in incident_based_caf_score_query_template to determine if an incident is violent.
_US_TN_CLASSIFICATION_V2_VIOLENT_INFRACTION_TYPES = [
    "AOO",  # ASSAULT OFFENDER - WITHOUT WEAPON
    "AOS",  # ASSAULT ON STAFF
    "AOW",  # ASSAULT OFFENDER - WEAPON
    "ASA",  # ASSAULT-STAFF-SERIOUS INJURY
    "ASB",  # ASSAULT-STAFF-INJURY
    "ASC",  # ASSAULT-STAFF-MINOR INJURY
    "ASD",  # ASSAULT-OFN-SERIOUS INJURY
    "ASE",  # ASSAULT-OFN-INJURY
    "ASF",  # ASSAULT-OFN-MINOR INJURY
    "ASG",  # ASSAULT-VIS-SERIOUS INJURY
    "ASH",  # ASSAULT-VIS-INJURY
    "ASI",  # ASSAULT-VIS-MINOR INJURY
    "ASJ",  # ASSAULT STAFF NO INJURY
    "ASK",  # ASSAULT OFFENDER NO INJURY
    "ASL",  # ASSAULT
    "ASM",  # ASSAULT VISITOR NO INJURY
    "ASO",  # ASSAULT STAFF - WITHOUT WEAPON
    "ASW",  # ASSAULT STAFF - WEAPON
    "AVO",  # ASSAULT VISITOR/GUEST - WITHOUT WEAPON
    "AVW",  # ASSAULT VISITOR/GUEST - WEAPON
    "FIG",  # FIGHTING
    "DEG",  # DEATH-STAFF-HOMICIDE (ON DUTY)
    "DEH",  # DEATH-OFN-HOMICIDE
    "DVH",  # DEATH-VISITOR-HOMICIDE
    "HOM",  # HOMICIDE
    "HOS",  # HOSTAGE SITUATION
    "RAP",  # RAPE
    "SXB",  # SEXUAL BATTERY
    "TEM",  # THREATENING EMPLOYEE
    "TOF",  # THREATENING OFFENDER
]


def incident_based_caf_score_query_template(
    score_definitions: dict,
    incident_filter_condition: str,
) -> str:
    """
    Generates a SQL query for calculating CAF (Classification Assessment Form)
    scores based on incarceration incidents for Tennessee's Classification V2 policy.

    This function creates a query that:
    1. Retrieves incarceration incidents within state prison spans
    2. Filters incidents based on the provided condition (e.g., violent vs non-violent)
    3. Counts incidents within configurable time windows (e.g., 0-6 months, 6-12 months)
    4. Calculates scores based on incident counts using the provided score definitions
    5. Aggregates adjacent spans with the same score and incidents list

    The resulting query produces spans with:
    - A total_score representing the sum of scores across all time windows
    - An incidents_list array containing details of each contributing incident

    Args:
        score_definitions: A dictionary mapping time windows to score mappings.
            Keys are tuples of (start_months, end_months) defining the lookback window.
            Values are dictionaries mapping incident counts to scores.
            Example: {
                (0, 6): {0: -1, 1: 0, 2: 1, 3: 2},   # 0-6 months: 0 incidents = -1, 1 = 0, etc.
                (6, 12): {0: 0, 1: 1, 2: 2, 3: 3},   # 6-12 months: different scoring
            }

        incident_filter_condition: A WHERE clause condition to filter which incidents
            are included in the scoring. This should reference columns available in
            the `us_tn_incarceration_incidents_preprocessed` table
            (e.g., "incident_class IN ('A', 'B') AND NOT is_violent").
    """

    def create_case_when_clause(score_mapping: dict, column_name: str) -> str:
        """Creates a CASE WHEN clause to map incident counts to scores."""
        case_when_cases = [
            f"WHEN num_incidents_{column_name} = {k} THEN {v}"
            for k, v in score_mapping.items()
            if k != max(score_mapping.keys())
        ]
        case_when_cases.append(
            f"WHEN num_incidents_{column_name} >= {max(score_mapping.keys())} THEN {score_mapping[max(score_mapping.keys())]}"
        )
        case_when_cases_clause = "\n".join(case_when_cases)
        case_when_clause = (
            f"CASE\n{case_when_cases_clause}\nELSE NULL END AS score_{column_name}"
        )
        return case_when_clause

    case_when_clauses = [
        create_case_when_clause(score_mapping, f"past_{start}_to_{end}_months")
        for (
            start,
            end,
        ), score_mapping in score_definitions.items()
    ]

    case_when_clauses_all = ",\n".join(case_when_clauses)

    incident_count_ctes = ",\n".join(
        [
            f"""incidents_past_{start}_to_{end}_months AS
        (
            WITH {num_events_within_time_interval_spans(
                events_cte="relevant_incidents",
                date_interval=end,
                date_interval_start=start,
                date_part="MONTH",
                index_columns=["person_id", "state_code", "custodial_authority_session_id"],
                event_list_field="incarceration_incident_id",
            )}
            SELECT
                person_id,
                state_code,
                custodial_authority_session_id,
                start_date,
                end_date,
                event_count AS num_incidents_past_{start}_to_{end}_months,
                event_list AS incident_ids_past_{start}_to_{end}_months,
            FROM event_count_spans
        )
        """
            for (start, end) in score_definitions.keys()
        ]
    )

    combined_cte_clauses = []

    for start, end in score_definitions.keys():
        incident_fields = []
        incident_id_fields = []
        for (
            field_start,
            field_end,
        ) in score_definitions.keys():
            if (field_start, field_end) != (start, end):
                incident_fields.append(
                    f"0 AS num_incidents_past_{field_start}_to_{field_end}_months"
                )
                incident_id_fields.append(
                    f"CAST([] AS ARRAY<INT64>) AS incident_ids_past_{field_start}_to_{field_end}_months"
                )
            else:
                incident_fields.append(f"num_incidents_past_{start}_to_{end}_months")
                incident_id_fields.append(f"incident_ids_past_{start}_to_{end}_months")
        incident_fields_all = ",\n".join(incident_fields)
        incident_id_fields_all = ",\n".join(incident_id_fields)
        combined_cte_clauses.append(
            f"""
    SELECT
        person_id,
        state_code,
        custodial_authority_session_id,
        start_date,
        end_date,
        {incident_fields_all},
        {incident_id_fields_all},
    FROM incidents_past_{start}_to_{end}_months
    """
        )

    max_incidents_fields = [
        f"MAX(num_incidents_past_{start}_to_{end}_months) AS num_incidents_past_{start}_to_{end}_months"
        for (start, end) in score_definitions.keys()
    ]

    # Aggregate incident IDs by flattening and re-aggregating arrays from each row
    # We use ARRAY_CONCAT_AGG to combine arrays from overlapping time windows
    max_incident_ids_fields = [
        f"ARRAY_CONCAT_AGG(incident_ids_past_{start}_to_{end}_months) AS incident_ids_past_{start}_to_{end}_months"
        for (start, end) in score_definitions.keys()
    ]

    max_incidents_clause = ",\n".join(max_incidents_fields + max_incident_ids_fields)

    # Build UNION ALL clauses to unnest each time window's incident IDs with their time period label
    # Each clause is a full SELECT from calculated_scores_separate that unnests one time window's IDs
    incident_ids_unnest_clauses = [
        f"""SELECT
            person_id,
            state_code,
            custodial_authority_session_id,
            start_date,
            end_date,
            incarceration_incident_id,
            '{start}-{end} months' AS incident_time_period
        FROM calculated_scores_separate,
        UNNEST(incident_ids_past_{start}_to_{end}_months) AS incarceration_incident_id"""
        for (start, end) in score_definitions.keys()
    ]
    incident_ids_unnest_union = "\n        UNION ALL\n        ".join(
        incident_ids_unnest_clauses
    )

    # Build comma-separated list of incident ID arrays to carry through
    incident_id_arrays_clause = ",\n".join(
        [
            f"incident_ids_past_{start}_to_{end}_months"
            for (start, end) in score_definitions.keys()
        ]
    )

    combined_cte = "\nUNION ALL\n".join(combined_cte_clauses)

    aggregate_score_clause = " + ".join(
        [
            f"score_past_{start}_to_{end}_months"
            for (start, end) in score_definitions.keys()
        ]
    )

    # Build list of violent infraction types
    violent_infraction_types_str = list_to_query_string(
        _US_TN_CLASSIFICATION_V2_VIOLENT_INFRACTION_TYPES,
        quoted=True,
        single_quote=True,
    )

    return f"""
    -- Get all state prison spans to link incidents to specific stays
    WITH state_prison_spans AS (
        SELECT
            person_id,
            state_code,
            custodial_authority_session_id,
            start_date,
            end_date_exclusive,
        FROM `{{project_id}}.sessions.custodial_authority_sessions_materialized`
        WHERE
            state_code = 'US_TN'
            AND custodial_authority = 'STATE_PRISON'
    )
    ,
    -- Get all incidents with relevant metadata, joined to their state prison span
    incidents AS (
        SELECT
            incidents.person_id,
            incidents.state_code,
            state_prison_spans.custodial_authority_session_id,
            incidents.incarceration_incident_id,
            incidents.incident_date,
            incidents.incident_class,  -- Class: A, B, or C
            incidents.infraction_type_raw_text,
            -- Flag whether incident is violent based on the infraction type
            -- Mark *all* Class C incidents as non-violent, because new Class C incidents
            -- will always be marked as non-violent, and there is no retroactive scoring for violent
            -- Class C incidents.
            COALESCE(incidents.infraction_type_raw_text IN (
                {violent_infraction_types_str}
                ), FALSE) AND incidents.incident_class != 'C' AS is_violent
        FROM `{{project_id}}.analyst_data.us_tn_incarceration_incidents_preprocessed_materialized` incidents
        LEFT JOIN state_prison_spans
        ON
            incidents.person_id = state_prison_spans.person_id
            AND incidents.state_code = state_prison_spans.state_code
            AND incidents.incident_date >= state_prison_spans.start_date
            AND incidents.incident_date < {nonnull_end_date_clause('state_prison_spans.end_date_exclusive')}
        WHERE incidents.state_code = "US_TN"
            -- Only include guilty dispositions
            AND incidents.disposition = 'GU'
            -- Exclude verbal warnings
            AND incidents.incident_details NOT LIKE "%VERBAL WARNING%"
            AND incidents.incident_class IS NOT NULL
            AND incidents.incident_date IS NOT NULL
        -- Keep only the most severe incident per person per date
        QUALIFY ROW_NUMBER() OVER(
            PARTITION BY incidents.person_id, incidents.incident_date 
            ORDER BY
                -- Prioritize Class A or B violent incidents
                CASE WHEN 
                    COALESCE(incidents.infraction_type_raw_text IN (
                        {violent_infraction_types_str}
                    ) 
                    AND incidents.incident_class IN ('A', 'B'), FALSE) 
                    THEN 1 
                    ELSE 0 
                    END DESC,
                -- Then prioritize incident class (A, B, C)
                incidents.incident_class, 
                incidents.injury_level DESC, 
                incidents.assault_score DESC,
                -- Then order by infraction type, just to make it more deterministic
                incidents.infraction_type_raw_text,
                -- Then order by incident ID, which is arbitrary but ensures determinism
                incidents.incarceration_incident_id
        ) = 1
    )
    ,
    -- Filter to only the incidents matching the provided filter condition (e.g., violent or non-violent)
    relevant_incidents AS (
        SELECT
            person_id,
            state_code,
            custodial_authority_session_id,
            incarceration_incident_id,
            incident_date AS event_date,
            infraction_type_raw_text,
            incident_class,
        FROM incidents
        WHERE {incident_filter_condition}
    )
    ,
    -- Count incidents within each time window (e.g., 0-6 months, 6-12 months, etc.)
    -- Each time window produces spans where the count changes
    {incident_count_ctes}
    ,
    -- Combine all time window spans into a single CTE for sub-sessionization
    combined_incident_spans AS (
        {combined_cte}
    )
    ,
    -- Break up overlapping spans from different time windows into non-overlapping sub-sessions
    {create_sub_sessions_with_attributes('combined_incident_spans', index_columns=['person_id', 'state_code', 'custodial_authority_session_id'])}
    ,
    -- Deduplicate sub-sessions by taking the max incident count for each time window
    sub_sessions_deduped AS (
        SELECT
            person_id,
            state_code,
            custodial_authority_session_id,
            start_date,
            end_date,
            {max_incidents_clause},
        FROM sub_sessions_with_attributes
        GROUP BY 1, 2, 3, 4, 5
    )
    ,
    -- Calculate the score for each time window based on incident counts
    calculated_scores_separate AS (
        SELECT
            incident_counts.person_id,
            incident_counts.state_code,
            incident_counts.custodial_authority_session_id,
            incident_counts.start_date,
            -- Clip spans to end of stay in state prison
            LEAST(incident_counts.end_date, {nonnull_end_date_clause('state_prison_spans.end_date_exclusive')}) AS end_date,
            -- Calculate score for each time window based on incident counts
            {case_when_clauses_all},
            -- Keep incident ID arrays for each time window
            {incident_id_arrays_clause},
        FROM sub_sessions_deduped incident_counts
        LEFT JOIN state_prison_spans
        USING (person_id, state_code, custodial_authority_session_id)
        -- Omit spans that start after the end of the state prison stay
        WHERE incident_counts.start_date < {nonnull_end_date_clause('state_prison_spans.end_date_exclusive')}
    )
    ,
    -- Unnest incident IDs from each time window with their time period label, then join to incidents
    incident_ids_with_time_period AS (
        {incident_ids_unnest_union}
    )
    ,
    incident_details_unnested AS (
        SELECT
            incident_periods.person_id,
            incident_periods.state_code,
            incident_periods.custodial_authority_session_id,
            incident_periods.start_date,
            incident_periods.end_date,
            incident_periods.incident_time_period,
            relevant_incidents.incarceration_incident_id,
            relevant_incidents.event_date AS incident_date,
            relevant_incidents.infraction_type_raw_text,
            relevant_incidents.incident_class
        FROM incident_ids_with_time_period incident_periods
        INNER JOIN relevant_incidents
            ON incident_periods.incarceration_incident_id = relevant_incidents.incarceration_incident_id
    )
    ,
    -- Aggregate incident details into an array of structs for each span
    incident_details_aggregated AS (
        SELECT
            person_id,
            state_code,
            custodial_authority_session_id,
            start_date,
            end_date,
            TO_JSON(
                ARRAY_AGG(
                    STRUCT(
                        incarceration_incident_id,
                        incident_date,
                        infraction_type_raw_text,
                        incident_class,
                        incident_time_period
                    )
                    ORDER BY incident_date
                )
            ) AS incidents_list
        FROM incident_details_unnested
        GROUP BY 1, 2, 3, 4, 5
    )
    -- Combine scores from all time windows and join incident details
    SELECT
        incident_counts.person_id,
        incident_counts.state_code,
        incident_counts.start_date,
        incident_counts.end_date AS end_date_exclusive,
        -- Sum up the individual score components to get the total score
        {aggregate_score_clause} AS total_score,
        -- Get incident details from the aggregated CTE (empty array if no incidents)
        IFNULL(incident_details_aggregated.incidents_list, TO_JSON([])) AS incidents_list
    FROM calculated_scores_separate incident_counts
    LEFT JOIN incident_details_aggregated
        ON incident_counts.person_id = incident_details_aggregated.person_id
        AND incident_counts.state_code = incident_details_aggregated.state_code
        AND incident_counts.custodial_authority_session_id = incident_details_aggregated.custodial_authority_session_id
        AND incident_counts.start_date = incident_details_aggregated.start_date
        AND incident_counts.end_date = incident_details_aggregated.end_date
    -- Filter out zero-length spans
    WHERE incident_counts.end_date > incident_counts.start_date
    """
