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
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)

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

COMPLIANT_REPORTING_REFERRAL_DENIAL_CONTACTS = [
    "ACIO",
    "DECF",
    "DECR",
    "DECT",
    "DEDF",
    "DEDU",
    "DEIJ",
    "DEIO",
    "DEIR",
    "REIO",
]

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
# TODO(#71981): Update this to use more flexible matching, not just direct string
# matching.
US_TN_CAF_V2_VIOLENT_FELONY_CHARGES = [
    "2ND DEGREE MURDER- INTENTIONAL KILLING",
    "ADULTERATION OF FOOD, LIQUIDS, MEDS- INTENDS DEATH",
    "ADULTERATION OF FOOD, LIQUIDS, MEDS- INTENDS INJURY",
    "ADULTERATION OF FOOD/LIQUIDS",
    "AGGRAVATED ARSON",
    "AGGRAVATED ASSAULT",
    "AGGRAVATED ASSAULT MINOR VICTIM",
    "AGGRAVATED ASSAULT ON 1ST RESPONDER/ NURSE",
    "AGGRAVATED ASSAULT RESULTING IN DEATH",
    "AGGRAVATED ASSAULT- DEADLY WEAPON",
    "AGGRAVATED ASSAULT- FAILURE TO PROTECT",
    "AGGRAVATED ASSAULT- FIRE GUN FROM CAR",
    "AGGRAVATED ASSAULT- RESTRAINING ORDER IN PLACE",
    "AGGRAVATED ASSAULT- SERIOUS BODILY INJURY",
    "AGGRAVATED ASSAULT- STRANGULATION",
    "AGGRAVATED HUMAN SMUGGLING",
    "AGGRAVATED KIDNAPPING",
    "AGGRAVATED PROSTITUTION",
    "AGGRAVATED PROSTITUTION (HIV)",
    "AGGRAVATED RAPE",
    "AGGRAVATED RAPE OF A CHILD",
    "AGGRAVATED RAPE- BODILY INJURY",
    "AGGRAVATED RAPE- MORE THAN 1 DEFEND.",
    "AGGRAVATED RAPE- WEAPON",
    "AGGRAVATED RIOT",
    "AGGRAVATED ROBBERY",
    "AGGRAVATED ROBBERY- DEADLY WEAPON",
    "AGGRAVATED ROBBERY- SER. BOD. INJURY",
    "AGGRAVATED SEXUAL ASSAULT",
    "AGGRAVATED SEXUAL BATTERY",
    "AGGRAVATED SEXUAL BATTERY- INJURY",
    "AGGRAVATED SEXUAL BATTERY- WEAPON",
    "AGGRAVATED SEXUAL EXPLOITATION OF MINOR",
    "AGGRAVATED SPOUSE RAPE",
    "ASSAULT & BATTERY (FELONY ONLY)",
    "ASSAULT & BATTERY W/INTENT CARNAL KNOWLEDGE",
    "ASSAULT AGAINST PARTICIPANT IN JUDICIAL PROCEEDINGS",
    "ASSAULT FROM AMBUSH",
    "ASSAULT ON LAW ENFORCEMENT OFFICER",
    "ASSAULT W/DEADLY WEAPON",
    "ASSAULT W/I AGGRAVATED KIDNAPPING",
    "ASSAULT W/INTENT MANSLAUGHTER",
    "ASSAULT W/INTENT TO COMMIT FELONY",
    "ASSAULT W/INTENT TO MURDER",
    "ASSAULT W/INTENT TO RAPE",
    "ASSAULT W/INTENT TO ROB",
    "CARJACKING",
    "CIVIL RIGHTS INTIMIDATION - WITH VIOLENCE",
    "CRIMINAL EXPOSURE TO HIV",
    "ESCAPE - TAKE HOSTAGE(S)",
    "ESP AGG KIDNAP- RANSOM OR HOSTAGE",
    "ESP AGG KIDNAP- SEROUS BOD INJURY",
    "ESP AGG KIDNAP- VIC UNDER 13",
    "ESPECIALLY AGG KIDNAP- DEADLY WEAPON",
    "ESPECIALLY AGGRAVATED RAPE",
    "ESPECIALLY AGGRAVATED RAPE OF A CHILD",
    "ESPECIALLY AGGRAVATED ROBBERY",
    "ESPECIALLY AGGRAVATED SEXUAL EXPLOITATION OF A MINOR",
    "GIFTS OF ADULTERATED CANDY/ FOOD",
    "HOME INVASION",
    "HUMAN SMUGGLING",
    "INTIMIDATION – VIOLENCE",
    "KIDNAP - SEXUAL ASSAULT, RANSOM, HOSTAGE,",
    "KIDNAPPING",
    "MAYHEM",
    "MURDER 1",
    "MURDER 2",
    "MURDER BY EXPLOSIVE/ BOMB",
    "MURDER BY TERRORISM",
    "MURDER DURING RAPE",
    "MURDER OF PERSON 70+",
    "PARTICIPATING IN A RIOT",
    "RAPE",
    "RAPE - FRAUD (DATE RAPE)",
    "RAPE - WITH VIOLENCE/FORCE/COERCION",
    "RAPE OF A CHILD",
    "RECKLESS AGGRAVATED ASSAULT",
    "RETALIATION FOR PAST ACTION",
    "ROBBERY - ARMED WITH DEADLY WEAPON",
    "SECOND DEGREE MURDER",
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
    "DEG",  # DEATH-STAFF-HOMICIDE (ON DUTY)
    "DEH",  # DEATH-OFN-HOMICIDE
    "DVH",  # DEATH-VISITOR-HOMICIDE
    "HOM",  # HOMICIDE
    "HOS",  # HOSTAGE SITUATION
    "RAP",  # RAPE
    "SXB",  # SEXUAL BATTERY
]

# Maps each CAF/DCAF/RCAF "generation" to its raw offense severity answer values,
# in ascending severity order. In the CAF, offense severity question is Q3, while
# in RCAF/DCAFs, offense severity question is Q2.
_US_TN_CLASSIFICATION_OFFENSE_SEVERITY_RAW_SCORE_MAP = {
    "CAF": [0, 1, 3, 4],
    "DCAF": [10, 11, 12, 13],
    "DCAF_V2": [10, 11, 12, 14],
    "RCAF": [10, 11, 12, 13],
    "RCAF_V2": [10, 11, 12, 13],
    "RCAF_V3": [10, 12, 13, 14],
}


def _build_offense_severity_lookup_cte() -> str:
    """
    Builds a lookup CTE from _US_TN_CLASSIFICATION_OFFENSE_SEVERITY_RAW_SCORE_MAP that
    maps (offense_severity_lookup_key, raw_score) to a 1-indexed
    offense_severity_row_number.
    """
    rows = []
    for (
        offense_severity_lookup_key,
        raw_scores,
    ) in _US_TN_CLASSIFICATION_OFFENSE_SEVERITY_RAW_SCORE_MAP.items():
        for idx, raw_score in enumerate(raw_scores):
            rows.append(
                f"SELECT '{offense_severity_lookup_key}' AS offense_severity_lookup_key, {raw_score} AS raw_score, {idx + 1} AS offense_severity_row_number"
            )
    return (
        "offense_severity_lookup AS (\n        "
        + "\n        UNION ALL ".join(rows)
        + "\n    )"
    )


def latest_caf_score_cte() -> str:
    """
    Returns CTEs that retrieve each person's latest CAF (Classification Assessment Form)
    offense severity (2026 policy) from assessment_score_sessions_materialized.

    This returns two CTEs:
    - offense_severity_lookup: Maps (offense_severity_lookup_key, raw_score) to offense_severity_row_number
    - CAF_scores: Assessment spans with offense_severity_row_number joined from the lookup

    The offense_severity_row_number is a 1-indexed position representing the severity level,
    allowing downstream code to use a consistent index regardless of assessment type.

    Returns:
        str: SQL CTEs as a string (without the WITH keyword)
    """
    return f"""
    {_build_offense_severity_lookup_cte()}
    ,
    -- pull offense severity from v1 or v2 CAF and deduplicate to pull a single assessment
    -- from each day
    all_CAF_scores AS (
        SELECT
            state_code,
            person_id,
            assessment_date,
            offense_severity_lookup.offense_severity_row_number,
            asmt.assessment_type,
        FROM
            `{{project_id}}.sessions.assessment_score_sessions_materialized` asmt
        LEFT JOIN offense_severity_lookup
            ON CASE
                WHEN asmt.assessment_type = 'CAF' THEN 'CAF'
                WHEN asmt.assessment_type IN ('DCAF', 'RCAF') THEN JSON_EXTRACT_SCALAR(asmt.assessment_metadata,'$.ClassificationType')
            END = offense_severity_lookup.offense_severity_lookup_key
            AND CASE
                WHEN asmt.assessment_type = 'CAF' THEN CAST(NULLIF(JSON_EXTRACT_SCALAR(asmt.assessment_metadata,'$.QUESTION3'), '') AS INT64)
                WHEN asmt.assessment_type IN ('DCAF', 'RCAF') THEN CAST(NULLIF(JSON_EXTRACT_SCALAR(asmt.assessment_metadata,'$.QUESTION2'), '') AS INT64)
            END = offense_severity_lookup.raw_score
        WHERE
            asmt.assessment_type IN ('CAF', 'DCAF', 'RCAF')
            AND state_code = 'US_TN'
            -- Ignore "dummy" 0 scores, used for placement to Max custody
            AND (
                CAST(NULLIF(JSON_EXTRACT_SCALAR(asmt.assessment_metadata,'$.CAFSCORE'), '') AS INT64) != 0
                OR asmt.assessment_level != 'MAXIMUM'
            )
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, assessment_date ORDER BY assessment_score DESC) = 1
    )
    ,
    -- create offense severity score spans
    caf_offense_severity_spans AS (
        SELECT
            state_code,
            person_id,
            offense_severity_row_number,
            assessment_type,
            assessment_date as start_date,
            LEAD(assessment_date) OVER (PARTITION BY person_id ORDER BY assessment_date) AS end_date_exclusive,
        FROM all_CAF_scores
    )
    """


def offense_severity_to_q2_score(
    generation_keys: list[str],
    q2_scores_by_rank: list[int],
) -> str:
    """
    Returns a SQL CASE expression that maps offense_severity_row_number to the
    appropriate q2_score.

    Args:
        generation_keys: The CAF/DCAF/RCAF "generation(s)" (keys in
            _US_TN_CLASSIFICATION_OFFENSE_SEVERITY_RAW_SCORE_MAP) this pricing
            applies to -- more than one when several generations share the same
            scoring (e.g. RCAF and RCAF_V2) -- used only to validate that
            [q2_scores_by_rank] has one entry per severity rank for every generation
            listed.
        q2_scores_by_rank: The point value to award for each offense_severity_row_number,
            in rank order (i.e. q2_scores_by_rank[0] is the score for rank 1, etc).

    Returns:
        str: SQL CASE expression mapping offense_severity_row_number to q2_score
    """
    for generation_key in generation_keys:
        if generation_key not in _US_TN_CLASSIFICATION_OFFENSE_SEVERITY_RAW_SCORE_MAP:
            raise ValueError(
                f"Unknown generation_key: [{generation_key}]. "
                f"Must be one of {list(_US_TN_CLASSIFICATION_OFFENSE_SEVERITY_RAW_SCORE_MAP.keys())}"
            )

        expected_rank_count = len(
            _US_TN_CLASSIFICATION_OFFENSE_SEVERITY_RAW_SCORE_MAP[generation_key]
        )
        if len(q2_scores_by_rank) != expected_rank_count:
            raise ValueError(
                f"Expected {expected_rank_count} q2_scores_by_rank for generation_key "
                f"[{generation_key}], got {len(q2_scores_by_rank)}: {q2_scores_by_rank}"
            )

    when_clauses = [
        f"WHEN offense_severity_row_number = {idx + 1} THEN {score}"
        for idx, score in enumerate(q2_scores_by_rank)
    ]
    return f"CASE {' '.join(when_clauses)} END"


def tn_classification_policy_2026_incidents() -> str:
    """
    Returns a SQL CTE that retrieves TN incarceration incidents with relevant metadata.

    Incidents are prioritized to later keep only the most severe incident per person per date.

    Returns:
        str: SQL CTE string
    """
    violent_infraction_types_str = list_to_query_string(
        _US_TN_CLASSIFICATION_V2_VIOLENT_INFRACTION_TYPES,
        quoted=True,
        single_quote=True,
    )
    return f"""(
        SELECT
            person_id,
            state_code,
            incarceration_incident_id,
            incident_date,
            incident_class,  -- Class: A, B, or C
            infraction_type_raw_text,
            -- Flag whether incident is violent based on the infraction type
            -- Mark *all* Class B/C incidents as non-violent, because new Class B/C incidents
            -- will always be marked as non-violent, and there is no retroactive scoring for violent
            -- Class B/C incidents.
            COALESCE(infraction_type_raw_text IN (
                {violent_infraction_types_str}
                ), FALSE) AND incident_class = 'A' AS is_violent,
            -- Rank incidents by severity within each (person, date), for deduplication.
            -- Lower rank = more severe.
            ROW_NUMBER() OVER (
                PARTITION BY person_id, incident_date
                ORDER BY
                    -- Prioritize Class A violent incidents
                    CASE WHEN
                        COALESCE(infraction_type_raw_text IN (
                            {violent_infraction_types_str}
                        ), FALSE) AND incident_class = 'A'
                        THEN 1 ELSE 0
                        END DESC,
                    -- Then prioritize incident class (A, B, C)
                    incident_class,
                    injury_level DESC,
                    assault_score DESC,
                    -- Then order by infraction type, just to make it more deterministic
                    infraction_type_raw_text,
                    -- Then order by incident ID, which is arbitrary but ensures determinism
                    incarceration_incident_id
            ) AS severity_rank
        FROM `{{project_id}}.analyst_data.us_tn_incarceration_incidents_preprocessed_materialized`
        WHERE state_code = "US_TN"
            -- Only include guilty dispositions
            AND disposition = 'GU'
            -- Exclude verbal warnings
            AND incident_details NOT LIKE "%VERBAL WARNING%"
            AND incident_class IS NOT NULL
            AND incident_date IS NOT NULL
    )"""
