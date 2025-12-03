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
"""Create helper SQL queries for Tennessee for use in task eligibility spans."""

from typing import List, Union

from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
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


def compliant_reporting_offense_type_condition(
    offense_flags: Union[str, List[str]]
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
