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
"""Helper SQL queries and criteria view builders for Idaho release planning,
covering the Earliest Possible Release Date (EPRD) and release districts."""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SENTENCE_SESSIONS_DATASET,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_release_district_spans import (
    CRC_REQUEST_NOTE_DATE_COLUMN_NAME,
    CRC_REQUEST_RELEASE_DISTRICT_COLUMN_NAME,
    RELEASE_DISTRICT_COLUMN_NAME,
    US_IX_RELEASE_DISTRICT_SPANS_VIEW_NAME,
    UsIxReleaseDistrict,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.observations.span_observation_big_query_view_builder import fix_indent
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)


def eprd_cte() -> str:
    """
    Defines a CTE that calculates the Earliest Possible Release Date (EPRD) for individuals in Idaho.

    The EPRD is calculated as specified below:
        EPRD = TPD, if the TPD exists
        EPRD = FTRD, if both TPD and PHD do not exist
        EPRD = PED, if TPD does not exist, the PHD is prior to the PED, and the PED after today.
        EPRD = PHD, if TPD does not exist and PHD is after PED.
    """

    return f"""ped_tpd_ftrd_spans AS (
SELECT
    span.state_code,
    span.person_id,
    span.start_date,
    span.end_date_exclusive AS end_date,
    span.group_parole_eligibility_date AS parole_eligibility_date,
    span.group_projected_parole_release_date AS tentative_parole_date,
    span.group_projected_full_term_release_date_max AS full_term_release_date
FROM `{{project_id}}.{{sentence_sessions_v2_dataset}}.person_projected_date_sessions_materialized` span
WHERE span.state_code = 'US_IX'
),
ped_tpd_phd_ftrd_spans AS (
SELECT
    state_code,
    person_id,
    start_date,
    -- Only retain PEDs in the future
    LEAST({nonnull_end_date_clause('end_date')}, parole_eligibility_date) AS end_date,
    parole_eligibility_date,
    NULL AS tentative_parole_date,
    NULL AS parole_hearing_date,
    NULL AS full_term_release_date
FROM ped_tpd_ftrd_spans
WHERE parole_eligibility_date IS NOT NULL
  -- Avoid zero-day spans when parole_eligibility_date <= start_date
  AND parole_eligibility_date > start_date

UNION ALL

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    NULL AS parole_eligibility_date,
    tentative_parole_date,
    NULL AS parole_hearing_date,
    full_term_release_date
FROM ped_tpd_ftrd_spans ped

UNION ALL

SELECT 
    state_code,
    person_id,
    start_date,
    end_date_exclusive AS end_date,
    NULL AS parole_eligibility_date,
    NULL AS tentative_parole_date,
    pds.initial_parole_hearing_date AS parole_hearing_date,
    NULL AS full_term_release_date
FROM `{{project_id}}.{{analyst_dataset}}.us_ix_parole_dates_spans_preprocessing_materialized` pds

UNION ALL

SELECT 
    pds.state_code,
    pds.person_id,
    start_date,
    end_date_exclusive AS end_date,
    NULL AS parole_eligibility_date,
    NULL AS tentative_parole_date,
    pds.next_parole_hearing_date AS parole_hearing_date,
    NULL AS full_term_release_date
FROM `{{project_id}}.{{analyst_dataset}}.us_ix_parole_dates_spans_preprocessing_materialized` pds
),
{create_sub_sessions_with_attributes(
table_name="ped_tpd_phd_ftrd_spans"
)},
grouped_sub_sessions AS (
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    MIN(parole_eligibility_date) AS parole_eligibility_date, 
    MIN(tentative_parole_date) AS tentative_parole_date, 
    --take the MIN of all future parole hearing dates, or if none exist, the MAX of all past parole hearing dates
    --this only works based on the subsessionizing done in us_ix_parole_dates_spans_preprocessing_materialized
    COALESCE(
        MIN(IF(parole_hearing_date > start_date, parole_hearing_date, NULL)),
        MAX(parole_hearing_date)
    ) AS parole_hearing_date,
    MIN(full_term_release_date) AS full_term_release_date,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
),
eprd_sessions AS (
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    parole_eligibility_date,
    tentative_parole_date,
    parole_hearing_date,
    full_term_release_date,
    CASE
        WHEN tentative_parole_date IS NOT NULL THEN tentative_parole_date
        WHEN parole_hearing_date IS NULL THEN full_term_release_date
        WHEN parole_eligibility_date > parole_hearing_date THEN parole_eligibility_date
        WHEN (parole_hearing_date >= parole_eligibility_date OR parole_eligibility_date IS NULL) THEN parole_hearing_date
        ELSE NULL
    END AS earliest_possible_release_date
FROM grouped_sub_sessions
)"""


def eprd_within_x_months_criteria_view_builder(
    *,
    criteria_name: str,
    description: str,
    num_months: int,
    meets_criteria_bool: bool = True,
    meets_criteria_default: bool = False,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a US_IX criteria view builder for whether someone is (or, when
    meets_criteria_bool is False, is not) within `num_months` of their upcoming
    Earliest Possible Release Date (EPRD) and has not yet passed that date.

    Args:
        criteria_name (str): Name of the criteria.
        description (str): Description of the criteria.
        num_months (int): Number of months to use for the leading window time.
        meets_criteria_bool (bool): Whether spans meet or do not meet the criteria.
            Defaults to True.
        meets_criteria_default (bool): meets_criteria value to use for periods the query
            produces no span for (e.g. when there is no upcoming EPRD). Defaults to
            False.
    """
    meets_criteria_condition = "" if meets_criteria_bool else "NOT"
    query_template = fix_indent(
        f"""
    WITH {eprd_cte()},
    critical_date_spans AS (
        SELECT
            state_code,
            person_id,
            start_date AS start_datetime,
            --crop spans so that EPRDs are always upcoming
            LEAST({nonnull_end_date_clause('end_date')}, earliest_possible_release_date) AS end_datetime,
            earliest_possible_release_date AS critical_date,
            parole_eligibility_date,
            tentative_parole_date,
            parole_hearing_date,
            full_term_release_date,
            earliest_possible_release_date
        FROM eprd_sessions
        --only include spans where EPRD is upcoming
        WHERE earliest_possible_release_date > start_date
    ),
    {critical_date_has_passed_spans_cte(meets_criteria_leading_window_time=num_months,
                                        attributes=["parole_eligibility_date",
                                                    "tentative_parole_date",
                                                    "parole_hearing_date",
                                                    "full_term_release_date",
                                                    "earliest_possible_release_date"],
                                        date_part="MONTH")}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        {meets_criteria_condition} critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(parole_eligibility_date,
                    tentative_parole_date,
                    parole_hearing_date,
                    full_term_release_date,
                    earliest_possible_release_date)) AS reason,
        parole_eligibility_date,
        tentative_parole_date,
        parole_hearing_date,
        full_term_release_date,
        earliest_possible_release_date
    FROM critical_date_has_passed_spans
    WHERE start_date IS DISTINCT FROM end_date""",
        indent_level=8,
    )
    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        criteria_spans_query_template=query_template,
        description=description,
        sentence_sessions_v2_dataset=SENTENCE_SESSIONS_DATASET,
        analyst_dataset=ANALYST_VIEWS_DATASET,
        state_code=StateCode.US_IX,
        meets_criteria_default=meets_criteria_default,
        reasons_fields=[
            ReasonsField(
                name="parole_eligibility_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Parole Eligibility Date (PED): The date on which the person becomes eligible for parole.",
            ),
            ReasonsField(
                name="tentative_parole_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Tentative Parole Date (TPD): The date on which the person is tentatively scheduled for parole.",
            ),
            ReasonsField(
                name="parole_hearing_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Parole Hearing Date (PHD): The date on which the person is scheduled for a parole hearing.",
            ),
            ReasonsField(
                name="full_term_release_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Full Term Release Date (FTRD): The maximum full term release date.",
            ),
            ReasonsField(
                name="earliest_possible_release_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Earliest Possible Release Date (EPRD): The earliest possible date on which the person can be released from incarceration.",
            ),
        ],
    )


def _release_district_criteria_merge_column_names(
    *, release_district_column_name: str
) -> list[str]:
    """Returns the us_ix_release_district_spans columns whose changes start a new
    criteria span when the criteria reads release_district_column_name."""
    if release_district_column_name == CRC_REQUEST_RELEASE_DISTRICT_COLUMN_NAME:
        # Keeps consecutive notes naming the same district as separate spans.
        return [
            CRC_REQUEST_RELEASE_DISTRICT_COLUMN_NAME,
            CRC_REQUEST_NOTE_DATE_COLUMN_NAME,
        ]
    if release_district_column_name == RELEASE_DISTRICT_COLUMN_NAME:
        return [RELEASE_DISTRICT_COLUMN_NAME]
    raise ValueError(
        f"Unexpected release_district_column_name [{release_district_column_name}]"
    )


def release_district_criteria_query(
    *,
    release_districts: list[UsIxReleaseDistrict],
    release_district_column_name: str,
) -> str:
    """Returns a criteria query over us_ix_release_district_spans that meets the
    criteria while the district in release_district_column_name is one of
    release_districts, with no span while that column is NULL.

    For example, with release_districts [DISTRICT_3, DISTRICT_4] and
    release_district_column_name crc_request_release_district, a resident with a D4
    note on 2025-06-03 and an ISC note on 2025-09-15 gets (columns abridged):

        start_date,end_date,meets_criteria,release_district
        2025-06-03,2025-09-15,true,4
        2025-09-15,,false,ISC

    Args:
        release_districts: The districts that meet the criteria.
        release_district_column_name: The view column to read, either
            crc_request_release_district (one span per CRC Request note) or
            release_district (the CRC note, home address or sentencing county
            district, whichever is set first).
    """
    source_column_names = _release_district_criteria_merge_column_names(
        release_district_column_name=release_district_column_name
    )
    release_districts_str = list_to_query_string(
        [district.value for district in release_districts],
        quoted=True,
        single_quote=True,
    )

    # Columns this criteria doesn't read split the view's spans, so the spans are
    # merged back on the columns it does read.
    merged_spans_query = aggregate_adjacent_spans(
        table_name="release_district_spans",
        attribute=[*source_column_names, "release_district_code"],
        end_date_field_name="end_date_exclusive",
    )

    return f"""
WITH release_district_spans AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        {list_to_query_string(source_column_names)},
        -- Turns DISTRICT 3 into 3 and leaves ISC as is.
        REGEXP_EXTRACT({release_district_column_name}, r'ISC|\\d+') AS release_district_code,
    FROM `{{project_id}}.{ANALYST_VIEWS_DATASET}.{US_IX_RELEASE_DISTRICT_SPANS_VIEW_NAME}_materialized`
    WHERE {release_district_column_name} IS NOT NULL
),
merged_release_district_spans AS (
{fix_indent(merged_spans_query, indent_level=4)}
)
SELECT
    state_code,
    person_id,
    start_date,
    end_date_exclusive AS end_date,
    {release_district_column_name} IN ({release_districts_str}) AS meets_criteria,
    TO_JSON(STRUCT(release_district_code AS release_district)) AS reason,
    release_district_code AS release_district,
FROM merged_release_district_spans
"""
