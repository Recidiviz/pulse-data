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
"""
Defines a criteria span view that shows periods of time during which someone is 
within 18 months of their Earliest Possible Release Date (EPRD) and has not yet 
passed that date.

The EPRD is the earliest of the following dates:
 - the Parole Eligibility Date (PED)
 - the Tentative Parole Date (TPD) if this date is past the PED
 - the Parole Hearing Date (PHD) if this date is past the PED and there's no TPD
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SENTENCE_SESSIONS_V2_ALL_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_INCARCERATION_WITHIN_18_MONTHS_OF_EPRD"

_DESCRIPTION = """
Defines a criteria span view that shows periods of time during which someone is 
within 18 months of their Earliest Possible Release Date (EPRD) and has not yet 
passed that date.

The EPRD is the earliest of the following dates:
 - the Parole Eligibility Date (PED)
 - the Tentative Parole Date (TPD) if this date is past the PED
 - the Parole Hearing Date (PHD) if this date is past the PED and there's no TPD
"""

_QUERY_TEMPLATE = f"""
WITH ped_spans AS (
    SELECT
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date_exclusive AS end_date,
        span.group_parole_eligibility_date AS parole_eligibility_date,
    FROM `{{project_id}}.sentence_sessions_v2_all.person_projected_date_sessions_materialized` span
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sess
        ON span.state_code = sess.state_code
        AND span.person_id = sess.person_id
        -- Restrict to spans that overlap with particular compartment levels
        AND compartment_level_1 = 'INCARCERATION'
        -- Use strictly less than for exclusive end_dates
        AND span.start_date < {nonnull_end_date_clause('sess.end_date_exclusive')}
        AND sess.start_date < {nonnull_end_date_clause('span.end_date_exclusive')}
    WHERE span.state_code = 'US_IX'
),
tpd_spans AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive as end_date,
        group_projected_parole_release_date AS tentative_parole_date,
      FROM
        `{{project_id}}.{{sentence_sessions_v2_dataset}}.person_projected_date_sessions_materialized`
      WHERE state_code = "US_IX"
        AND group_projected_parole_release_date IS NOT NULL 
),
ped_tpd_phd_spans AS (
    -- Periods of time that fall within 18 months of the PED.
    -- The end_date does not go past the PED.
    SELECT 
        state_code,
        person_id,
        GREATEST(start_date,
                DATE_SUB(parole_eligibility_date, INTERVAL 18 MONTH)) AS start_date,
        LEAST({nonnull_end_date_clause('end_date')}, parole_eligibility_date) AS end_date,
        parole_eligibility_date,
        NULL AS tentative_parole_date,
        NULL AS parole_hearing_date,
    FROM ped_spans

    UNION ALL

    -- Periods of time that fall within 18 months of the TPD.
    -- The end_date does not go past the TPD. 
    -- We only consider periods where the TPD is past the PED.
    SELECT 
        ped.state_code,
        ped.person_id,
        GREATEST(ped.start_date,
                DATE_SUB(pds.tentative_parole_date, INTERVAL 18 MONTH)) AS start_date,
        LEAST({nonnull_end_date_clause('ped.end_date')}, pds.tentative_parole_date) AS end_date,
        NULL AS parole_eligibility_date,
        pds.tentative_parole_date,
        NULL AS parole_hearing_date,
    FROM ped_spans ped
    LEFT JOIN tpd_spans pds
        USING(state_code, person_id)
    WHERE ped.parole_eligibility_date < pds.tentative_parole_date

    UNION ALL

    -- Periods of time that fall within 18 months of the initial PHD.
    -- The end_date does not go past the PHD. 
    -- We only consider periods where the PHD is past the PED.
    SELECT 
        ped.state_code,
        ped.person_id,
        GREATEST(ped.start_date,
                DATE_SUB(pds.initial_parole_hearing_date, INTERVAL 18 MONTH)) AS start_date,
        LEAST({nonnull_end_date_clause('ped.end_date')}, pds.initial_parole_hearing_date) AS end_date,
        NULL AS parole_eligibility_date,
        NULL AS tentative_parole_date,
        pds.initial_parole_hearing_date AS parole_hearing_date,
    FROM ped_spans ped
    LEFT JOIN `{{project_id}}.{{analyst_dataset}}.us_ix_parole_dates_spans_preprocessing_materialized` pds
        USING(state_code, person_id)
    WHERE ped.parole_eligibility_date < pds.initial_parole_hearing_date

    UNION ALL

    -- Periods of time that fall within 18 months of the next PHD.
    -- The end_date does not go past the PHD. 
    -- We only consider periods where the PHD is past the PED.
    SELECT 
        ped.state_code,
        ped.person_id,
        GREATEST(ped.start_date,
                DATE_SUB(pds.next_parole_hearing_date, INTERVAL 18 MONTH)) AS start_date,
        LEAST({nonnull_end_date_clause('ped.end_date')}, pds.next_parole_hearing_date) AS end_date,
        NULL AS parole_eligibility_date,
        NULL AS tentative_parole_date,
        pds.next_parole_hearing_date AS parole_hearing_date,
    FROM ped_spans ped
    LEFT JOIN `{{project_id}}.{{analyst_dataset}}.us_ix_parole_dates_spans_preprocessing_materialized` pds
        USING(state_code, person_id)
    WHERE ped.parole_eligibility_date < pds.next_parole_hearing_date
),
{create_sub_sessions_with_attributes(
    table_name="ped_tpd_phd_spans"
)},
grouped_sub_sessions AS (
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    True AS meets_criteria,
    MIN(parole_eligibility_date) AS parole_eligibility_date, 
    MIN(tentative_parole_date) AS tentative_parole_date, 
    MIN(parole_hearing_date) AS parole_hearing_date,
    MIN(LEAST(  
        {nonnull_end_date_clause('parole_eligibility_date')},  
        {nonnull_end_date_clause('tentative_parole_date')},  
        {nonnull_end_date_clause('parole_hearing_date')}  
    )) AS earliest_possible_release_date 
FROM sub_sessions_with_attributes
WHERE start_date != {nonnull_end_date_clause('end_date')}
GROUP BY 1,2,3,4
)
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    meets_criteria,
    TO_JSON(STRUCT(parole_eligibility_date,
                tentative_parole_date,
                parole_hearing_date,
                earliest_possible_release_date)) AS reason,
    parole_eligibility_date,
    tentative_parole_date,
    parole_hearing_date,
    earliest_possible_release_date
FROM grouped_sub_sessions
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    sentence_sessions_v2_dataset=SENTENCE_SESSIONS_V2_ALL_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    state_code=StateCode.US_IX,
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
            name="earliest_possible_release_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Earliest Possible Release Date (EPRD): The earliest possible date on which the person can be released from incarceration.",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
