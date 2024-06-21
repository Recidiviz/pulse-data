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
# ============================================================================
"""Describes the spans of time when a TN client on Probation has no zero-tolerance codes recorded
within their sentence span. Probation clients with zero-tolerance codes recorded have a high
likelihood of missing ingested sentencing data in TN. Parole clients are excluded as they do not
have missing sentence data issues.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_NO_ZERO_TOLERANCE_CODES_SPANS"

_DESCRIPTION = """Describes the spans of time when a TN client on Probation has no zero-tolerance codes recorded
within their sentence span. Probation clients with zero-tolerance codes recorded have a high
likelihood of missing ingested sentencing data in TN. Parole clients are excluded as they do not
have missing sentence data issues.
"""

_QUERY_TEMPLATE = f"""
    WITH critical_date_spans AS (
        /* This CTE uses sentence spans to get date imposed for each unique sentence being served during each span,
        and brings in zero tolerance codes that occurred after each date imposed. This allows us to create spans
        of time when the critical date (zero tolerance contact code date) has passed */
        SELECT
            sentences.person_id,
            sentences.state_code,
            sentences.date_imposed AS start_datetime,
            /* Setting latest sentence span end date to NULL so that if the zero tolerance code occurred after the
                latest sentence start, we're still flagging that as critical date has passed since it signals
                potentially missing sentencing data */
            IF(
                MAX({nonnull_end_date_exclusive_clause('sentences.projected_completion_date_max')}) 
                    OVER(PARTITION BY sentences.person_id) = {nonnull_end_date_exclusive_clause('sentences.projected_completion_date_max')},
                NULL,
                sentences.projected_completion_date_max
            ) end_datetime,
            contact_date AS critical_date,
        FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sentences
        LEFT JOIN `{{project_id}}.{{analyst_dataset}}.us_tn_zero_tolerance_codes_materialized` contact
          ON sentences.person_id = contact.person_id
          AND contact_date > sentences.date_imposed
        WHERE sentences.state_code = 'US_TN'
          AND {nonnull_end_date_exclusive_clause('projected_completion_date_max')} > date_imposed
    ),
    /* The critical_date_has_passed_spans_cte() method creates spans of time when the critical date
    (zero tolerance contact code date) has passed. The output has overlapping and non-collapsed adjacent spans.
    The create_sub_sessions_with_attributes() helper takes that input and outputs non-overlapping spans (i.e there
    may be multiple rows for a given start/end date which preserve input attributes. The final step is to collapse
    those */
    {critical_date_has_passed_spans_cte()},
    union_supervision_type AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            critical_date_has_passed,
            critical_date,
            NULL AS supervision_type
        FROM critical_date_has_passed_spans

        UNION ALL

        SELECT
            state_code,
            person_id,
            start_date,
            termination_date AS end_date,
            NULL AS critical_date_has_passed,
            NULL AS critical_date,
            CASE WHEN supervision_type_raw_text LIKE '%PAO%' THEN 'PAROLE'
                 ELSE supervision_type END AS supervision_type, 
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` sp
        WHERE sp.state_code = 'US_TN'
        AND {nonnull_end_date_exclusive_clause('sp.termination_date')} > start_date
    ),
    {create_sub_sessions_with_attributes('union_supervision_type')},
    grouped_dates AS (
         SELECT
            DISTINCT
            person_id,
            state_code,
            start_date,
            end_date,
            -- Someone meets the criteria if there are no zero tolerance contact codes in a given span
            -- LOGICAL_OR(critical_date_has_passed) AS critical_date_has_passed,
            COALESCE(FIRST_VALUE(critical_date_has_passed) OVER(critical_date_window), FALSE) AS critical_date_has_passed,
            FIRST_VALUE(supervision_type) OVER(sup_type_window) AS supervision_type,
        FROM sub_sessions_with_attributes
        WINDOW sup_type_window AS (
          PARTITION BY state_code, person_id, start_date, end_date
            ORDER BY CASE WHEN supervision_type = 'PAROLE' THEN 0 
                          WHEN supervision_type IS NOT NULL THEN 1
                          ELSE 2 END
        ),
        critical_date_window AS (
          PARTITION BY state_code, person_id, start_date, end_date
            ORDER BY CASE WHEN critical_date_has_passed THEN 0 ELSE 1 END
        )

    ),
    reason_blob AS (
        SELECT
            person_id,
            state_code,
            start_date,
            end_date,
            TO_JSON(
              STRUCT(
                ARRAY_AGG(
                  IF(critical_date >= end_date, NULL, critical_date) IGNORE NULLS ORDER BY critical_date
                ) AS zero_tolerance_code_dates
              )
            ) AS reason,
            ARRAY_AGG(
                IF(critical_date >= end_date, NULL, critical_date) IGNORE NULLS ORDER BY critical_date
            ) AS zero_tolerance_code_dates,
        FROM sub_sessions_with_attributes
        GROUP BY 1,2,3,4
    )
    SELECT
        g.state_code,
        g.person_id,
        g.start_date,
        IF(
            MAX({nonnull_end_date_exclusive_clause('g.end_date')}) OVER(PARTITION BY g.person_id) = {nonnull_end_date_exclusive_clause('g.end_date')},
            NULL,
            g.end_date
        ) AS end_date,
        -- Someone meets the criteria if there are no zero tolerance contact codes in a given span, unless they are on parole
        CASE WHEN supervision_type = 'PAROLE' THEN TRUE ELSE NOT critical_date_has_passed END AS meets_criteria,
        CASE WHEN supervision_type = 'PAROLE' THEN NULL ELSE reason END AS reason,
        CASE WHEN supervision_type = 'PAROLE' THEN NULL ELSE zero_tolerance_code_dates END AS zero_tolerance_code_dates,
    FROM grouped_dates g
    LEFT JOIN reason_blob r
        ON g.state_code = r.state_code
        AND g.person_id = r.person_id
        AND g.start_date = r.start_date
        AND {nonnull_end_date_exclusive_clause('g.end_date')} = {nonnull_end_date_exclusive_clause('r.end_date')} 
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        analyst_dataset=ANALYST_VIEWS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="zero_tolerance_code_dates",
                type=bigquery.enums.SqlTypeNames.RECORD,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
