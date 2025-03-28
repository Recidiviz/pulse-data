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
"""A view that takes dates in ACIS and projects future CSBD (Community Supervision Begin Dates) &
TPR/DTP dates (Transition Program Release/Drug Transition Program).

To use this table in a product view, you will need to identify only the currently active
sentences by specifying `end_date IS NULL`.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.calculator.query.state.views.sentence_sessions.person_projected_date_sessions import (
    PERSON_PROJECTED_DATE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AZ_PROJECTED_DATES_VIEW_NAME = "us_az_projected_dates"

US_AZ_PROJECTED_DATES_VIEW_DESCRIPTION = """An AZ specific view extending person_projected_date_sessions to include
additional relevant dates from ACIS and calculates the following projected dates based on AZ sentencing policy:
 - CSBD (Community Supervision Begin Dates)
 - TPR/DTP dates (Transition Program Release/Drug Transition Program)
"""

US_AZ_PROJECTED_DATES_QUERY_TEMPLATE = f"""
    WITH
      csbd_and_ercd_dates AS (
          SELECT DISTINCT
            state_code,
            person_id,
            sentence_group_external_id,
            start_date,
            end_date_exclusive AS end_date,
            group_parole_eligibility_date AS csbd_date,
            group_projected_full_term_release_date_min AS ercd_date,
            group_projected_full_term_release_date_max as sed_date,
            CAST(NULL AS DATE) AS csed_date,
            CAST(NULL AS DATE) AS acis_tpr_date,
            CAST(NULL AS DATE) AS acis_dtp_date,
            TRUE AS has_active_sentence,
          FROM
            `{{project_id}}.{PERSON_PROJECTED_DATE_SESSIONS_VIEW_BUILDER.table_for_query.to_str()}`,
          UNNEST
            -- Unnest the active sentences and pull in the corresponding sentence_group_external_id
            (sentence_array)
          JOIN
            `{{project_id}}.{{normalized_state_dataset}}.state_sentence`
          USING
            (state_code, person_id, sentence_id)
          WHERE
            state_code = 'US_AZ'
      ),
      task_deadline_dates AS (
          -- Pull additional relevant sentence dates ingested within state_task_deadline,
          -- restricted to rows updated within the group serving sessions identified above
          SELECT
            task_deadline.state_code,
            task_deadline.person_id,
            group_dates.sentence_group_external_id,
            DATE_ADD(CAST(FORMAT_DATETIME('%Y-%m-%d', update_datetime) AS DATE), INTERVAL 1 DAY) AS start_date,
            DATE_ADD(CAST(FORMAT_DATETIME('%Y-%m-%d', LEAD(update_datetime) OVER (
              PARTITION BY task_deadline.person_id, group_dates.sentence_group_external_id, task_subtype
              ORDER BY update_datetime)) AS DATE), INTERVAL 1 DAY
            ) AS end_date,
            CAST(NULL AS DATE) AS csbd_date,
            CAST(NULL AS DATE) AS ercd_date,
            CAST(NULL AS DATE) AS sed_date,
            -- Parse out the additional relevant sentence group dates
            IF(task_subtype = 'COMMUNITY SUPERVISION END DATE', due_date, NULL) AS csed_date,
            IF(
              task_subtype = 'STANDARD TRANSITION RELEASE' AND JSON_VALUE(task_metadata, '$.status') != 'DENIED',
              eligible_date,
              NULL
            ) AS acis_tpr_date,
            IF(
              task_subtype = 'DRUG TRANSITION RELEASE' AND JSON_VALUE(task_metadata, '$.status') != 'DENIED',
              eligible_date,
              NULL
            ) AS acis_dtp_date,
            CAST(NULL AS BOOL) AS has_active_sentence,
          FROM
            `{{project_id}}.{{normalized_state_dataset}}.state_task_deadline` task_deadline
          JOIN
            csbd_and_ercd_dates group_dates
          ON
            group_dates.state_code = task_deadline.state_code
            AND group_dates.person_id = task_deadline.person_id
            AND group_dates.sentence_group_external_id = JSON_VALUE(task_deadline.task_metadata, '$.sentence_group_external_id')
            AND DATE_ADD(CAST(FORMAT_DATETIME('%Y-%m-%d', task_deadline.update_datetime) AS DATE), INTERVAL 1 DAY)
                BETWEEN group_dates.start_date AND {nonnull_end_date_exclusive_clause("group_dates.end_date")}
          WHERE
            task_subtype IN (
              'COMMUNITY SUPERVISION END DATE',
              'STANDARD TRANSITION RELEASE',
              'DRUG TRANSITION RELEASE'
            )
      ),
      union_cte AS (
          SELECT
            *
          FROM
            csbd_and_ercd_dates
          UNION ALL
          SELECT
            *
          FROM
            task_deadline_dates
      ),
      {create_sub_sessions_with_attributes("union_cte", index_columns=["person_id", "state_code"])},
      combine_cte AS (
          SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            MAX(csed_date) AS csed_date,
            MAX(csbd_date) AS csbd_date,
            MAX(ercd_date) AS ercd_date,
            MAX(sed_date) AS sed_date,
            MAX(acis_tpr_date) AS acis_tpr_date,
            MAX(acis_dtp_date) AS acis_dtp_date,
            LOGICAL_OR(has_active_sentence) AS has_active_sentence
          FROM
            sub_sessions_with_attributes
          WHERE start_date != {nonnull_end_date_clause('end_date')}
          GROUP BY
            1,2,3,4
          -- Only keep spans that overlap with an active sentence period
          HAVING has_active_sentence
      )
    SELECT
      state_code,
      person_id,
      start_date,
      end_date,
      csed_date,
      csbd_date,
      -- Formula used: ERCD - 77 Days = CSBD
      DATE_SUB(ercd_date, INTERVAL 77 DAY) AS projected_csbd_date,
      ercd_date,
      sed_date,
      acis_tpr_date,
      -- Formula used: CSBD - 90 Days = ERCD - 77 Days = TPR Date
      COALESCE(DATE_SUB(csbd_date, INTERVAL 90 DAY),DATE_SUB(ercd_date, INTERVAL 90 + 77 DAY)) AS projected_tpr_date,
      acis_dtp_date,
      -- Formula used: CSBD - 90 Days = ERCD - 77 Days = DTP Date
      COALESCE(DATE_SUB(csbd_date, INTERVAL 90 DAY),DATE_SUB(ercd_date, INTERVAL 90 + 77 DAY)) AS projected_dtp_date,
    FROM ({aggregate_adjacent_spans(table_name='combine_cte',
          attribute=['csed_date', 
          'csbd_date',
          'ercd_date',
          'sed_date',
          'acis_tpr_date',
          'acis_dtp_date'],
          index_columns=["person_id", "state_code"])}
    )
"""


US_AZ_PROJECTED_DATES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_AZ_PROJECTED_DATES_VIEW_NAME,
    description=US_AZ_PROJECTED_DATES_VIEW_DESCRIPTION,
    view_query_template=US_AZ_PROJECTED_DATES_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AZ_PROJECTED_DATES_VIEW_BUILDER.build_and_print()
