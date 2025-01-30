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
TPR/DTP dates (Transition Program Release/Drug Transition Program)"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AZ_PROJECTED_DATES_VIEW_NAME = "us_az_projected_dates"

US_AZ_PROJECTED_DATES_VIEW_DESCRIPTION = """A view that takes dates in ACIS and projects future CSBD (Community Supervision Begin Dates) &
TPR/DTP dates (Transition Program Release/Drug Transition Program)"""

US_AZ_PROJECTED_DATES_QUERY_TEMPLATE = f"""
    WITH
      csbd_and_ercd_dates AS (
          SELECT
            state_code,
            person_id,
            CAST(FORMAT_DATETIME('%Y-%m-%d', group_update_datetime) AS DATE) AS start_date,
            CAST(FORMAT_DATETIME('%Y-%m-%d', LEAD(group_update_datetime) OVER (PARTITION BY state_code, person_id, sentence_group_id ORDER BY group_update_datetime)) AS DATE) AS end_date,
            parole_eligibility_date_external AS csbd_date,
            projected_full_term_release_date_min_external AS ercd_date,
            CAST(NULL AS DATE) AS acis_tpr_date,
            CAST(NULL AS DATE) AS acis_dtp_date,
            group_update_datetime AS update_datetime,
          FROM
            `{{project_id}}.{{normalized_state_dataset}}.state_sentence_group_length`
          WHERE
            state_code = 'US_AZ'
          ),
      acis_tpr_dates AS (
          SELECT
            state_code,
            person_id,
            DATE_ADD(CAST(FORMAT_DATETIME('%Y-%m-%d', update_datetime) AS DATE), INTERVAL 1 DAY) AS start_date,
            DATE_ADD(CAST(FORMAT_DATETIME('%Y-%m-%d', LEAD(update_datetime) OVER (PARTITION BY state_code, person_id, JSON_EXTRACT_SCALAR(task_metadata, '$.sentence_group_external_id') ORDER BY update_datetime)) AS DATE), INTERVAL 1 DAY) AS end_date,
            CAST(NULL AS DATE) AS csbd_date,
            CAST(NULL AS DATE) AS ercd_date,
            CASE WHEN JSON_EXTRACT(task_metadata, '$.status') != '"DENIED"'
              THEN eligible_date
              ELSE NULL
            END AS acis_tpr_date,
            CAST(NULL AS DATE) AS acis_dtp_date,
            update_datetime AS update_datetime
          FROM
            `{{project_id}}.{{normalized_state_dataset}}.state_task_deadline`
          WHERE
            state_code = 'US_AZ'
            AND task_subtype = 'STANDARD TRANSITION RELEASE' 
          ),
      acis_dtp_dates AS (
          SELECT
            state_code,
            person_id,
            DATE_ADD(CAST(FORMAT_DATETIME('%Y-%m-%d', update_datetime) AS DATE), INTERVAL 1 DAY) AS start_date,
            DATE_ADD(CAST(FORMAT_DATETIME('%Y-%m-%d', LEAD(update_datetime) OVER (PARTITION BY state_code, person_id, JSON_EXTRACT_SCALAR(task_metadata, '$.sentence_group_external_id') ORDER BY update_datetime)) AS DATE), INTERVAL 1 DAY) AS end_date,
            CAST(NULL AS DATE) AS csbd_date,
            CAST(NULL AS DATE) AS ercd_date,
            CAST(NULL AS DATE) AS acis_tpr_date,
            CASE WHEN JSON_EXTRACT(task_metadata, '$.status') != '"DENIED"'
              THEN eligible_date
              ELSE NULL
            END AS acis_dtp_date,
            update_datetime AS update_datetime
          FROM
            `{{project_id}}.{{normalized_state_dataset}}.state_task_deadline`
          WHERE
            state_code = 'US_AZ'
            AND task_subtype = 'DRUG TRANSITION RELEASE' 
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
            acis_tpr_dates
          UNION ALL
          SELECT
            *
          FROM
            acis_dtp_dates 
          ),
      {create_sub_sessions_with_attributes("union_cte")},
      dedup_cte AS (
          SELECT
            *
          FROM
            sub_sessions_with_attributes
          WHERE start_date != {nonnull_end_date_clause('end_date')}
            -- These ensure that the latest update_datetime is used for the given dates, as dates can be updated multiple times within a day
          QUALIFY
            ROW_NUMBER() OVER (PARTITION BY person_id, state_code, start_date, end_date, csbd_date, ercd_date, acis_tpr_date, acis_dtp_date 
                ORDER BY update_datetime DESC) = 1
          ),
      combine_cte AS (
          SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            MAX(csbd_date) AS csbd_date,
            MAX(ercd_date) AS ercd_date,
            MAX(acis_tpr_date) AS acis_tpr_date,
            MAX(acis_dtp_date) AS acis_dtp_date,
          FROM
            dedup_cte
          GROUP BY
            1,2,3,4
          )
    SELECT
      state_code,
      person_id,
      start_date,
      end_date,
      csbd_date,
      -- Formula used: ERCD - 77 Days = CSBD
      DATE_SUB(ercd_date, INTERVAL 77 DAY) AS projected_csbd_date,
      ercd_date,
      acis_tpr_date,
      -- Formula used: CSBD - 90 Days = ERCD - 77 Days = TPR Date
      COALESCE(DATE_SUB(csbd_date, INTERVAL 90 DAY),DATE_SUB(ercd_date, INTERVAL 90 + 77 DAY)) AS projected_tpr_date,
      acis_dtp_date,
      -- Formula used: CSBD - 90 Days = ERCD - 77 Days = DTP Date
      COALESCE(DATE_SUB(csbd_date, INTERVAL 90 DAY),DATE_SUB(ercd_date, INTERVAL 90 + 77 DAY)) AS projected_dtp_date,
    FROM ({aggregate_adjacent_spans(table_name='combine_cte',
          attribute=['csbd_date',
          'ercd_date',
          'acis_tpr_date',
          'acis_dtp_date'])}
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
