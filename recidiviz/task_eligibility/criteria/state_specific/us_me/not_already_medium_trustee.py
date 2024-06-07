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
# =============================================================================

"""
Defines a criteria view that shows spans of time for
which clients are designated "Medium Trustee" status.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ME_NOT_ALREADY_MEDIUM_TRUSTEE"

_DESCRIPTION = """
Defines a criteria view that shows spans of time for
which clients are designated "Medium Trustee" status.
"""

_QUERY_TEMPLATE = f"""
WITH
  medium_trustee_spans AS(
      SELECT
        state_code,
        person_id,
        alert.E_ALERT_DESC AS alert_description,
        SAFE_CAST(LEFT(EFFCT_DATE, 10) AS DATE) AS start_date,
        SAFE_CAST(LEFT(INEFFCT_DATE, 10) AS DATE) AS end_date,
        alert_h.NOTES_TX AS notes_body,
        TRUE AS is_medium_trustee,
      FROM
        `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_102_ALERT_HISTORY_latest` alert_h
      LEFT JOIN
        `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_1020_ALERT_latest` alert
      ON
        alert_h.CIS_1020_ALERT_CD = alert.ALERT_CD
      INNER JOIN
        `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id`
      ON
        alert_h.CIS_100_CLIENT_ID = external_id
        AND id_type = 'US_ME_DOC'
      WHERE
        alert_h.CIS_1020_ALERT_CD = "147" ),
  {create_sub_sessions_with_attributes('medium_trustee_spans')},
  deduped_sub_sessions AS (
      SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        is_medium_trustee,
      FROM
        sub_sessions_with_attributes
      GROUP BY
        1,
        2,
        3,
        4,
        5)
SELECT
  state_code,
  person_id,
  start_date,
  end_date,
  NOT is_medium_trustee AS meets_criteria,
  TO_JSON(STRUCT( start_date AS eligible_date )) AS reason,
  start_date AS eligible_date,
FROM ({aggregate_adjacent_spans(table_name='deduped_sub_sessions',
      attribute=['is_medium_trustee'])})
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="eligible_date",
                type=bigquery.enums.SqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
