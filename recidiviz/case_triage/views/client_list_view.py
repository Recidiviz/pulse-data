# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Creates the view builder and view for listing all clients."""

from recidiviz.big_query.selected_columns_big_query_view import SelectedColumnsBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import CASE_TRIAGE_DATASET, DATAFLOW_METRICS_MATERIALIZED_DATASET
from recidiviz.case_triage.views.dataset_config import VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENT_LIST_QUERY_TEMPLATE = """
WITH supervision_start_dates AS (
  SELECT
    person_id,
    state_code,
    supervision_type,
    -- This is MAX to handle the case where a person may have had multiple periods
    -- on supervision. We surmise that there should be no overlapping supervision
    -- periods for a single person under a single supervision type.
    MAX(start_date) AS supervision_start_date
  FROM
    `{project_id}.{dataflow_metrics_materialized_dataset}.most_recent_supervision_start_metrics_materialized`
  GROUP BY
    person_id,
    state_code,
    supervision_type
),
latest_face_to_face AS (
  SELECT
    person_id,
    state_code,
    MAX(most_recent_face_to_face_date) AS most_recent_face_to_face_date
  FROM
    `{project_id}.{dataflow_metrics_materialized_dataset}.most_recent_supervision_case_compliance_metrics_materialized`
  WHERE
    person_external_id IS NOT NULL
  GROUP BY person_id, state_code
)
SELECT
    {columns}
FROM
  `{project_id}.{dataflow_metrics_materialized_dataset}.most_recent_daily_supervision_population_materialized`
LEFT JOIN
  `{project_id}.state.state_person`
USING (person_id, gender, state_code)
-- TODO(#5463): When we ingest employment info, we should replace this joined table with the correct table.
LEFT JOIN
  `{project_id}.{case_triage_dataset}.employment_periods`
USING (person_external_id, state_code)
LEFT JOIN
  `{project_id}.{case_triage_dataset}.latest_assessments`
USING (person_id, state_code)
LEFT JOIN
  latest_face_to_face
USING (person_id, state_code)
LEFT JOIN
  supervision_start_dates
USING (person_id, state_code, supervision_type)
WHERE
  supervision_level IS NOT NULL
  AND supervising_officer_external_id IS NOT NULL
"""

CLIENT_LIST_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id='etl_clients',
    view_query_template=CLIENT_LIST_QUERY_TEMPLATE,
    case_triage_dataset=CASE_TRIAGE_DATASET,
    dataflow_metrics_materialized_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    columns=[
        'state_code',
        'supervising_officer_external_id',
        'person_external_id',
        'full_name',
        'gender',
        'current_address',
        'birthdate',
        'birthdate_inferred_from_age',
        'supervision_start_date',
        'projected_end_date',
        'supervision_type',
        'case_type',
        'supervision_level',
        'employer',
        'most_recent_assessment_date',
        'assessment_score',
        'most_recent_face_to_face_date',
    ],
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENT_LIST_VIEW_BUILDER.build_and_print()
