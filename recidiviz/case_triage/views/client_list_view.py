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
from recidiviz.calculator.query.state.dataset_config import CASE_TRIAGE_DATSET
from recidiviz.case_triage.views.dataset_config import VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENT_LIST_QUERY_TEMPLATE = """
WITH
  most_recent_client_dates AS (
  SELECT
    supervising_officer_external_id,
    po_report_recipients.state_code,
    MAX(date_of_supervision) AS date_of_supervision
  FROM
    `{project_id}.dataflow_metrics_materialized.most_recent_supervision_population_metrics_materialized` most_recent_metrics
  LEFT JOIN
    `{project_id}.static_reference_tables.po_report_recipients` po_report_recipients
  ON
    po_report_recipients.state_code = most_recent_metrics.state_code
    AND po_report_recipients.officer_external_id = most_recent_metrics.supervising_officer_external_id
  GROUP BY
    supervising_officer_external_id,
    state_code )
SELECT
  {columns}
FROM
  most_recent_client_dates
INNER JOIN
  `{project_id}.dataflow_metrics_materialized.most_recent_supervision_population_metrics_materialized`
USING
  (supervising_officer_external_id,
    state_code,
    date_of_supervision)
LEFT JOIN
  `{project_id}.state.state_person`
USING
  (person_id,
    state_code)
-- TODO(#5463): When we ingest employment info, we should replace this joined table with the correct table.
LEFT JOIN
  `{project_id}.{case_triage_dataset}.employment_periods`
USING (person_external_id, state_code)
WHERE
  supervision_level IS NOT NULL
"""

CLIENT_LIST_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id='etl_clients',
    view_query_template=CLIENT_LIST_QUERY_TEMPLATE,
    case_triage_dataset=CASE_TRIAGE_DATSET,
    columns=[
        'supervising_officer_external_id',
        'person_external_id',
        'full_name',
        'current_address',
        'birthdate',
        'birthdate_inferred_from_age',
        'supervision_type',
        'case_type',
        'supervision_level',
        'state_code',
        'employer',
    ],
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENT_LIST_VIEW_BUILDER.build_and_print()
