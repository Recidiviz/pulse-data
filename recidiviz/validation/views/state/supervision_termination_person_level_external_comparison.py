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

"""A view comparing person-level supervision termination metrics to the person-level values from external metrics
provided by the state.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SUPERVISION_TERMINATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_NAME = (
    "supervision_termination_person_level_external_comparison"
)

SUPERVISION_TERMINATION_PERSON_LEVEL_EXTERNAL_COMPARISON_DESCRIPTION = """
Comparison of internal and external lists of supervision terminations.
"""

SUPERVISION_TERMINATION_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE = """
WITH external_data AS (
  -- NOTE: You can replace this part of the query with your own query to test the SELECT query you will use to
  -- generate data to insert into the `supervision_termination_person_level` table.
  SELECT state_code, person_external_id, termination_date
  FROM `{project_id}.{external_accuracy_dataset}.supervision_termination_person_level_materialized`
), external_data_with_ids AS (
  SELECT
    external_data.state_code,
    termination_date,
    external_data.person_external_id,
    COALESCE(CAST(person_id AS STRING), 'UNKNOWN_PERSON') as person_id 
  FROM external_data
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` all_state_person_ids
  ON external_data.state_code = all_state_person_ids.state_code AND external_data.person_external_id = all_state_person_ids.external_id
  -- Limit to supervision IDs in states that have multiple
  AND (external_data.state_code != 'US_ND' OR id_type = 'US_ND_SID')
  AND (external_data.state_code != 'US_PA' OR id_type = 'US_PA_PBPP')
), internal_data AS (
  SELECT
    state_code,
    termination_date,
    person_external_id,
    CAST(person_id AS STRING) AS person_id
  FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_termination_metrics_materialized`
  -- TODO(#5422): Remove termination_reason filter once we have removed transfers from events
  -- Transfers terminations and returns from absconsion are not included in the external termination data
  WHERE termination_reason NOT IN ('RETURN_FROM_ABSCONSION', 'TRANSFER_WITHIN_STATE', 'TRANSFER_TO_OTHER_JURISDICTION')
    AND (state_code <> 'US_PA' OR termination_reason <> 'ABSCONSION')
  AND supervision_type != 'INVESTIGATION'
), internal_metrics_for_valid_regions_and_dates AS (
  SELECT * FROM
  -- Only compare regions and months for which we have external validation data
  (SELECT DISTINCT state_code, termination_date FROM external_data)
  LEFT JOIN
    internal_data
  USING (state_code, termination_date)
)

SELECT
  state_code,
  state_code AS region_code,
  termination_date,
  external_data.person_id AS external_person_id,
  internal_data.person_id AS internal_person_id,
  external_data.person_external_id AS external_person_external_id,
  internal_data.person_external_id AS internal_person_external_id,
FROM
  external_data_with_ids external_data
FULL OUTER JOIN
  internal_metrics_for_valid_regions_and_dates internal_data
USING(state_code, termination_date, person_id)
"""

SUPERVISION_TERMINATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SUPERVISION_TERMINATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_NAME,
    view_query_template=SUPERVISION_TERMINATION_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE,
    description=SUPERVISION_TERMINATION_PERSON_LEVEL_EXTERNAL_COMPARISON_DESCRIPTION,
    external_accuracy_dataset=dataset_config.EXTERNAL_ACCURACY_DATASET,
    materialized_metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TERMINATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER.build_and_print()
