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
"""Implements BQ View to fetch information on the the latest assessments.

TODO(#5769): Move next_assessment_date to the calculation pipeline directly.
TODO(#5809): Output the assessment score in the calculation pipeline directly.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.case_triage.views.dataset_config import VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


LATEST_ASSESSMENTS_QUERY_VIEW = """
WITH latest_interaction_dates AS (
  SELECT
    person_id,
    state_code,
    MAX(most_recent_assessment_date) AS assessment_date
  FROM
    `{project_id}.{dataflow_metrics_materialized_dataset}.most_recent_supervision_case_compliance_metrics_materialized`
  WHERE
    person_external_id IS NOT NULL
  GROUP BY person_id, state_code
),
latest_assessments AS (
  SELECT
    person_id,
    state_code,
    assessment_date,
    -- This MAX is needed in instances where there are multiple assessments in a given day (likely because the
    -- data sources that we get our info from create a new row every time an assessment is "saved"). We pick
    -- the one with the highest id because we have no better way to sort, and there is likely a correlation
    -- between higher external id and more recently saved.
    MAX(external_id) AS external_id
  FROM
    latest_interaction_dates
  LEFT JOIN
    `{project_id}.state.state_assessment`
  USING (person_id, state_code, assessment_date)
  GROUP BY person_id, state_code, assessment_date
)
SELECT
    person_id,
    state_code,
    assessment_date AS most_recent_assessment_date,
    assessment_score
FROM
    latest_assessments
INNER JOIN
    `{project_id}.state.state_assessment`
USING (person_id, state_code, assessment_date, external_id)
"""

LATEST_ASSESSMENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id="latest_assessments",
    view_query_template=LATEST_ASSESSMENTS_QUERY_VIEW,
    dataflow_metrics_materialized_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LATEST_ASSESSMENTS_VIEW_BUILDER.build_and_print()
