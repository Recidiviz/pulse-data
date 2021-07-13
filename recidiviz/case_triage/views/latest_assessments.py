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
"""Implements BQ View to fetch information on the latest assessments.

TODO(#5769): Move next_assessment_date to the calculation pipeline directly.
TODO(#5809): Output the assessment score in the calculation pipeline directly.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.case_triage.views.dataset_config import VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

LATEST_ASSESSMENTS_QUERY_VIEW = """
WITH latest_assessments AS (
  SELECT
    person_id,
    state_code,
    assessment_date,
    assessment_score,
    -- This ordering is needed in instances where there are multiple assessments in a given day
    -- (likely because the data sources that we get our info from create a new row every time an
    -- assessment is "saved"). We pick the one with the highest id because we have no better way
    -- to sort, and there is likely a correlation between higher external id and more recently saved.
    -- This ordering also bakes in the assumption that all assessment ids are fewer than 128
    -- characters long.
    ROW_NUMBER()
      OVER (PARTITION BY person_id ORDER BY assessment_date DESC, FORMAT('%128s', external_id) DESC) AS row_number
  FROM
    `{project_id}.state.state_assessment`
)
SELECT
    person_id,
    state_code,
    assessment_date AS most_recent_assessment_date,
    assessment_score
FROM
    latest_assessments
WHERE
  row_number = 1
"""

LATEST_ASSESSMENTS_DESCRIPTION = """
Generates the latest assessments for people on supervision and outputs the associated
 dates and scores.
"""

LATEST_ASSESSMENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id="latest_assessments",
    description=LATEST_ASSESSMENTS_DESCRIPTION,
    view_query_template=LATEST_ASSESSMENTS_QUERY_VIEW,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LATEST_ASSESSMENTS_VIEW_BUILDER.build_and_print()
