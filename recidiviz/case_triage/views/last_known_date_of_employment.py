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
"""Creates the view builder and view for fetching the last known date of employment.

As part of TODO(#5463), we should clean this up and probably think about setting up
a calc pipeline that outputs this information.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.case_triage.views.dataset_config import VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

LAST_KNOWN_DATE_OF_EMPLOYMENT_QUERY_TEMPLATE = """
SELECT
  state_code,
  person_external_id,
  IF(
    MAX(COALESCE(recorded_end_date, CURRENT_DATE())) > CURRENT_DATE(),
    CURRENT_DATE(),
    MAX(COALESCE(recorded_end_date, CURRENT_DATE()))
  ) AS last_known_date_of_employment
FROM
  `{project_id}.{case_triage_dataset}.employment_periods_materialized`
WHERE
  NOT is_unemployed
GROUP BY state_code, person_external_id
"""

LAST_KNOWN_DATE_OF_EMPLOYMENT_DESCRIPTION = """
View for fetching the last known date of employment
"""

LAST_KNOWN_DATE_OF_EMPLOYMENT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id="last_known_date_of_employment",
    description=LAST_KNOWN_DATE_OF_EMPLOYMENT_DESCRIPTION,
    view_query_template=LAST_KNOWN_DATE_OF_EMPLOYMENT_QUERY_TEMPLATE,
    case_triage_dataset=VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LAST_KNOWN_DATE_OF_EMPLOYMENT_VIEW_BUILDER.build_and_print()
