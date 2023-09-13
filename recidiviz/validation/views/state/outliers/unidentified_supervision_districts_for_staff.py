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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.    See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.    If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""
View that returns the count of staff by role subtype who do have a supervision_district on their
staff record that does not match a district in the supervision_districts product view.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import OUTLIERS_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views.dataset_config import VIEWS_DATASET

_VIEW_NAME = "unidentified_supervision_districts_for_staff"

_VIEW_DESCRIPTION = (
    "View that returns the count of staff by role subtype who do have a supervision_district on their "
    "staff record that does not match a district in the supervision_districts product view."
)

_QUERY_TEMPLATE = """
WITH officers_with_metrics AS (
  SELECT DISTINCT
    o.state_code,
    o.external_id,
    o.supervisor_external_id,
    "SUPERVISION_OFFICER" AS role_subtype,
    supervision_district
  FROM `{project_id}.{outliers_dataset}.supervision_officers_materialized` o
  LEFT JOIN `{project_id}.{outliers_dataset}.supervision_officer_metrics_materialized` m
    ON o.state_code = m.state_code AND o.external_id = m.officer_id
  WHERE
    m.period = 'YEAR'
    AND m.end_date = DATE_TRUNC(CURRENT_DATE("US/Eastern"), MONTH)
), 
supervisors_for_officers_with_metrics AS (
  SELECT DISTINCT
    s.state_code,
    s.external_id,
    "SUPERVISION_OFFICER_SUPERVISOR" AS role_subtype,
    s.supervision_district
  FROM `{project_id}.{outliers_dataset}.supervision_officer_supervisors_materialized` s
  INNER JOIN officers_with_metrics
    ON officers_with_metrics.state_code = s.state_code AND officers_with_metrics.supervisor_external_id = s.external_id
),
officers_and_supervisors AS (
  SELECT 
    * EXCEPT (supervisor_external_id)
  FROM officers_with_metrics

  UNION ALL 

  SELECT 
    *
  FROM supervisors_for_officers_with_metrics
)

SELECT
  officers_and_supervisors.state_code AS region_code,
  officers_and_supervisors.role_subtype,
  officers_and_supervisors.supervision_district,
  COUNT(DISTINCT officers_and_supervisors.external_id) AS num_staff_with_unidentified_district
FROM officers_and_supervisors
LEFT JOIN `{project_id}.{outliers_dataset}.supervision_districts_materialized` d 
  ON officers_and_supervisors.state_code = d.state_code AND officers_and_supervisors.supervision_district = d.external_id
WHERE 
  officers_and_supervisors.supervision_district IS NOT NULL 
  AND d.external_id IS NULL
GROUP BY 1, 2, 3
"""

UNIDENTIFIED_SUPERVISION_DISTRICTS_FOR_STAFF_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    should_materialize=True,
    outliers_dataset=OUTLIERS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        UNIDENTIFIED_SUPERVISION_DISTRICTS_FOR_STAFF_VIEW_BUILDER.build_and_print()
