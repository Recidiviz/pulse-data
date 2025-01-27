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
View that returns supervisors in US_IX and US_PA that don't have a supervision_district 
or one that is not matched to any of the district managers' districts in the 
DM outliers product view and vice cersa.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import OUTLIERS_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views.dataset_config import VIEWS_DATASET

_VIEW_NAME = "unmatched_supervision_districts_for_email_states"

_VIEW_DESCRIPTION = """
View that returns supervisors in US_IX and US_PA that don't have a supervision_district 
or one that is not matched to any of the district managers' districts in the DM outliers
product view and vice versa. If this validation is failing and the current_supervision_staff_missing_district
validation is passing, then investigate the locations of the staff surfaced here and
check with SEM/PMs to see if we expect the DM with an unmatched district to be CC'd to 
a set of emails or if we expect the supervisor to have a certain DM CC'd onto their email 
(which should be done via matching supervision_district values). If both validations are
failing then there is a deeper root cause to investigate!
"""

_QUERY_TEMPLATE = """
WITH 
officers_with_metrics AS (
  SELECT DISTINCT
    o.state_code,
    o.external_id,
    o.supervisor_external_id,
    supervision_district
  FROM `{project_id}.{outliers_dataset}.supervision_officers_materialized` o 
  LEFT JOIN `{project_id}.{outliers_dataset}.supervision_officer_outlier_status_materialized` s
    ON o.state_code = s.state_code AND o.external_id = s.officer_id
  WHERE\
    s.period = 'YEAR'
    AND s.end_date = DATE_TRUNC(CURRENT_DATE("US/Eastern"), MONTH)
    AND s.status = 'FAR'
)
, supervisors_for_officers_with_metrics AS (    
  SELECT DISTINCT
    s.state_code,
    s.email,
    s.external_id,
    s.supervision_district,
  FROM `{project_id}.{outliers_dataset}.supervision_officer_supervisors_materialized` s 
  INNER JOIN officers_with_metrics o 
    ON o.state_code = s.state_code AND o.supervisor_external_id = s.external_id
)
, cte AS (
  SELECT DISTINCT
    s.state_code,
    s.email,
    s.external_id,
    UPPER(supervision_district) AS supervisor_district,
    UPPER(dm.supervision_district) AS dm_district,
  FROM supervisors_for_officers_with_metrics s 
  FULL OUTER JOIN `{project_id}.{outliers_dataset}.supervision_district_managers_materialized` dm 
    USING (state_code, supervision_district)
  WHERE
    s.state_code IN ('US_IX', 'US_PA')
  ORDER BY 1, 2
)

SELECT
  state_code,
  state_code AS region_code, 
  email,
  external_id,
  supervisor_district, 
  dm_district,
FROM cte 
WHERE 
  supervisor_district IS NULL
  OR dm_district IS NULL
"""

UNMATCHED_SUPERVISION_DISTRICTS_FOR_EMAIL_STATES_VIEW_BUILDER = (
    SimpleBigQueryViewBuilder(
        dataset_id=VIEWS_DATASET,
        view_id=_VIEW_NAME,
        view_query_template=_QUERY_TEMPLATE,
        description=_VIEW_DESCRIPTION,
        should_materialize=True,
        outliers_dataset=OUTLIERS_VIEWS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        UNMATCHED_SUPERVISION_DISTRICTS_FOR_EMAIL_STATES_VIEW_BUILDER.build_and_print()
