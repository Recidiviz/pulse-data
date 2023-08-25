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
View that returns information for supervision officers who do not have a corresponding record in
the outliers supervision_officers product view, but are officers with metrics in the current
12-month reporting period (via supervision_officer_aggregated_metrics product view).
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import OUTLIERS_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views.dataset_config import VIEWS_DATASET

_VIEW_NAME = "unidentified_supervision_officers"

_VIEW_DESCRIPTION = (
    "View that returns information for supervision officers who do not have a corresponding record in "
    "the outliers supervision_officers product view, but are officers with metrics in the "
    "12-month reporting period that ends on the first of the current month."
)

_QUERY_TEMPLATE = """
SELECT 
  m.state_code AS region_code,
  m.officer_id
FROM `{project_id}.{outliers_dataset}.supervision_officer_metrics_materialized` m
FULL OUTER JOIN `{project_id}.{outliers_dataset}.supervision_officers_materialized` o 
  ON m.state_code = o.state_code AND m.officer_id = o.external_id
WHERE
  m.officer_id IS NOT NULL 
  AND m.period = 'YEAR'
  AND m.end_date = DATE_TRUNC(CURRENT_DATE("US/Eastern"), MONTH)
  AND (o.external_id IS NULL OR o.full_name IS NULL)
"""

UNIDENTIFIED_SUPERVISION_OFFICERS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    should_materialize=True,
    outliers_dataset=OUTLIERS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        UNIDENTIFIED_SUPERVISION_OFFICERS_VIEW_BUILDER.build_and_print()
