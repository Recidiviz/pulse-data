# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""A view which provides a comparison of supervision population counts by district between views for the dashboard
and views for the public dashboard."""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SUPERVISION_POPULATION_BY_DISTRICT_DASHBOARD_COMPARISON_VIEW_NAME = \
    'supervision_population_by_district_dashboard_comparison'

SUPERVISION_POPULATION_BY_DISTRICT_DASHBOARD_COMPARISON_DESCRIPTION = """ 
Compares counts of supervision populations by district between the dashboard and the public dashboard. """

SUPERVISION_POPULATION_BY_DISTRICT_DASHBOARD_COMPARISON_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH dashboard_supervision_population AS (
      SELECT * FROM `{project_id}.{dashboard_dataset}.revocations_by_period` 
      WHERE district != 'ALL'
      AND supervision_type != 'ALL'
      AND metric_period_months = 1
    ), public_dashboard_supervision_population AS (
      SELECT * FROM `{project_id}.{public_dashboard_dataset}.supervision_population_by_district_by_demographics`
      WHERE race_or_ethnicity = 'ALL'
      AND gender = 'ALL'
      AND age_bucket = 'ALL'
    )
    
    SELECT
      state_code as region_code,
      supervision_type,
      district,
      dashboard_supervision_population.total_supervision_count as dashboard_supervision_count,
      public_dashboard_supervision_population.total_supervision_count as public_dashboard_supervision_count 
    FROM 
      dashboard_supervision_population
    FULL OUTER JOIN
      public_dashboard_supervision_population
    USING (state_code, supervision_type, district)
    ORDER BY state_code, supervision_type
"""

SUPERVISION_POPULATION_BY_DISTRICT_DASHBOARD_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SUPERVISION_POPULATION_BY_DISTRICT_DASHBOARD_COMPARISON_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_BY_DISTRICT_DASHBOARD_COMPARISON_QUERY_TEMPLATE,
    description=SUPERVISION_POPULATION_BY_DISTRICT_DASHBOARD_COMPARISON_DESCRIPTION,
    dashboard_dataset=state_dataset_config.DASHBOARD_VIEWS_DATASET,
    public_dashboard_dataset=state_dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        SUPERVISION_POPULATION_BY_DISTRICT_DASHBOARD_COMPARISON_VIEW_BUILDER.build_and_print()
