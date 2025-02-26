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
"""A view which provides a comparison of supervision success counts by period between views for the dashboard
and views for the public dashboard."""

# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SUPERVISION_SUCCESS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_NAME = \
    'supervision_success_by_period_dashboard_comparison'

SUPERVISION_SUCCESS_BY_PERIOD_DASHBOARD_COMPARISON_DESCRIPTION = """ 
Compares counts of supervision success by period between the dashboard and the public dashboard. """

SUPERVISION_SUCCESS_BY_PERIOD_DASHBOARD_COMPARISON_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH dashboard_success AS (
      SELECT * EXCEPT (district),
        IFNULL(district, 'EXTERNAL_UNKNOWN') as district
      FROM `{project_id}.{dashboard_dataset}.supervision_termination_by_type_by_period`  
      WHERE supervision_type != 'ALL'
    ), public_dashboard_success AS (
      SELECT * FROM `{project_id}.{public_dashboard_dataset}.supervision_success_by_period_by_demographics`
      WHERE race_or_ethnicity = 'ALL'
      AND gender = 'ALL'
      AND age_bucket = 'ALL'
    ), dashboard_metric_periods AS (
      SELECT DISTINCT metric_period_months
      FROM dashboard_success
    ), public_dashboard_metric_periods AS (
      SELECT DISTINCT metric_period_months
      FROM public_dashboard_success
    )
    
    SELECT
      state_code as region_code,
      district,
      supervision_type,
      metric_period_months,
      IFNULL(dashboard_success.successful_termination, 0) as dashboard_successful_termination,
      IFNULL(public_dashboard_success.successful_termination_count, 0) as public_dashboard_successful_termination,
      IFNULL((dashboard_success.revocation_termination + dashboard_success.successful_termination), 0) as dashboard_projected_completion,
      IFNULL(public_dashboard_success.projected_completion_count, 0) as public_dashboard_projected_completion
    FROM 
      dashboard_success
    FULL OUTER JOIN
      public_dashboard_success
    USING (state_code, metric_period_months, district, supervision_type)
     -- We cannot compare district breakdowns for probation because the public dashboard uses judicial districts --
    WHERE  (supervision_type = 'PAROLE' OR district = 'ALL')
    -- Only compare metric periods for which both dashboards are producing output --
    AND metric_period_months IN 
    (SELECT * FROM dashboard_metric_periods)
    AND metric_period_months IN
    (SELECT * FROM public_dashboard_metric_periods)
    ORDER BY state_code, metric_period_months, district, supervision_type
"""

SUPERVISION_SUCCESS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SUPERVISION_SUCCESS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_NAME,
    view_query_template=SUPERVISION_SUCCESS_BY_PERIOD_DASHBOARD_COMPARISON_QUERY_TEMPLATE,
    description=SUPERVISION_SUCCESS_BY_PERIOD_DASHBOARD_COMPARISON_DESCRIPTION,
    dashboard_dataset=state_dataset_config.DASHBOARD_VIEWS_DATASET,
    public_dashboard_dataset=state_dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        SUPERVISION_SUCCESS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_BUILDER.build_and_print()
