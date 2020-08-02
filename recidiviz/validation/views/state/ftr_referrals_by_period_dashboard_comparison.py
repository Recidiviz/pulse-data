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
"""Compares counts of FTR referrals between between views for the dashboard and views for the public dashboard."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config
from recidiviz.calculator.query.state import dataset_config as state_dataset_config

FTR_REFERRALS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_NAME = 'ftr_referrals_by_period_dashboard_comparison'

FTR_REFERRALS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_DESCRIPTION = \
    """Compares counts of FTR referrals between between views for the dashboard and views for the public dashboard."""

FTR_REFERRALS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH dashboard_ftr_referrals AS (
      SELECT * FROM `{project_id}.{dashboard_dataset}.ftr_referrals_by_period`
      WHERE supervision_type = 'ALL'
      AND district = 'ALL'
    ), public_dashboard_referrals AS (
      SELECT * FROM `{project_id}.{public_dashboard_dataset}.ftr_referrals_by_prioritized_race_and_ethnicity_by_period` 
      WHERE race_or_ethnicity = 'ALL'
    )
    
    SELECT
      state_code as region_code,
      metric_period_months,
      IFNULL(count, 0) as dashboard_ftr_referral_count,
      IFNULL(ftr_referral_count, 0) as public_dashboard_ftr_referral_count
    FROM
      dashboard_ftr_referrals
    FULL OUTER JOIN
      public_dashboard_referrals 
    USING (state_code, metric_period_months)
    """

FTR_REFERRALS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=FTR_REFERRALS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_NAME,
    view_query_template=FTR_REFERRALS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_QUERY_TEMPLATE,
    description=FTR_REFERRALS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_DESCRIPTION,
    dashboard_dataset=state_dataset_config.DASHBOARD_VIEWS_DATASET,
    public_dashboard_dataset=state_dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        FTR_REFERRALS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_BUILDER.build_and_print()
