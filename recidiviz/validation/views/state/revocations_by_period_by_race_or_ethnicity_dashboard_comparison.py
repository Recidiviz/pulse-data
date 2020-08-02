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
"""A view which provides a comparison of revocation counts by period by race/ethnicity between views for the dashboard
and views for the public dashboard."""

# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

REVOCATIONS_BY_PERIOD_BY_RACE_OR_ETHNICITY_DASHBOARD_COMPARISON_VIEW_NAME = \
    'revocations_by_period_by_race_or_ethnicity_dashboard_comparison'

REVOCATIONS_BY_PERIOD_BY_RACE_OR_ETHNICITY_DASHBOARD_COMPARISON_DESCRIPTION = """ 
Compares counts of revocations by period and by race/ethnicity between the dashboard and the public dashboard. """

REVOCATIONS_BY_PERIOD_BY_RACE_OR_ETHNICITY_DASHBOARD_COMPARISON_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH dashboard_revocations AS (
      SELECT * FROM `{project_id}.{dashboard_dataset}.revocations_by_race_and_ethnicity_by_period`
      WHERE district = 'ALL'
      AND supervision_type IN ('ALL', 'PROBATION', 'PAROLE')
      AND metric_period_months = 36
      AND race_or_ethnicity IN ('AMERICAN_INDIAN_ALASKAN_NATIVE', 'BLACK', 'HISPANIC', 'WHITE')
    ), public_dashboard_revocations AS (
      SELECT * FROM `{project_id}.{public_dashboard_dataset}.supervision_revocations_by_period_by_type_by_demographics`
      WHERE race_or_ethnicity IN ('AMERICAN_INDIAN_ALASKAN_NATIVE', 'BLACK', 'HISPANIC', 'WHITE')
      AND supervision_type IN ('ALL', 'PROBATION', 'PAROLE')
    )
    
    
    SELECT
      state_code as region_code,
      race_or_ethnicity,
      supervision_type,
      IFNULL(dashboard_revocations.revocation_count, 0) as dashboard_revocation_count,
      IFNULL(public_dashboard_revocations.revocation_count, 0) as public_dashboard_revocation_count
    FROM
      dashboard_revocations 
    FULL OUTER JOIN
      public_dashboard_revocations 
    USING (state_code, race_or_ethnicity, supervision_type, metric_period_months)
    ORDER BY state_code, metric_period_months, supervision_type, race_or_ethnicity 
"""

REVOCATIONS_BY_PERIOD_BY_RACE_OR_ETHNICITY_DASHBOARD_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=REVOCATIONS_BY_PERIOD_BY_RACE_OR_ETHNICITY_DASHBOARD_COMPARISON_VIEW_NAME,
    view_query_template=REVOCATIONS_BY_PERIOD_BY_RACE_OR_ETHNICITY_DASHBOARD_COMPARISON_QUERY_TEMPLATE,
    description=REVOCATIONS_BY_PERIOD_BY_RACE_OR_ETHNICITY_DASHBOARD_COMPARISON_DESCRIPTION,
    dashboard_dataset=state_dataset_config.DASHBOARD_VIEWS_DATASET,
    public_dashboard_dataset=state_dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        REVOCATIONS_BY_PERIOD_BY_RACE_OR_ETHNICITY_DASHBOARD_COMPARISON_VIEW_BUILDER.build_and_print()
