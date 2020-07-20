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
"""A view which provides a comparison of revocation counts by source violation type between views for the dashboard
and views for the public dashboard."""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

REVOCATIONS_BY_VIOLATION_TYPE_DASHBOARD_COMPARISON_VIEW_NAME = 'revocations_by_violation_type_dashboard_comparison'

REVOCATIONS_BY_VIOLATION_TYPE_DASHBOARD_COMPARISON_DESCRIPTION = """ 
Compares counts of revocations by source violation type between the dashboard and the public dashboard. """

REVOCATIONS_BY_VIOLATION_TYPE_DASHBOARD_COMPARISON_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH dashboard_revocations_all_districts AS (
      SELECT * FROM `{project_id}.{dashboard_dataset}.revocations_by_violation_type_by_month`
      WHERE supervision_type != 'ALL'
    ), public_dashboard_revocations_no_breakdowns AS (
      SELECT * FROM `{project_id}.{public_dashboard_dataset}.supervision_revocations_by_month_by_type_by_demographics`
      WHERE race_or_ethnicity = 'ALL'
      AND gender = 'ALL'
      AND age_bucket = 'ALL'
    )

    SELECT
      state_code as region_code,
      year,
      month,
      district,
      supervision_type,
      IFNULL(dashboard_revocations_all_districts.absconsion_count, 0) as dashboard_absconsion_count,
      IFNULL(dashboard_revocations_all_districts.felony_count, 0) as dashboard_new_crime_count,
      IFNULL(dashboard_revocations_all_districts.technical_count, 0) as dashboard_technical_count,
      IFNULL(dashboard_revocations_all_districts.unknown_count, 0) as dashboard_unknown_count,
      IFNULL(public_dashboard_revocations_no_breakdowns.absconsion_count, 0) as public_dashboard_absconsion_count,
      IFNULL(public_dashboard_revocations_no_breakdowns.new_crime_count, 0) as public_dashboard_new_crime_count,
      IFNULL(public_dashboard_revocations_no_breakdowns.technical_count, 0) as public_dashboard_technical_count,
      IFNULL(public_dashboard_revocations_no_breakdowns.unknown_count, 0) as public_dashboard_unknown_count,
    FROM 
      dashboard_revocations_all_districts
    FULL OUTER JOIN
      public_dashboard_revocations_no_breakdowns
    USING (state_code, year, month, district, supervision_type)
    ORDER BY state_code, year, month, district, supervision_type
"""

REVOCATIONS_BY_VIOLATION_TYPE_DASHBOARD_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=REVOCATIONS_BY_VIOLATION_TYPE_DASHBOARD_COMPARISON_VIEW_NAME,
    view_query_template=REVOCATIONS_BY_VIOLATION_TYPE_DASHBOARD_COMPARISON_QUERY_TEMPLATE,
    description=REVOCATIONS_BY_VIOLATION_TYPE_DASHBOARD_COMPARISON_DESCRIPTION,
    dashboard_dataset=state_dataset_config.DASHBOARD_VIEWS_DATASET,
    public_dashboard_dataset=state_dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        REVOCATIONS_BY_VIOLATION_TYPE_DASHBOARD_COMPARISON_VIEW_BUILDER.build_and_print()
