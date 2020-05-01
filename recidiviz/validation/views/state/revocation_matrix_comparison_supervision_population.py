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

"""A view which provides a comparison of total supervision population counts summed across all dimensional breakdowns
in all of the views that support the Revocation Analysis Matrix tool."""

# pylint: disable=trailing-whitespace
from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

REVOCATION_MATRIX_COMPARISON_SUPERVISION_POPULATION_VIEW_NAME = 'revocation_matrix_comparison_supervision_population'

REVOCATION_MATRIX_COMPARISON_SUPERVISION_POPULATION_DESCRIPTION = """ 
Revocation matrix comparison of summed supervision population counts """

REVOCATION_MATRIX_COMPARISON_SUPERVISION_POPULATION_QUERY = \
    """
    /*{description}*/
    WITH by_district as (
      SELECT state_code as region_code, SUM(total_supervision_count) as total_supervision
      FROM `{project_id}.{view_dataset}.revocations_matrix_distribution_by_district`
      GROUP BY state_code
    ),
    by_risk_level as (
      SELECT state_code as region_code, SUM(total_supervision_count) as total_supervision
      FROM `{project_id}.{view_dataset}.revocations_matrix_distribution_by_risk_level`
      GROUP BY state_code
    ),
    by_gender as (
      SELECT state_code as region_code, SUM(total_supervision_count) as total_supervision
      FROM `{project_id}.{view_dataset}.revocations_matrix_distribution_by_gender`
      GROUP BY state_code
    ),
    by_race as (
      SELECT state_code as region_code, SUM(total_supervision_count) as total_supervision
      FROM `{project_id}.{view_dataset}.revocations_matrix_distribution_by_race`
      GROUP BY state_code
    )
    SELECT bd.region_code,
           bd.total_supervision as district_sum,
           brl.total_supervision as risk_level_sum,
           bg.total_supervision as gender_sum,
           br.total_supervision as race_sum
    FROM by_district bd JOIN by_risk_level brl on bd.region_code = brl.region_code
    JOIN by_gender bg on brl.region_code = bg.region_code
    JOIN by_race br on bg.region_code = br.region_code
""".format(
        description=REVOCATION_MATRIX_COMPARISON_SUPERVISION_POPULATION_DESCRIPTION,
        project_id=PROJECT_ID,
        view_dataset=VIEWS_DATASET,
    )

REVOCATION_MATRIX_COMPARISON_SUPERVISION_POPULATION_VIEW = bqview.BigQueryView(
    view_id=REVOCATION_MATRIX_COMPARISON_SUPERVISION_POPULATION_VIEW_NAME,
    view_query=REVOCATION_MATRIX_COMPARISON_SUPERVISION_POPULATION_QUERY
)

if __name__ == '__main__':
    print(REVOCATION_MATRIX_COMPARISON_SUPERVISION_POPULATION_VIEW.view_id)
    print(REVOCATION_MATRIX_COMPARISON_SUPERVISION_POPULATION_VIEW.view_query)
