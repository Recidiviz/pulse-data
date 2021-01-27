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
"""Counts of the total state population by gender, grouped by state-specific categories."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

STATE_GENDER_POPULATION_VIEW_NAME = 'state_gender_population'

STATE_GENDER_POPULATION_VIEW_DESCRIPTION = """Counts of the total state population by gender."""

STATE_GENDER_POPULATION_VIEW_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      state_code,
      gender,
      population_count,
      total_state_population_count
    FROM `{project_id}.{static_reference_dataset}.state_gender_population_counts`
    """

STATE_GENDER_POPULATION_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=STATE_GENDER_POPULATION_VIEW_NAME,
    view_query_template=STATE_GENDER_POPULATION_VIEW_QUERY_TEMPLATE,
    dimensions=['state_code', 'gender'],
    description=STATE_GENDER_POPULATION_VIEW_DESCRIPTION,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        STATE_GENDER_POPULATION_VIEW_BUILDER.build_and_print()
