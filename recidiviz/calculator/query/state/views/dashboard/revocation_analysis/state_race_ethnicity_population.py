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
"""Counts of the total state population by race/ethnicity, grouped by state-specific categories."""
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.calculator.query.state.views.dashboard.revocation_analysis.revocations_matrix_distribution_by_race import (
    US_PA_SUPPORTED_RACE_VALUES,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

STATE_RACE_ETHNICITY_POPULATION_VIEW_NAME = "state_race_ethnicity_population"

STATE_RACE_ETHNICITY_POPULATION_VIEW_DESCRIPTION = (
    """Counts of the total state population by race/ethnicity."""
)

STATE_RACE_ETHNICITY_POPULATION_VIEW_QUERY_TEMPLATE = """
SELECT
    state_code,
    {state_specific_race_or_ethnicity_groupings},
    SUM(population) as population_count,
    SUM(SUM(population)) OVER (PARTITION BY state_code) as total_state_population_count
FROM `{project_id}.reference_views.state_resident_population_combined_race_ethnicity`
GROUP BY state_code, race_or_ethnicity
"""

STATE_RACE_ETHNICITY_POPULATION_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=STATE_RACE_ETHNICITY_POPULATION_VIEW_NAME,
    view_query_template=STATE_RACE_ETHNICITY_POPULATION_VIEW_QUERY_TEMPLATE,
    dimensions=("state_code", "race_or_ethnicity"),
    description=STATE_RACE_ETHNICITY_POPULATION_VIEW_DESCRIPTION,
    state_specific_race_or_ethnicity_groupings=state_specific_query_strings.state_specific_race_or_ethnicity_groupings(
        supported_race_overrides={StateCode.US_PA: US_PA_SUPPORTED_RACE_VALUES}
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STATE_RACE_ETHNICITY_POPULATION_VIEW_BUILDER.build_and_print()
