# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Calculates representation priority for races and ethnicities in each state."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    EXTERNAL_REFERENCE_VIEWS_DATASET,
)

STATE_RESIDENT_POPULATION_COMBINED_RACE_ETHNICITY_PRIORITY_QUERY_TEMPLATE = """
WITH sub_population_totals AS (
    SELECT state_code, race AS race_or_ethnicity, SUM(population) AS population
    FROM `{project_id}.{external_reference_views_dataset}.state_resident_population`
    GROUP BY 1, 2
    UNION ALL
    SELECT state_code, ethnicity AS race_or_ethnicity, SUM(population) AS population
    FROM `{project_id}.{external_reference_views_dataset}.state_resident_population`
    WHERE ethnicity != 'NOT_HISPANIC'
    GROUP BY 1, 2
)
SELECT
    state_code,
    race_or_ethnicity,
    ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY population) AS representation_priority
FROM sub_population_totals
"""

STATE_RESIDENT_POPULATION_COMBINED_RACE_ETHNICITY_PRIORITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=EXTERNAL_REFERENCE_VIEWS_DATASET,
    view_id="state_resident_population_combined_race_ethnicity_priority",
    description="View over the state resident population data that calculates "
    "representation priority for combined race and ethnicity.",
    view_query_template=STATE_RESIDENT_POPULATION_COMBINED_RACE_ETHNICITY_PRIORITY_QUERY_TEMPLATE,
    external_reference_views_dataset=EXTERNAL_REFERENCE_VIEWS_DATASET,
)
