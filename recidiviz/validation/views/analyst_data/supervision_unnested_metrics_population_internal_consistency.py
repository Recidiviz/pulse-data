# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""A validation view comparing the daily population in unnested metrics views
to the sum of population counts disaggregated by risk level, case type, and supervision type"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.calculator.query.state.views.analyst_data.supervision_unnested_metrics import (
    SUPERVISION_METRICS_SUPPORTED_LEVELS,
    SUPERVISION_METRICS_SUPPORTED_LEVELS_INDEX_COLUMNS,
    SUPERVISION_METRICS_SUPPORTED_LEVELS_NAMES,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SUPERVISION_UNNESTED_METRICS_POPULATION_INTERNAL_CONSISTENCY_VIEW_NAME = (
    "supervision_unnested_metrics_population_internal_consistency"
)

SUPERVISION_UNNESTED_METRICS_POPULATION_INTERNAL_CONSISTENCY_DESCRIPTION = """
Return all rows of supervision unnested metrics comparing daily population 
to the sum of population counts disaggregated by risk level,
case type, and supervision type.
"""

# Create a list of all table comparison sub-queries
SUPERVISION_UNNESTED_METRICS_POPULATION_INTERNAL_CONSISTENCY_QUERY_FRAGMENTS = [
    f"""
SELECT
    state_code AS region_code,
    "{SUPERVISION_METRICS_SUPPORTED_LEVELS_NAMES[level]}" AS level,
    -- Concatenate all index columns relevant to an analysis level
    CONCAT({", ' - ', ".join(
        [
            x for x in SUPERVISION_METRICS_SUPPORTED_LEVELS_INDEX_COLUMNS[level].split(", ")
            if (x != "state_code") or (level == "state_code")
        ]
    )}) AS level_value,
    date,
    daily_population,
    -- Supervision types
    population_parole + population_probation + population_community_confinement
    AS daily_population_supervision_type_sum,
    -- Case types
    population_general_case_type + population_domestic_violence_case_type + population_sex_offense_case_type +
    population_drug_case_type + population_mental_health_case_type + population_other_case_type +
    population_unknown_case_type AS daily_population_case_type_sum,
    -- Risk levels
    population_low_risk_level + population_high_risk_level + population_unknown_risk_level
    AS daily_population_risk_level_sum,
FROM
    `{{project_id}}.{{analyst_dataset}}.supervision_{SUPERVISION_METRICS_SUPPORTED_LEVELS_NAMES[level]}_unnested_metrics`
"""
    for level in SUPERVISION_METRICS_SUPPORTED_LEVELS
]

# Union all supported supervision analysis levels
SUPERVISION_UNNESTED_METRICS_POPULATION_INTERNAL_CONSISTENCY_QUERY_TEMPLATE = (
    "\nUNION ALL\n".join(
        SUPERVISION_UNNESTED_METRICS_POPULATION_INTERNAL_CONSISTENCY_QUERY_FRAGMENTS
    )
)

SUPERVISION_UNNESTED_METRICS_POPULATION_INTERNAL_CONSISTENCY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SUPERVISION_UNNESTED_METRICS_POPULATION_INTERNAL_CONSISTENCY_VIEW_NAME,
    view_query_template=SUPERVISION_UNNESTED_METRICS_POPULATION_INTERNAL_CONSISTENCY_QUERY_TEMPLATE,
    description=SUPERVISION_UNNESTED_METRICS_POPULATION_INTERNAL_CONSISTENCY_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_UNNESTED_METRICS_POPULATION_INTERNAL_CONSISTENCY_VIEW_BUILDER.build_and_print()
