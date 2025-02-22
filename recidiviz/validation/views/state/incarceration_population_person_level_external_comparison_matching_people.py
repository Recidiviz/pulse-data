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

"""A view comparing various values from internal person-level incarceration population metrics to the person-level
values from external metrics provided by the state where we both agree that the person was on incarceration.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.state.incarceration_population_person_level_template import (
    incarceration_population_person_level_query,
)

INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_PREFIX = (
    "incarceration_population_person_level_external_comparison_matching_people_"
)

INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_DESCRIPTION_PREFIX = """
Compares internal and external lists of person-level incarceration populations among rows where we both agree the person was incarcerated on that day.
"""

INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_WITH_FACILITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_PREFIX
    + "facility",
    view_query_template=incarceration_population_person_level_query(
        include_unmatched_people=False, external_data_required_fields={"facility"}
    ),
    description=INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_DESCRIPTION_PREFIX
    + " Only includes external data with facility information.",
    external_accuracy_dataset=dataset_config.EXTERNAL_ACCURACY_DATASET,
    materialized_metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_WITH_FACILITY_VIEW_BUILDER.build_and_print()
