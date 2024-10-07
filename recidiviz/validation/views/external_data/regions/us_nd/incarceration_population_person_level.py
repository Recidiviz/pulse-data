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
"""A view detailing the incarceration population at the person level for North Dakota."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

# TODO(#10883): Ignoring this ND data for now because we are not sure that it is correct.
# remove the LIMIT 0 when we are sure it is correct.
VIEW_QUERY_TEMPLATE = """
SELECT
  'US_ND' AS state_code,
  Offender_ID as person_external_id,
  'US_ND_ELITE' as external_id_type,
  DATE('2021-11-01') as date_of_stay,
  Facility as facility
FROM `{project_id}.{us_nd_validation_oneoff_dataset}.incarcerated_individuals_2021_11_01`
--- TODO(#10884): remove the LIMIT 0 when we are sure it is correct.
LIMIT 0
"""

US_ND_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_ND),
    view_id="incarceration_population_person_level",
    description="A view detailing the incarceration population at the person level for North Dakota",
    view_query_template=VIEW_QUERY_TEMPLATE,
    should_materialize=True,
    us_nd_validation_oneoff_dataset=dataset_config.validation_oneoff_dataset_for_state(
        StateCode.US_ND
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
