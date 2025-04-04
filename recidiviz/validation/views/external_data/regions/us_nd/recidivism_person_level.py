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
"""A view containing recidivations at the person level for North Datokta."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

VIEW_QUERY_TEMPLATE = """
SELECT
    region_code AS state_code,
    release_cohort,
    follow_up_period,
    person_external_id,
    recidivated
FROM `{project_id}.{us_nd_validation_oneoffs_dataset}.recidivism_person_level_raw`
"""

US_ND_RECIDIVISM_PERSON_LEVEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_ND),
    view_id="recidivism_person_level",
    description="A view detailing recidivations at the person level for North Datokta",
    view_query_template=VIEW_QUERY_TEMPLATE,
    us_nd_validation_oneoffs_dataset=dataset_config.validation_oneoff_dataset_for_state(
        StateCode.US_ND
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_RECIDIVISM_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
