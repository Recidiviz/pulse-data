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
"""A view containing external data for person level supervision terminations to validate against."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

_QUERY_TEMPLATE = """
SELECT * FROM `{project_id}.{us_id_validation_dataset}.supervision_termination_person_level_raw`
UNION ALL
SELECT * FROM `{project_id}.{us_pa_validation_dataset}.supervision_termination_person_level_raw`
UNION ALL
SELECT * FROM `{project_id}.{us_me_validation_dataset}.supervision_termination_person_level_raw`
"""

SUPERVISION_TERMINATION_PERSON_LEVEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.EXTERNAL_ACCURACY_DATASET,
    view_id="supervision_termination_person_level",
    view_query_template=_QUERY_TEMPLATE,
    description="Contains external data for person level supervision terminations to "
    "validate against. See http://go/external-validations for instructions on adding "
    "new data.",
    us_id_validation_dataset=dataset_config.validation_dataset_for_state(
        StateCode.US_ID
    ),
    us_pa_validation_dataset=dataset_config.validation_dataset_for_state(
        StateCode.US_PA
    ),
    us_me_validation_dataset=dataset_config.validation_dataset_for_state(
        StateCode.US_ME
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TERMINATION_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
