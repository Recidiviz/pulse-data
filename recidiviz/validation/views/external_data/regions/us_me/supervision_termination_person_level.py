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
"""A view containing supervision terminations at the person level for Maine."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

VIEW_QUERY_TEMPLATE = """
SELECT 
    'US_ME' AS state_code,
    person_external_id,
    release_date AS termination_date
FROM `{project_id}.{us_me_validation_dataset}.population_releases_materialized`
-- -- Filter to releases from supervision to liberty
WHERE released_from_location_type IN ('4')
AND (
    released_to_location_type NOT IN (
    '2',  -- Adult DOC Facilities
    '3', -- Juvenile DOC Facilities
    '7',  -- Adult pre-release centers
    '9', -- County Jails
    '19', -- Federal transfers
    '15', -- Juvenile probation offices
    '4' -- Adult probation offices
    ) OR released_to_location_type IS NULL -- released to liberty
)
"""

US_ME_SUPERVISION_TERMINATION_PERSON_LEVEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_ME),
    view_id="supervision_termination_person_level",
    description="A view detailing supervision terminations at the person level for Maine",
    view_query_template=VIEW_QUERY_TEMPLATE,
    us_me_validation_dataset=dataset_config.validation_dataset_for_state(
        StateCode.US_ME
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_SUPERVISION_TERMINATION_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
