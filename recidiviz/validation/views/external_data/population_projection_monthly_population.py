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
"""A view containing external data for aggregate projected populations to validate against."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

# TODO(#27269): Update this to build the query by collecting files
_QUERY_TEMPLATE = """
SELECT * FROM `{project_id}.{us_ix_validation_dataset}.population_projection_monthly_population`
"""

POPULATION_PROJECTION_MONTHLY_POPULATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.EXTERNAL_ACCURACY_DATASET,
    view_id="population_projection_monthly_population",
    view_query_template=_QUERY_TEMPLATE,
    description="Contains external data for aggregate projected populations to "
    "validate against. See http://go/external-validations for instructions on adding "
    "new data.",
    us_ix_validation_dataset=dataset_config.validation_dataset_for_state(
        StateCode.US_IX
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        POPULATION_PROJECTION_MONTHLY_POPULATION_VIEW_BUILDER.build_and_print()
