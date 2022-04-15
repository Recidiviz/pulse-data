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
"""A view revealing when normalized incarceration periods have a null
specialized_purpose_for_incarceration value, which should never happen.

Existence of any rows indicates a bug in IP normalization logic.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
)
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.utils.normalized_entities_validation_utils import (
    validation_query_for_normalized_entity,
)

INVALID_NULL_SPFI_NORMALIZED_IPS_VIEW_NAME = "invalid_null_spfi_normalized_ips"

INVALID_NULL_SPFI_NORMALIZED_IPS_DESCRIPTION = (
    """Metrics with invalid null values for specialized_purpose_for_incarceration."""
)


INVALID_NULL_SPFI_NORMALIZED_IPS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INVALID_NULL_SPFI_NORMALIZED_IPS_VIEW_NAME,
    view_query_template=validation_query_for_normalized_entity(
        normalized_entity_class=NormalizedStateIncarcerationPeriod,
        additional_columns_to_select=["person_id"],
        invalid_rows_filter_clause="WHERE specialized_purpose_for_incarceration IS NULL",
        validation_description=INVALID_NULL_SPFI_NORMALIZED_IPS_DESCRIPTION,
    ),
    description=INVALID_NULL_SPFI_NORMALIZED_IPS_DESCRIPTION,
    normalized_state_dataset=state_dataset_config.NORMALIZED_STATE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INVALID_NULL_SPFI_NORMALIZED_IPS_VIEW_BUILDER.build_and_print()
