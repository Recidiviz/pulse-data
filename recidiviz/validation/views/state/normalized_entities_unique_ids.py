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
"""A view revealing when normalization pipelines are creating entities with
duplicate ID values.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.utils.normalized_entities_validation_utils import (
    unique_entity_id_values_query,
)

NORMALIZED_ENTITIES_UNIQUE_IDS_VIEW_NAME = "normalized_entities_unique_ids"

NORMALIZED_ENTITIES_UNIQUE_IDS_DESCRIPTION = """Reveals when normalization pipelines
are creating entities with duplicate ID values."""

NORMALIZED_ENTITIES_UNIQUE_IDS_QUERY_TEMPLATE = f"""
  /*{{description}}*/
  {unique_entity_id_values_query()}
"""

NORMALIZED_ENTITIES_UNIQUE_IDS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=NORMALIZED_ENTITIES_UNIQUE_IDS_VIEW_NAME,
    view_query_template=NORMALIZED_ENTITIES_UNIQUE_IDS_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    description=NORMALIZED_ENTITIES_UNIQUE_IDS_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        NORMALIZED_ENTITIES_UNIQUE_IDS_VIEW_BUILDER.build_and_print()
