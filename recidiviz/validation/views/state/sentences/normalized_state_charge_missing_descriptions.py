# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""A validation view that contains all of the normalized state charges that are missing
offense descriptions."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

NORMALIZED_STATE_CHARGE_MISSING_DESCRIPTIONS_VIEW_NAME = (
    "normalized_state_charge_missing_descriptions"
)

NORMALIZED_STATE_CHARGE_MISSING_DESCRIPTIONS_DESCRIPTION = """
Normalized state charge rows with null offense descriptions, impacting
Workflows auto-filled forms and any analysis by offense type.
"""

NORMALIZED_STATE_CHARGE_MISSING_DESCRIPTIONS_QUERY_TEMPLATE = """
SELECT
  state_code AS region_code,
  person_id,
  charge_id,
  external_id,
FROM `{project_id}.{normalized_state_dataset}.state_charge`
WHERE description IS NULL
"""

NORMALIZED_STATE_CHARGE_MISSING_DESCRIPTIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=NORMALIZED_STATE_CHARGE_MISSING_DESCRIPTIONS_VIEW_NAME,
    view_query_template=NORMALIZED_STATE_CHARGE_MISSING_DESCRIPTIONS_QUERY_TEMPLATE,
    description=NORMALIZED_STATE_CHARGE_MISSING_DESCRIPTIONS_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        NORMALIZED_STATE_CHARGE_MISSING_DESCRIPTIONS_VIEW_BUILDER.build_and_print()
