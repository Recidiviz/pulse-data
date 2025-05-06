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
the uniform/state-agnostic offense labels from CJARS."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

NORMALIZED_STATE_CHARGE_MISSING_UNIFORM_OFFENSE_LABELS_VIEW_NAME = (
    "normalized_state_charge_missing_uniform_offense_labels"
)

NORMALIZED_STATE_CHARGE_MISSING_UNIFORM_OFFENSE_LABELS_DESCRIPTION = """
Normalized state charge rows found without any uniform/state-agnostic offense
information. This occurs when the state charge description string is not located in
the `gcs_backed_tables.offense_description_to_labels` mapping table.
This can be caused because the CJARS script needs to be rerun with the latest state
charge descriptions:
https://docs.google.com/document/d/1f1QDDAwSST82io1dFxaRZHZLcnawfzQLQlqyodAYCnk/edit
"""

NORMALIZED_STATE_CHARGE_MISSING_UNIFORM_OFFENSE_LABELS_QUERY_TEMPLATE = """
SELECT
  state_code,
  state_code AS region_code,
  person_id,
  charge_id,
  external_id,
  description,
FROM `{project_id}.normalized_state.state_charge` charge
LEFT JOIN `{project_id}.reference_views.cleaned_offense_description_to_labels` charge_labels
ON charge.description = charge_labels.offense_description
WHERE charge.description IS NOT NULL AND charge_labels.offense_description IS NULL
"""

NORMALIZED_STATE_CHARGE_MISSING_UNIFORM_OFFENSE_LABELS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=NORMALIZED_STATE_CHARGE_MISSING_UNIFORM_OFFENSE_LABELS_VIEW_NAME,
    view_query_template=NORMALIZED_STATE_CHARGE_MISSING_UNIFORM_OFFENSE_LABELS_QUERY_TEMPLATE,
    description=NORMALIZED_STATE_CHARGE_MISSING_UNIFORM_OFFENSE_LABELS_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        NORMALIZED_STATE_CHARGE_MISSING_UNIFORM_OFFENSE_LABELS_VIEW_BUILDER.build_and_print()
