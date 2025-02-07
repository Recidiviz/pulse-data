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
"""Converts the state, race, and ethnicity values into our enum values."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    EXTERNAL_REFERENCE_VIEWS_DATASET,
)
from recidiviz.datasets.static_data.config import EXTERNAL_REFERENCE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def _error(column: str) -> str:
    msg = (
        f"View {EXTERNAL_REFERENCE_VIEWS_DATASET}.state_resident_populations "
        f"has a new value not covered by CASE WHEN statement in the |{column}| column. "
        "Value: "
    )
    # Using pipes because black just hated quoting quotes in a quoted quote
    return f"ERROR(CONCAT('{msg}', '|', {column}, '|'))"


STATE_RESIDENT_POPULATION_QUERY_TEMPLATE = f"""
SELECT
  state_info.state_code,
  age_group, -- This is not normalized consistently across products so leave as is.
  CASE race
    WHEN 'American Indian or Alaska Native' THEN 'AMERICAN_INDIAN_ALASKAN_NATIVE'
    WHEN 'Asian' THEN 'ASIAN'
    WHEN 'Black or African American' THEN 'BLACK'
    WHEN 'Native Hawaiian or Other Pacific Islander' THEN 'NATIVE_HAWAIIAN_PACIFIC_ISLANDER'
    WHEN 'More than one race' THEN 'OTHER'
    WHEN 'White' THEN 'WHITE'
    WHEN 'Not Available' THEN 'EXTERNAL_UNKNOWN'
    ELSE {_error('race')}
  END as race,
  CASE ethnicity
    WHEN 'Hispanic or Latino' THEN 'HISPANIC'
    WHEN 'Not Hispanic or Latino' THEN 'NOT_HISPANIC'
    ELSE {_error('ethnicity')}
  END as ethnicity,
  CASE gender
    WHEN 'Female' THEN 'FEMALE'
    WHEN 'Male' THEN 'MALE'
    ELSE {_error('gender')}
  END as gender,
  population
FROM `{{project_id}}.{{external_reference_dataset}}.state_resident_populations`
LEFT JOIN `{{project_id}}.{{external_reference_views_dataset}}.state_info` state_info
  -- TODO(#10703): Remove this US_IX condition once Atlas is merged into US_ID
  ON state = IF(state_info.state_code = 'US_IX', 'Idaho', state_info.name)
"""

STATE_RESIDENT_POPULATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=EXTERNAL_REFERENCE_VIEWS_DATASET,
    view_id="state_resident_population",
    description="View over the state resident population data that converts state, "
    "race, and ethnicity values into our enum values.",
    view_query_template=STATE_RESIDENT_POPULATION_QUERY_TEMPLATE,
    external_reference_dataset=EXTERNAL_REFERENCE_DATASET,
    external_reference_views_dataset=EXTERNAL_REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STATE_RESIDENT_POPULATION_VIEW_BUILDER.build_and_print()
