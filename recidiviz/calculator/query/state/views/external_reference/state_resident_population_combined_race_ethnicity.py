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
"""Combines the race and ethnicity columns using our prioritization logic."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    EXTERNAL_REFERENCE_VIEWS_DATASET,
)

STATE_RESIDENT_POPULATION_COMBINED_RACE_ETHNICITY_QUERY_TEMPLATE = """
SELECT
  p.state_code,
  age_group,
  -- Note, this works differently than the legacy logic did. That would count folks who
  -- are Black and Hispanic twice, and drop someone from the majority group to maintain
  -- the same total count. This instead will count them as either Black or Hispanic,
  -- whichever is least represented, similar to how prioritization works in Dataflow.
  IF(race_priority.representation_priority < IFNULL(ethnicity_priority.representation_priority, 100),
      race,
      ethnicity
  ) AS race_or_ethnicity,
  gender,
  SUM(population) AS population
FROM `{project_id}.{external_reference_views_dataset}.state_resident_population` p
LEFT JOIN `{project_id}.{external_reference_views_dataset}.state_resident_population_combined_race_ethnicity_priority` race_priority
ON p.state_code = race_priority.state_code AND race = race_priority.race_or_ethnicity
LEFT JOIN `{project_id}.{external_reference_views_dataset}.state_resident_population_combined_race_ethnicity_priority` ethnicity_priority
ON p.state_code = ethnicity_priority.state_code AND ethnicity = ethnicity_priority.race_or_ethnicity
GROUP BY 1, 2, 3, 4
"""

STATE_RESIDENT_POPULATION_COMBINED_RACE_ETHNICITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=EXTERNAL_REFERENCE_VIEWS_DATASET,
    view_id="state_resident_population_combined_race_ethnicity",
    description="View over the state resident population data that combines race and "
    "ethnicity into a single prioritized column.",
    view_query_template=STATE_RESIDENT_POPULATION_COMBINED_RACE_ETHNICITY_QUERY_TEMPLATE,
    external_reference_views_dataset=EXTERNAL_REFERENCE_VIEWS_DATASET,
)
