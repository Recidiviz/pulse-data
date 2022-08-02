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
"""US_ME - Early Discharge Eligibility"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ME_DAY_0_EARLY_DISCHARGE_VIEW_NAME = "us_me_day_0_early_discharge"

US_ME_DAY_0_EARLY_DISCHARGE_VIEW_DESCRIPTION = (
    """Early Discharge Eligibility Criteria for Maine"""
)

US_ME_DAY_0_EARLY_DISCHARGE_QUERY_TEMPLATE = """
WITH current_probation_population AS (
  SELECT
    cs.person_id,
    external_id AS person_external_id,
    TRIM(CONCAT(
      JSON_EXTRACT_SCALAR(full_name, "$.given_names"),
      " ",
      JSON_EXTRACT_SCALAR(full_name, "$.surname")
    )) AS person_name,
    p.birthdate,
    cs.start_date,
  FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized` cs
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id`
    USING (state_code, person_id)
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person` p
    USING (state_code, person_id)
  WHERE cs.state_code = "US_ME"
    AND cs.end_date IS NULL
    AND cs.compartment_level_2 = "PROBATION"
),
supervision_violations AS (
  SELECT DISTINCT
    person_id,
    violation_date,
  FROM `{project_id}.{state_dataset}.state_supervision_violation`
  WHERE state_code = "US_ME"
)

SELECT p.* EXCEPT (person_id)
FROM current_probation_population p
LEFT JOIN supervision_violations sv
  ON p.person_id = sv.person_id
  AND sv.violation_date >= p.start_date
WHERE sv.person_id IS NULL
ORDER BY start_date, person_external_id
"""

US_ME_DAY_0_EARLY_DISCHARGE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_id=US_ME_DAY_0_EARLY_DISCHARGE_VIEW_NAME,
    dataset_id=ANALYST_VIEWS_DATASET,
    description=US_ME_DAY_0_EARLY_DISCHARGE_VIEW_DESCRIPTION,
    view_query_template=US_ME_DAY_0_EARLY_DISCHARGE_QUERY_TEMPLATE,
    state_dataset=STATE_BASE_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_DAY_0_EARLY_DISCHARGE_VIEW_BUILDER.build_and_print()
