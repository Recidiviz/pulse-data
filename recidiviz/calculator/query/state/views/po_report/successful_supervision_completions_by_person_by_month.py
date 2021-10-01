# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Successful supervision completions by person by month."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUCCESSFUL_SUPERVISION_COMPLETIONS_BY_PERSON_BY_MONTH_VIEW_NAME = (
    "successful_supervision_completions_by_person_by_month"
)

SUCCESSFUL_SUPERVISION_COMPLETIONS_BY_PERSON_BY_MONTH_DESCRIPTION = """
 Successful supervision completions by person by month.
 """

# TODO(#4586): Update all references to "discharges" in the PO report dataset to instead be "successful completions"
# TODO(#4155): Use the supervision_termination_metrics instead of the raw state_supervision_period table
# TODO(#4491): Consider using `external_id` instead of `agent_external_id`
SUCCESSFUL_SUPERVISION_COMPLETIONS_BY_PERSON_BY_MONTH_QUERY_TEMPLATE = """
    /*{description}*/
    WITH supervision_periods AS (
      SELECT
        state_code, sp.person_id, supervision_period_id,
        start_date, termination_date, termination_reason,
        EXTRACT(YEAR FROM termination_date) AS year,
        EXTRACT(MONTH FROM termination_date) AS month,
        agent.agent_external_id AS officer_external_id,
      FROM `{project_id}.{state_dataset}.state_supervision_period` sp
      LEFT JOIN `{project_id}.{reference_views_dataset}.supervision_period_to_agent_association` agent
        USING (state_code, supervision_period_id)
      -- Only the following supervision types should be included in the PO report
      WHERE supervision_type IN ('DUAL', 'PROBATION', 'PAROLE', 'INTERNAL_UNKNOWN')
      AND agent.agent_external_id IS NOT NULL
    ),
    successful_supervision_completions AS (
      -- Gather all successful supervision completions from the last 3 years
      SELECT
        *
      FROM supervision_periods
      WHERE termination_date IS NOT NULL
         AND termination_reason IN ('COMMUTED', 'DISMISSED', 'DISCHARGE', 'EXPIRATION', 'PARDONED')
         AND EXTRACT(YEAR FROM termination_date) >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
    ),
    overlapping_open_period AS (
      -- Find the supervision periods that should not be counted as completions because of another overlapping period
      SELECT
        p1.supervision_period_id
      FROM successful_supervision_completions p1
      JOIN supervision_periods p2
        USING (state_code, person_id)
      WHERE p1.supervision_period_id != p2.supervision_period_id
        -- Find any overlapping supervision period (started on or before the termination date,
        -- ended after the termination date)
        AND p2.start_date <= p1.termination_date
        AND p1.termination_date < COALESCE(p2.termination_date, CURRENT_DATE())
        -- Count the period as an overlapping open period if it wasn't discharged in the same month
        AND (p2.termination_reason NOT IN ('DISCHARGE', 'EXPIRATION')
            OR DATE_TRUNC(p2.termination_date, MONTH) != DATE_TRUNC(p1.termination_date, MONTH))
    )
    SELECT DISTINCT
      state_code, person_id, year, month, officer_external_id,
      termination_date AS successful_completion_date
    FROM successful_supervision_completions
    LEFT JOIN overlapping_open_period USING (supervision_period_id)
    WHERE overlapping_open_period.supervision_period_id IS NULL
    """

SUCCESSFUL_SUPERVISION_COMPLETIONS_BY_PERSON_BY_MONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=SUCCESSFUL_SUPERVISION_COMPLETIONS_BY_PERSON_BY_MONTH_VIEW_NAME,
    should_materialize=True,
    view_query_template=SUCCESSFUL_SUPERVISION_COMPLETIONS_BY_PERSON_BY_MONTH_QUERY_TEMPLATE,
    description=SUCCESSFUL_SUPERVISION_COMPLETIONS_BY_PERSON_BY_MONTH_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    state_dataset=dataset_config.STATE_BASE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUCCESSFUL_SUPERVISION_COMPLETIONS_BY_PERSON_BY_MONTH_VIEW_BUILDER.build_and_print()
