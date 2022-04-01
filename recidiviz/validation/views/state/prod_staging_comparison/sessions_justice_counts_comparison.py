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

"""Part 1 of creating a view comparing sessions population counts in prod and staging
to the justice counts population counts. This view combines sessions + justice counts in both prod and staging separately.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.justice_counts.dataset_config import (
    JUSTICE_COUNTS_DASHBOARD_DATASET,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SESSIONS_JUSTICE_COUNTS_COMPARISON_VIEW_NAME = "sessions_justice_counts_comparison"

SESSIONS_JUSTICE_COUNTS_COMPARISON_DESCRIPTION = """
Comparison of justice counts and sessions data on prison, parole, and probation populations (project agnostic)
"""

JUSTICE_COUNTS_ANNUAL_ONLY_STATES = ["US_ID"]

SESSIONS_JUSTICE_COUNTS_COMPARISON_QUERY_TEMPLATE = """
/* Inner joins sessions and justice counts population for prison, parole, and probation compartments. */

    SELECT
      state_code,
      metric,
      jc.date_reported,
      MAX(jc.value) AS justice_counts_value, --all rows when grouped should have same justice counts value, so can take whatever aggregation
      COUNT(*) AS sessions_value,
    FROM (
      SELECT
        state_code,
        metric,
        date_reported,
        value
      FROM `{project_id}.{justice_counts_dataset}.unified_corrections_metrics_monthly`
      UNION ALL
      SELECT
        state_code,
        metric,
        date_reported,
        value
      FROM `{project_id}.{justice_counts_dataset}.unified_corrections_metrics_annual`
      WHERE state_code IN ('{annual_only_states}')
    ) jc

    INNER JOIN (
      -- one to many join for all observations that have Justice Counts dates between their start and end sessions dates

      SELECT
        state_code,
        start_date,
        end_date,
        CASE
            -- Only include the in-state correctional compartments
            WHEN compartment_level_1 = "INCARCERATION" THEN "POPULATION_PRISON"
            WHEN compartment_level_1 = "SUPERVISION" THEN
              CASE
                WHEN compartment_level_2 IN ("PAROLE", "DUAL") THEN "POPULATION_PAROLE"
                WHEN compartment_level_2 = "PROBATION"
                    -- Justice Counts probation data collection process in TN includes
                    -- Community Confinement unlike other states
                    OR (state_code = "US_TN" AND compartment_level_2 = "COMMUNITY_CONFINEMENT")
                    THEN "POPULATION_PROBATION"
              END
            END AS metric
      FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
      ) sessions
    USING (state_code, metric)
    WHERE jc.date_reported BETWEEN sessions.start_date AND COALESCE(sessions.end_date,'9999-01-01')
    GROUP BY 1, 2, 3

"""

SESSIONS_JUSTICE_COUNTS_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SESSIONS_JUSTICE_COUNTS_COMPARISON_VIEW_NAME,
    view_query_template=SESSIONS_JUSTICE_COUNTS_COMPARISON_QUERY_TEMPLATE,
    description=SESSIONS_JUSTICE_COUNTS_COMPARISON_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    justice_counts_dataset=JUSTICE_COUNTS_DASHBOARD_DATASET,
    annual_only_states="', '".join(JUSTICE_COUNTS_ANNUAL_ONLY_STATES),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SESSIONS_JUSTICE_COUNTS_COMPARISON_VIEW_BUILDER.build_and_print()
