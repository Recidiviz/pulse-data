#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#   =============================================================================
"""A template for building views to find people who transition out of supervision."""

from typing import List


def supervision_transition_template(outflow_compartments: List[str]) -> str:
    return f"""
      SELECT
        state_code,
        person_id,
        end_date AS transition_date,
        IF(compartment_level_2 = 'DUAL', 'PAROLE', compartment_level_2) AS supervision_type,
        gender,
        supervising_officer_external_id_end as supervising_officer,
      FROM
        `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized`
      WHERE
        state_code = 'US_ID'
        AND compartment_level_1 = 'SUPERVISION'
        AND compartment_level_2 IN ('PAROLE', 'PROBATION', 'INFORMAL_PROBATION', 'BENCH_WARRANT', 'DUAL', 'ABSCONSION')
        AND outflow_to_level_1 IN ('{"', '".join(outflow_compartments)}')
        AND end_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 64 MONTH)
        -- (5 years X 12 months) + (3 for 90-day avg) + (1 to capture to beginning of first month) = 64 months
    """
