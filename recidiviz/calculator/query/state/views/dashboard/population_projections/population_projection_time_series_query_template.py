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
"""Population Projection time series template"""


def population_projection_query(compartment: str) -> str:
    return f"""
        /*{{description}}*/
        WITH prepared_data AS (
          SELECT
            * EXCEPT (simulation_group),
            simulation_group as gender,
            ABS((year - {{cur_year}}) * 12 + (month - {{cur_month}})) as offset,
          FROM `{{project_id}}.{{population_projection_dataset}}.microsim_projection`
        )
    
        SELECT
          year,
          month,
          legal_status,
          gender,
          state_code,
          simulation_tag,
          total_population,
          total_population_min,
          total_population_max,
        FROM prepared_data
        WHERE compartment = '{compartment}'
            AND offset <= 60
        ORDER BY year, month
    """
