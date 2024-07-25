# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""View logic to prepare US_ND supervision staff data for Workflows"""

from typing import Optional

from recidiviz.calculator.query.state.views.workflows.us_me.staff_template import (
    get_caseload_staff_ids_query,
)


def build_us_nd_staff_template(
    caseload_source_table: str,
    columns_minus_supervisor_id: Optional[str] = "{columns}",
) -> str:
    """Builds the US_ND incarceration staff templates."""
    return f"""
        WITH
        caseload_staff_ids AS (
            {get_caseload_staff_ids_query(caseload_source_table, 'US_ND')}
        ),
        caseload_staff AS (
            SELECT DISTINCT
                ids.id,
                ids.state_code,
                CAST(NULL AS STRING) AS district,
                CAST(NULL AS STRING) AS email,
                UPPER(state_table.FIRST_NAME) as given_names,
                UPPER(state_table.LAST_NAME) as surname,
                CAST(NULL AS STRING) AS role_subtype,
            FROM caseload_staff_ids ids
            INNER JOIN `{{project_id}}.{{us_nd_raw_data_up_to_date_dataset}}.recidiviz_elite_staff_members_latest` state_table
                ON REPLACE(REPLACE(state_table.STAFF_ID, '.00',''), ',', '') = ids.id
                    AND ids.state_code = 'US_ND'
        )
        SELECT 
            {columns_minus_supervisor_id}
        FROM caseload_staff
"""
