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
"""View logic to prepare US_IX SENTENCING staff data for PSI tools"""

US_IX_SENTENCING_STAFF_TEMPLATE = """
WITH 
    -- Create array of all the external Ids of cases where this person is the client
    caseIds AS (
        SELECT DISTINCT
            AssignedToUserId,
            STRING_AGG(PSIReportId, ',' ORDER BY UpdateDate) AS caseIds
        FROM   `{project_id}.{us_ix_raw_data_up_to_date_dataset}.com_PSIReport_latest`
        GROUP BY AssignedToUserId
    )
    SELECT DISTINCT
        psi.AssignedToUserId as external_id,
        staff.full_name,
        staff.email,
        "US_IX" AS state_code,
        CONCAT('[', caseIds,']') AS caseIds
    FROM `{project_id}.{us_ix_raw_data_up_to_date_dataset}.com_PSIReport_latest` psi
    LEFT JOIN `{project_id}.{normalized_state_dataset}.state_staff_external_id` id 
        ON psi.AssignedToUserId = id.external_id and id_type = 'US_IX_EMPLOYEE'
    LEFT JOIN `{project_id}.{normalized_state_dataset}.state_staff` staff 
        ON  staff.staff_id = id.staff_id
    LEFT JOIN caseIds c 
        ON psi.AssignedToUserId = c.AssignedToUserId
"""
