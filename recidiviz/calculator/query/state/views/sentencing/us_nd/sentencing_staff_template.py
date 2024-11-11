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
"""View logic to prepare US_ND SENTENCING staff data for PSI tools

This view pulls in the following pieces of information about each client that appears in 
ND's PSI data: 
- External ID
- Full name
- Email
- Case IDs (as court case numbers)
"""

US_ND_SENTENCING_STAFF_TEMPLATE = """
SELECT
    DISTINCT AGENT as external_id,
    s.full_name,
    s.email,
    COURT1,
    COURT2,
    COURT3
FROM
    `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_psi_latest`
LEFT JOIN
    `{project_id}.{normalized_state_dataset}.state_staff_external_id` sei
ON
    (AGENT = sei.external_id
    AND id_type = 'US_ND_DOCSTARS_OFFICER')
LEFT JOIN
  `{project_id}.{normalized_state_dataset}.state_staff` s
USING
    (staff_id)

"""
