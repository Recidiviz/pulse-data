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
"""View logic to prepare US_ND Sentencing clients data for PSI tools

This view pulls in the following pieces of information about each client that appears in 
ND's PSI data: 
- External ID (SID) 
- Full name
- County of Residence
- Case IDs (as court case numbers)
- LSIR score

There are three possible case numbers associated with each client in the raw data, so I included
each of those fields here. 

This view also demonstrates rudimentary join logic to the sessions view that contains
LSIR data from ND. That view pulls directly from raw data, so is like a preprocessed view
for LSIR scores. These are indexed on the individual in ND, not the case - the way I worked
around this is by including a date condition in the JOIN statement to only pull results
that were entered into Docstars before the PSI was due. This information is under the "case record" 
header of the data needs sheet, so should perhaps be moved to that query, but will need 
to be handled creatively to be associated with a particular case. 
"""

US_ND_SENTENCING_CLIENT_TEMPLATE = """
SELECT
  DISTINCT REPLACE(psi.SID, ',', '') AS external_id,
  p.full_name as full_name,
  p.gender,
  p.birthdate,
  off_raw.COUNTY_RESIDENCE as county,
  COURT1,
  COURT2,
  COURT3,
FROM
    `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_psi_latest` psi
LEFT JOIN
    `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
ON
    (REPLACE(psi.SID, ',', '') = external_id
    AND id_type = 'US_ND_SID')
LEFT JOIN
    `{project_id}.{normalized_state_dataset}.state_person` p
USING
    (person_id)
LEFT JOIN 
    `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_offenders_latest` off_raw
ON 
    (REPLACE(psi.SID, ',', '') = off_raw.SID)
"""
