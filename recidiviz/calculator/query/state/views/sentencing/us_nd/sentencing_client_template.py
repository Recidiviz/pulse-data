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
-- This join is most likely buggy, but can be used as an example of how to connect these data
-- to LSIR scores that have been associated with this person in the past. There is no LSIR
-- field on the PSI data specifically, so it is probably easiest to use this sessions view
-- of LSI raw data from ND. 
LEFT JOIN
    `{project_id}.{sessions_dataset}.us_nd_raw_lsir_assessments` lsir
ON
-- I joined on the person, and then also limited the LSI results to those that were created
-- before the PSI was due. This still generated what I would expect to be far too many results; 
-- maybe we want to find the LSI results that were logged closest to the PSI due date? 
    (lsir.person_id = p.person_id 
    AND lsir.assessment_date < pARSE_DATETIME('%m/%d/%Y  %I:%M:%S %p', psi.DATE_DUE))
"""
