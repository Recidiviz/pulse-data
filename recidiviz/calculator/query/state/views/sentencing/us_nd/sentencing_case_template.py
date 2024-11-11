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
"""View logic to prepare US_ND Sentencing case data for PSI tools.

See notes in the client record template about connecting LSIR scores to cases.

There are two main sources for translating CST (Common Statute Table, ND-specific statues)
and NCIC (National Crime Information Center) codes into their descriptions. 

We receive a raw data file directly from ND that maps NCIC codes to their descriptions. 
ND believes this file translates CST codes, but none of the codes included are CST codes. 
I joined that reference file to one offense code in this query for an example.

We have also done a good bit of work to map between NCIC / UCCS / NIBRS uniform crime codes
so that we can standardize labels for them. See recidiviz/common/ncic.py for details. 
The accuracy of the ncic_to_nibrs_to_uccs source table for that query is unknown.
"""

US_ND_SENTENCING_CASE_TEMPLATE = """
SELECT
    COURT1,
    COURT2,
    COURT3, 
    AGENT AS staff_id,
    REPLACE(SID,',','') AS client_id,
    DATE_DUE as due_date,
    DATE_ORD AS assigned_date,
    DATE_COM AS completion_date,
    docstars_psi.CST_1, 
    docstars_psi.CST_2,
    docstars_psi.CST_3,
    docstars_psi.CST_4,
    docstars_psi.CST_5,
    docstars_psi.OFF_1 AS NCIC_1, 
    ncic.DESCRIPTION AS NCIC_1_DESCRIPTION,
    docstars_psi.OFF_2 AS NCIC_2,
    docstars_psi.OFF_3 AS NCIC_3,
    docstars_psi.OFF_4 AS NCIC_4,
    docstars_psi.OFF_5 AS NCIC_5,
FROM
    `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_psi_latest`
LEFT JOIN 
    `{project_id}.{us_nd_raw_data_up_to_date_dataset}.recidiviz_docstars_cst_ncic_code_latest` ncic
ON 
    (OFF_1 = CODE)
"""
