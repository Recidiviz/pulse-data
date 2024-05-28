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
"""View logic to prepare US_IX Sentencing clients data for PSI tools"""

US_IX_SENTENCING_CLIENT_TEMPLATE = """
    WITH
    -- Gets the most recent address on record for JII
    recent_jii_address AS(
        SELECT 
            *,
            ROW_NUMBER() OVER(
                PARTITION BY OffenderId 
                ORDER BY StartDate DESC, Offender_AddressId DESC
            ) AS recency_rank
        FROM `{project_id}.{us_ix_raw_data_up_to_date_dataset}.ind_Offender_Address_latest`
    ),
    -- Create array of all the external Ids of cases where this person is the client
    caseIds AS (
        SELECT DISTINCT
            OffenderId,
            STRING_AGG(PSIReportId, ',') OVER (PARTITION BY OffenderId ORDER BY UpdateDate) as caseIds
        FROM  `{project_id}.{us_ix_raw_data_up_to_date_dataset}.com_PSIReport_latest`
    )
    SELECT DISTINCT
        psi.OffenderId AS external_id,
        person.full_name,
        person.birthdate AS birth_date,
        person.gender,
        UPPER(loc.LocationName) AS county,
        "US_IX" AS state_code,
        CONCAT('[', caseIds,']') AS caseIds
    FROM `{project_id}.{us_ix_raw_data_up_to_date_dataset}.com_PSIReport_latest` psi
    LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id`  id 
        ON psi.OffenderId = id.external_id and id_type = 'US_IX_DOC'
    LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person` person 
        ON person.person_id = id.person_id
    LEFT JOIN recent_jii_address address 
        ON address.OffenderId = psi.OffenderId
    LEFT JOIN `{project_id}.{us_ix_raw_data_up_to_date_dataset}.ref_Address_latest` ref 
        ON address.AddressId = ref.AddressId
    LEFT JOIN `{project_id}.{us_ix_raw_data_up_to_date_dataset}.ref_Location_latest` loc 
        ON loc.LocationId = ref.CountyId
    LEFT JOIN caseIds c 
        ON psi.OffenderId = c.OffenderId
    WHERE recency_rank = 1

"""
