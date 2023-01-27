#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
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
#  =============================================================================
"""CTE Logic that is shared across US_ID Workflows queries."""


def us_id_latest_phone_number() -> str:
    return """
    SELECT
        "US_ID" AS state_code,
        pei.external_id AS person_external_id,
        c.phonenumber AS phone_number,
      FROM `{project_id}.{us_id_raw_data}.cis_personphonenumber_latest` a
      LEFT JOIN `{project_id}.{us_id_raw_data}.cis_codephonenumbertype_latest` b
        ON a.codephonenumbertypeid = b.id
      LEFT JOIN `{project_id}.{us_id_raw_data}.cis_phonenumber_latest` c
        ON a.phonenumberid = c.id
      LEFT JOIN `{project_id}.{us_id_raw_data}.cis_offenderphonenumber_latest` d
        ON a.id = d.id
      LEFT JOIN `{project_id}.{us_id_raw_data}.cis_offender_latest` e
        ON a.personid = e.id
      INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
        ON e.offendernumber = pei.external_id
      WHERE b.active = 'T'
        AND a.primaryphone = 'T'
        AND pei.state_code="US_ID"
      QUALIFY ROW_NUMBER() OVER(PARTITION BY pei.person_id ORDER BY d.startdate)=1
    """
