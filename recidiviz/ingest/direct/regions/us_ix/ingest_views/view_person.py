# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query that generates the state person entity using the following tables: ind_Offender, ind_Offender_Address,
ref_Address, ref_AddressType, ind_AliasName, ind_AliasNameType, ref_NameSuffixType, ind_Race, ind_Gender, ind_EthnicOrigin"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    WITH current_address AS (
        # How current_address is determined:
        #   if PrimaryAddress = True then use that as current_address (never a case where an individual has more than 1 primary address)
        #   if individual doesn't have a Primary Address listed, then use the address with the most recent start date
        #   if there are ties, then pick the address with a null end date.  If all addresses in the tie have enddates, pick the most recent end date.
        #   if there are still ties in this scenario, sort by AddressId and break tie using ROW_NUMBER()
        SELECT * EXCEPT (address_priority)
        FROM (
          SELECT
              OffenderId,
              o.AddressId,
              PrimaryAddress,
              StartDate,
              EndDate,
              a.StreetNumber,
              a.StreetName,
              -- In Atlas, the city / county association fields are named confusingly /
              -- backwards, so CountyId actually leads us to the location record of the
              -- relevant city for the address.
              COALESCE(county.LocationName, a.OutOfStatePlace) AS LocationName,
              s.LocationCode AS State_LocationCode,
              a.ZipCode,
              ROW_NUMBER() OVER (
                  PARTITION BY OffenderId
                  ORDER BY PrimaryAddress DESC, StartDate DESC NULLS LAST, EndDate DESC NULLS FIRST, o.AddressId
              ) AS address_priority
          FROM {ind_Offender_Address} o
          LEFT JOIN {ref_Address} a
              ON o.AddressID = a.AddressID
          LEFT JOIN {ref_Location} jurisdiction
              ON a.JurisdictionId = jurisdiction.LocationId
          LEFT JOIN {ref_Location} county
              ON a.CountyId = county.LocationId
          LEFT JOIN {ref_Location} s
              ON a.StateId = s.LocationId
        ) a
        WHERE address_priority = 1
    ),
    alias_records AS (
        SELECT 
            n.OffenderId,
            n.FirstName,
            n.MiddleName,
            n.LastName,
            n.NameSuffixTypeId,
            r.NameSuffixTypeDesc,
            n.AliasNameTypeId,
            t.AliasNameTypeDesc
        FROM {ind_AliasName} n 
        LEFT JOIN {ind_AliasNameType} t
            USING(AliasNameTypeId)
        LEFT JOIN {ref_NameSuffixType} r
            USING(NameSuffixTypeId)
    ),
    # using AliasNameTypeDesc = Default for full_name on person since there's one default name per individual
    name_records AS (
        SELECT *
        FROM alias_records
        WHERE AliasNameTypeId = '0'
    ),
    -- confirmed that all phone numbers are of length 10
    -- using the below ranking system, there will only ever be one phone number per person
    -- ranking system: take most recently updated primary phone, otherwise take most recently updated phone overall
    --                 if there are still ties, break tie randomly using Offender_PhoneId
    current_phone_numbers as (
        SELECT     
            OffenderId,
            PhoneNumber,
        FROM (
            SELECT
            OffenderId,
            PrimaryPhone,
            off_phone.UpdateDate,
            PhoneNumber,
            RANK() OVER(PARTITION BY OffenderId ORDER BY PrimaryPhone desc, CAST(off_phone.UpdateDate as DATETIME) desc, Offender_PhoneId desc) as rnk
            FROM {ind_Offender_Phone} off_phone 
                LEFT JOIN {ref_Phone} phone on off_phone.PhoneId = phone.PhoneId
        ) sub1
        WHERE rnk = 1
    )

    SELECT
        o.OffenderId,
        a.StreetNumber,
        a.StreetName,
        a.LocationName,
        a.State_LocationCode,
        a.ZipCode,
        n.FirstName,
        n.MiddleName,
        n.LastName,
        n.NameSuffixTypeDesc,
        CAST(SUBSTR(o.BirthDate, 1, 10) AS DATE) AS BirthDate,
        g.GenderDesc,
        r.RaceDesc,
        e.EthnicOriginDesc,
        p.PhoneNumber,
        o.EmailAddress
    FROM {ind_Offender} o
    LEFT JOIN {ind_Race} r
        ON o.RaceId = r.RaceId
    LEFT JOIN current_address a
        ON o.OffenderId = a.OffenderId
    LEFT JOIN name_records n
        ON o.OffenderId = n.OffenderId
    LEFT JOIN {ind_Gender} g
        ON o.GenderId = g.GenderId
    LEFT JOIN {ind_EthnicOrigin} e
        ON o.EthnicOriginId = e.EthnicOriginId
    LEFT JOIN current_phone_numbers p
        ON o.OffenderId = p.OffenderId
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix", ingest_view_name="person", view_query_template=VIEW_QUERY_TEMPLATE
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
