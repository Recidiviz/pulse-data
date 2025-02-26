# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Query for person date of birth info."""
from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import \
    DirectIngestPreProcessedIngestViewBuilder
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH current_address_view AS (
    SELECT * EXCEPT(row_num) FROM
        (SELECT
          personid,
          ARRAY_TO_STRING([line1, city, state_abbreviation, {cis_personaddress}.zipcode], ', ') AS current_address,
          ROW_NUMBER() OVER (PARTITION BY personid ORDER BY startdate DESC) as row_num
        FROM
            {cis_offenderaddress}
        FULL OUTER JOIN
            {cis_personaddress}
        ON
            id = personaddressid
        LEFT JOIN
            `{{project_id}}.reference_tables.state_ids`
        ON
            codestateid = CAST(state_id AS STRING)
        WHERE personid IS NOT NULL
            AND {cis_offenderaddress}.validaddress = 'T' -- Valid address
            AND {cis_offenderaddress}.enddate IS NULL -- Active address
            AND {cis_personaddress}.codeaddresstypeid IN ('1')) -- Physical address
    WHERE row_num = 1)


SELECT
    *
    EXCEPT(updt_dt, updt_usr_id)  # Seem to update every week? (table is generated)
FROM
    {offender}
LEFT JOIN
    {ofndr_dob}
ON
  {offender}.docno = {ofndr_dob}.ofndr_num
LEFT JOIN
    current_address_view
ON
  current_address_view.personid = {offender}.docno
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region='us_id',
    ingest_view_name='offender_ofndr_dob_address',
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols='docno'
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
