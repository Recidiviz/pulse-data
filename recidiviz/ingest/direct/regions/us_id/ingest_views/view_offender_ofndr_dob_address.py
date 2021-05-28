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
from recidiviz.calculator.query.state.dataset_config import (
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH current_address_view AS (
    SELECT * EXCEPT(row_num) FROM
        (SELECT
          offendernumber,
          ARRAY_TO_STRING([line1, city, state_abbreviation, {{cis_personaddress}}.zipcode], ', ') AS current_address,
          ROW_NUMBER() OVER (PARTITION BY personid ORDER BY startdate DESC) as row_num
        FROM
            {{cis_offenderaddress}}
        FULL OUTER JOIN
            {{cis_personaddress}}
        ON
            id = personaddressid
        FULL OUTER JOIN 
            {{cis_offender}}
        ON
            {{cis_offender}}.id = {{cis_personaddress}}.personid
        LEFT JOIN
            `{{{{project_id}}}}.{STATIC_REFERENCE_TABLES_DATASET}.state_ids`
        ON
            codestateid = CAST(state_id AS STRING)
        WHERE offendernumber IS NOT NULL
            AND {{cis_offenderaddress}}.validaddress = 'T' -- Valid address
            AND {{cis_offenderaddress}}.enddate IS NULL -- Active address
            AND {{cis_personaddress}}.codeaddresstypeid IN ('1')) -- Physical address
    WHERE row_num = 1),
ethnicity AS (
    SELECT
        ofndr_num as docno,
        race_cd
    FROM
        {{ofndr}}
)


SELECT
    *
    EXCEPT(
        updt_dt,
        updt_usr_id,  # Seem to update every week? (table is generated)
        race_cd
    ),
    IF(ethnic_cd IS NULL,
        race_cd,
        IF(race_cd IS NULL OR race_cd = 'U',
            ethnic_cd,
            race_cd
        )
    ) AS race_cd
FROM
    {{offender}}
LEFT JOIN
    {{ofndr_dob}}
ON
  {{offender}}.docno = {{ofndr_dob}}.ofndr_num
LEFT JOIN
    ethnicity
USING (docno)
LEFT JOIN
    current_address_view
ON
  current_address_view.offendernumber = {{offender}}.docno
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_id",
    ingest_view_name="offender_ofndr_dob_address",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="docno",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
