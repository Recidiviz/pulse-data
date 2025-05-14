# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Query that generates incarceration incidents in Utah."""

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.ingest.direct.regions.us_ut.ingest_views.common_code_constants import (
    INCARCERATION_LOCATION_TYPE_CODES,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH
-- build base incdnt (incident) view
incidents AS (
    SELECT
        -- incident id (external id)
        incdnt_id,
        -- incident location info
        incdnt_loc_std_des as incident_location,
        incdnt_loc_desc as location_within_facility,
        -- incident type info
        incdnt_cat_desc,
        -- incident date
        CAST(CAST(incdnt_dt AS DATETIME) AS DATE) AS incdnt_date,
        -- incident description
        incdnt_title,
        incdnt_smry,
        -- person info
        ofndr_num
    FROM {{incdnt}}
    LEFT JOIN {{incdnt_cd}}
    USING (incdnt_cd)
    LEFT JOIN {{incdnt_cat_cd}}
    USING (incdnt_cat_cd)
    LEFT JOIN {{incdnt_loc_cd}}
    USING (incdnt_loc_cd)
    LEFT JOIN (
        SELECT DISTINCT -- distinct as it's possible to have multiple entries for each triplet
            incdnt_id,
            ofndr_num,
            incdnt_cntct_role
        FROM {{incdnt_cntct}}
    )
    USING (incdnt_id)
    LEFT JOIN {{incdnt_cntct_role}}
    USING (incdnt_cntct_role)
    LEFT JOIN {{body_loc_cd}} incdnt_loc
    ON (incdnt_loc_std_des = incdnt_loc.body_loc_desc)
    WHERE 
        -- filter down to incidents that occurred in incarceration location types
        (incdnt_loc.loc_typ_cd IN ({list_to_query_string(INCARCERATION_LOCATION_TYPE_CODES, quoted=True)})
        OR incdnt_loc_desc LIKE "%JAIL%"
        OR incdnt_loc_desc LIKE "%CORR FACILTY%"
        OR incdnt_loc_desc = 'DIAGNOSTIC')
        -- some of the incdnt contacts are for jii who involved as non-suspects
        AND cntct_role_desc IN ("UNKNOWN", "SUSPECT")
),
-- build base dscpln (outcome) view
outcomes AS (
    SELECT 
        -- case ids 
        udc_dcpln_case_num,
        ofndr_num,
        incdnt_id,
        dcpln_hrng_id,
        dcpln_hrng_rslt_id,
        -- info about case
        CAST(CAST(filing_dt AS DATETIME)AS DATE) AS filing_date,
        -- info about hearing
        CAST(CAST(final_hrng_dt AS DATETIME)AS DATE) AS final_hearing_date,
        hrng_typ_desc,
        -- info about outcome date
        CAST(CAST(rslt.strt_dt AS DATETIME)AS DATE) AS outcome_start_date,
        CAST(CAST(rslt.end_dt AS DATETIME)AS DATE) AS outcome_end_date,
        -- info about sanction outcome severity
        rstrctn_days,
        snctn_desc AS sanction_description,
        hrng_ord AS hearing_outcome_description
    FROM {{dcpln_case}}
    LEFT JOIN {{dcpln_hrng}}
    USING (udc_dcpln_case_num)
    LEFT JOIN {{dcpln_hrng_rslt}} rslt
    USING (dcpln_hrng_id)
    LEFT JOIN {{snctn}}
    USING (snctn_cd)
    LEFT JOIN {{dcpln_hrng_typ_cd}}
    USING (dcpln_hrng_typ_cd)
)
SELECT *
FROM incidents
LEFT JOIN outcomes 
USING(incdnt_id, ofndr_num)
WHERE ofndr_num IS NOT NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ut",
    ingest_view_name="incarceration_incident",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
