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
"""Query containing offender information."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH annotated_rows AS (
    SELECT 
        *,
        (
            -- Known ND prison / jail addresses
            ADDRESS_UPPERCASE NOT LIKE '%3100 RAILROAD AVE%'
            AND ADDRESS_UPPERCASE NOT LIKE '%NDSP%'
            AND ADDRESS_UPPERCASE NOT LIKE '%PO BOX 5521%'
            AND ADDRESS_UPPERCASE NOT LIKE '%440 MCKENZIE ST%'
            AND ADDRESS_UPPERCASE NOT LIKE '%ABSCOND%'
            AND ADDRESS_UPPERCASE NOT LIKE '%250 N 31ST%'
            AND ADDRESS_UPPERCASE NOT LIKE '%461 34TH ST S%'
            AND ADDRESS_UPPERCASE NOT LIKE '%1600 2ND AVE SW%'
            AND ADDRESS_UPPERCASE NOT LIKE '%702 1ST AVE S%'
            AND ADDRESS_UPPERCASE NOT LIKE '%311 S 4TH ST STE 101%'
            AND ADDRESS_UPPERCASE NOT LIKE '%222 WALNUT ST W%'
            AND ADDRESS_UPPERCASE NOT LIKE '%709 DAKOTA AVE STE D%'
            AND ADDRESS_UPPERCASE NOT LIKE '%113 MAIN AVE E STE B%'
            AND ADDRESS_UPPERCASE NOT LIKE '%712 5TH AVE%'
            AND ADDRESS_UPPERCASE NOT LIKE '%705 EAST HIGHLAND DR. SUITE B%'
            AND ADDRESS_UPPERCASE NOT LIKE '%705 E HIGHLAND DR STE B%'
            AND ADDRESS_UPPERCASE NOT LIKE '%135 SIMS ST STE 205%'
            AND ADDRESS_UPPERCASE NOT LIKE '%638 COOPER AVE%'
            AND ADDRESS_UPPERCASE NOT LIKE '%206 MAIN ST W%'
            AND ADDRESS_UPPERCASE NOT LIKE '%519 MAIN ST STE 8%'
            AND ADDRESS_UPPERCASE NOT LIKE '%115 S 5TH ST STE A%'
            AND ADDRESS_UPPERCASE NOT LIKE '%117 HWY 49%'
            -- Garbage addresses
            AND ADDRESS_UPPERCASE NOT LIKE '%XXX%'
            AND ADDRESS_UPPERCASE NOT LIKE '%***%'
            AND CITY_UPPERCASE NOT IN ('ABSCONDER', 'ABSCONDED')
            -- General filter for addresses that seem to be correctional facilities of some sort
            AND ADDRESS_UPPERCASE NOT LIKE '%JAIL%'
            AND ADDRESS_UPPERCASE NOT LIKE '%PRISON%'
            AND ADDRESS_UPPERCASE NOT LIKE '%CCC%'
            AND ADDRESS_UPPERCASE NOT LIKE '%PENITENTIARY%'
        )  AS is_valid_address
    FROM (
        SELECT 
            *,
            UPPER(ADDRESS) AS ADDRESS_UPPERCASE,
            UPPER(CITY) AS CITY_UPPERCASE
        FROM {docstars_offenders}
    ) people
)
SELECT
    SID,
    ITAGROOT_ID,
    LAST_NAME,
    FIRST,
    MIDDLE,
    IF(is_valid_address, ADDRESS, NULL) AS ADDRESS,
    IF(is_valid_address, CITY, NULL) AS CITY,
    IF(is_valid_address, STATE, NULL) AS STATE,
    IF(is_valid_address, ZIP, NULL) AS ZIP,
    DOB,
    AGENT,
    SUP_LVL,
    SUPER_OVERRIDE,
    PREVIOUS_AGENT,
    RECORD_STATUS,
    COMPLETION_IND,
    ALIASFLAG,
    ADDRESS2,
    CITY2,
    STATE2,
    ZIP2,
    SITEID,
    ABSCONDER,
    SEXOFF,
    GOODTIMEDATE,
    RACE,
    SEX,
    C_MARITAL,
    D_DEP,
    E_LIV_ARR,
    F_VETERAN,
    G_INCOME,
    H_EMPLOY,
    I_JOB_CL,
    J_LAST_GR,
    K_PUB_ASST,
    INACTIVEDATE,
    BIGSIXT1,
    BIGSIXT2,
    BIGSIXT3,
    BIGSIXT4,
    BIGSIXT5,
    BIGSIXT6,
    ACTIVEREVOCATION_IND,
    LSITOTAL,
    CCCFLAG,
    RecDate,
    SORAC_SCORE,
    HOMELESS,
    CREATED_BY,
    RECORDCRDATE,
    LAST_HOME_VISIT,
    LAST_FACE_TO_FACE,
    MAILING_ADDRESS2,
    PHYSICAL_ADDRESS2,
    COUNTY_RESIDENCE,
    EARLY_TERMINATION_DATE,
    EARLY_TERMINATION_ACKNOWLEDGED
FROM annotated_rows;
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_nd",
    ingest_view_name="docstars_offenders",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="SID",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
