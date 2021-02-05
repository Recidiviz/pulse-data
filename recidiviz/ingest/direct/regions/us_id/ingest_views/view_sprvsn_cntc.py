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
"""Query for supervision contacts for a person."""
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import \
    DirectIngestPreProcessedIngestViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """SELECT
    *
    EXCEPT(vld_flg, lastoffcontact) # Unused and repeated across tables
FROM
    {sprvsn_cntc}
LEFT JOIN
    {cntc_loc_cd}
USING 
    (cntc_loc_cd)
LEFT JOIN
    {cntc_rslt_cd}
USING 
    (cntc_rslt_cd)
LEFT JOIN
    {cntc_typ_cd}
USING 
    (cntc_typ_cd)
LEFT JOIN
    {cntc_title_cd}
USING 
    (cntc_title_cd)
LEFT JOIN
    {applc_usr}
ON
    udc_agnt1_id = usr_id
WHERE
    cntc_typ_desc IN ('FACE TO FACE', 'VIRTUAL')
    # TOOD(3394): Ingest all of contacts not just those since 2019 once we have time / need.
    AND CAST(cntc_dt AS DATETIME) > CAST('2019-01-01' AS DATETIME) 
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region='us_id',
    ingest_view_name='sprvsn_cntc',
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols='ofndr_num, cntc_dt'
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
