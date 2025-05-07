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
"""Query that collects information about supervision contacts in Utah."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
    sprvsn_cntc_id, 
    sc.ofndr_num,
    loc.cntc_loc_desc,
    rslt.cntc_rslt_desc,
    typ.cntc_typ_desc,
    CAST(cntc_dt AS DATETIME) AS cntc_dt,
    udc_agnt1_id,
    udc_agnt2_id,
    CONCAT(addr.address, ' ', unit, ',', city, ',', st, ' ', zip) AS contact_address
FROM {sprvsn_cntc} sc
LEFT JOIN {cntc_loc_cd} loc
USING(cntc_loc_cd)
LEFT JOIN {cntc_rslt_cd} rslt
USING(cntc_rslt_cd)
LEFT JOIN {cntc_typ_cd} typ
USING(cntc_typ_cd)
LEFT JOIN {ofndr_addr_hist} addr_hist
USING(ofndr_addr_hist_id)
LEFT JOIN {addr} addr
USING(addr_id)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ut",
    ingest_view_name="supervision_contact",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
