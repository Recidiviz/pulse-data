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

"""Query containing incarceration incident information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT 
    control_number,
    misconduct_number,
    misconduct_date,
    institution,
    report_date,
    hearing_after_date,
    place_hvl_code,
    place_extended,
    ctgory_of_chrgs_1,
    ctgory_of_chrgs_2,
    ctgory_of_chrgs_3,
    ctgory_of_chrgs_4,
    ctgory_of_chrgs_5,
    confinement,
    confinement_date,
    drug_related
FROM {dbo_Miscon}
JOIN
-- In 20-ish cases, the control_number in dbo_Miscon does not correspond to any control number in 
-- dbo_tblSearchInmateInfo. We ignore these ids - we only ingest information about people listed in 
-- dbo_tblSearchInmateInfo;
(SELECT DISTINCT control_number FROM {dbo_tblSearchInmateInfo}) ids
USING (control_number)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="dbo_Miscon",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
