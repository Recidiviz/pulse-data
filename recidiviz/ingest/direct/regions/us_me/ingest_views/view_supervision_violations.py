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
"""Query containing MEDOC supervision violation information.
"""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT 
    violation.Violation_Id,
    violation.Cis_100_Client_Id as Client_Id,
    violation.Start_Date,
    violation.Finding_Date,
    violation_finding.Violation_Finding_Descr_Tx,
    violation_type.Sent_Calc_Sys_Desc as Violation_Type_Desc,
    disposition.Sent_Calc_Sys_Desc as Disposition_Desc,
    served_at.Sent_Calc_Sys_Desc as Served_At_Desc,
FROM {CIS_480_VIOLATION} violation
LEFT JOIN {CIS_4800_VIOLATION_FINDING} violation_finding ON violation.Cis_4800_Violation_Finding_Cd = violation_finding.Violation_Finding_Cd
LEFT JOIN {CIS_4009_SENT_CALC_SYS} violation_type ON violation.Cis_4009_Violation_Type_Cd = violation_type.Sent_Calc_Sys_Cd
LEFT JOIN {CIS_4009_SENT_CALC_SYS} disposition on violation.Cis_4009_Disposition_Cd = disposition.Sent_Calc_Sys_Cd
LEFT JOIN {CIS_4009_SENT_CALC_SYS} served_at on violation.Cis_4009_Served_At_Cd = served_at.Sent_Calc_Sys_Cd
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_me",
    ingest_view_name="supervision_violations",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="Cis_100_Client_Id, Violation_Id",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
