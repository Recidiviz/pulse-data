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
"""Query containing supervision staff information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT 
    DOC_ID,
    CYCLE_NO,
    se.ENROLLMENT_REF_ID,
    se.CLASS_REF_ID,
    se.ENROLLMENT_STATUS_CD,
    NULLIF(se.REFERRAL_DT, '7799-12-31') AS REFERRAL_DT,
    se.REFERRED_BY_USER_REF_ID,
    NULLIF(ACTUAL_START_DT, '7799-12-31') AS ACTUAL_START_DT,
    NULLIF(ACTUAL_EXIT_DT, '7799-12-31') AS ACTUAL_EXIT_DT,
    classes.CLASS_TITLE,
    l.LOC_ACRONYM
FROM {OFNDR_PDB_CLASS_SCHEDULE_ENROLLMENTS} se
LEFT JOIN {OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF} xref
USING (OFNDR_CYCLE_REF_ID)
LEFT JOIN {MASTER_PDB_CLASSES} classes
USING (CLASS_REF_ID)
LEFT JOIN {MASTER_PDB_CLASS_SCHEDULES} s
USING (CLASS_SCHEDULE_REF_ID,CLASS_REF_ID)
LEFT JOIN {MASTER_PDB_CLASS_LOCATIONS} cl
USING (CLASS_REF_ID,CLASS_LOCATION_REF_ID)
LEFT JOIN {MASTER_PDB_LOCATIONS} l
USING (LOC_REF_ID)
WHERE 
    se.DELETE_IND = 'N' 
    AND xref.DELETE_IND = 'N' 
    AND classes.DELETE_IND = 'N'
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="program_assignment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
