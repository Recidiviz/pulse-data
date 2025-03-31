# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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

"""Query containing supervision violation information. There is quite a bit more
information on violations that could be ingested, but keeping this barebones as time is
of the essence."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT 
    -- Basic violation and sanction info
    rim_violation_rove_id,
    rove.ofndr_num,
    rvr.violation_dt, 
    rvr.violation_desc,
    sh.created_dt as sanction_history_create_date,
    rsc.rim_status_desc,
    sc.sanction_desc,

    -- RIM Level ID seems to reflect the level of the violation while sanction_lvl
    -- represents the level of the sanction.
    rvc.rim_level_id,
    sc.sanction_level,

    -- Useful for debugging
    rim_ofndr_violation_event_id

-- Get violation information
FROM {rim_ofndr_violation_event} rove
LEFT JOIN {rim_violation_rove} rvr USING (rim_ofndr_violation_event_id)
LEFT JOIN {rim_violation_cd} rvc USING (rim_violation_id)

-- Get sanction information
LEFT JOIN {rim_sanction_history} sh USING (rim_ofndr_violation_event_id)
LEFT JOIN {rim_sanction_cd} sc USING (rim_sanction_id)
LEFT JOIN {rim_status_cd} rsc USING (rim_status_id)

WHERE 
    -- Only include non-dismissed violations
    rvr.stricken_dismissed_flg = 'false'

    -- Only include violations that are finalized -- there are retracted violations we
    -- ignore because of this, but I think that's good. Check out the rim_sanction_cd
    -- table for more info on what's being excluded
    AND (rsc.pending_flg = 'false' AND (rsc.final_state_flg = 'true' OR rsc.denied_flg = 'true'))

    -- 925 times there is no mapping between the violation event and the violation to
    -- code mapping table -- I think this is a bug in the data
    AND rvr.rim_violation_rove_id IS NOT NULL

-- Each rim_ofndr_violation_event_id maps to multiple violations
-- (rim_violation_rove_id), but all violations associated with the same
-- rim_ofndr_violation_event_id are sanctioned as a group. Below, we get the most recent
-- status for each violation. However, because they are sanctioned as a gorup, we should
-- expect that the statuses each violation within a group will be the same.
QUALIFY ROW_NUMBER() OVER (PARTITION BY rim_violation_rove_id ORDER BY sh.created_dt DESC) = 1
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="US_UT",
    ingest_view_name="supervision_violation",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
