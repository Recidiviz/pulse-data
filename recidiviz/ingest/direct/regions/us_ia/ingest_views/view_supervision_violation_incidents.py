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
"""Query containing supervision violation incidents that don't have a corresponding parole or probation report filed"""

from recidiviz.ingest.direct.regions.us_ia.ingest_views.query_fragments import (
    FIELD_RULE_VIOLATIONS_CTE,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = (
    f"WITH {FIELD_RULE_VIOLATIONS_CTE}"
    + """

    SELECT 
        OffenderCd,
        FieldRuleViolationIncidentId,
        IncidentDt,
        FRV_I.EnteredDt
    FROM FRV_I
    LEFT JOIN {IA_DOC_PVR_FRVI}
        USING(FieldRuleViolationIncidentId, OffenderCd)
    LEFT JOIN {IA_DOC_ProbationROVFieldRuleViolationInfo}  
        USING(FieldRuleViolationIncidentId, OffenderCd)
    WHERE ParoleViolationReviewId IS NULL AND ProbationReportOfViolationId IS NULL
"""
)


VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ia",
    ingest_view_name="supervision_violation_incidents",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
