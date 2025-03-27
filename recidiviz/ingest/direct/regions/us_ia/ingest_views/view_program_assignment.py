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
"""
Query containing program assignment information.

In IA, there are interventions and intervention programs.  Interventions are more granular,
and multiple interventions could be under the same umbrella intervention program.
There could also be interventions that aren't connected to an intervention program.
There could also be intervention programs that are standalone and don't have interventions
underneath it.  

For StateProgramAssignment ingest, we'll ingest each individual intervention as a separate
StateProgramAssignment, and then additionally ingest each standalong intervention program
that doesn't have any corresponding interventions as a separate StateProgramAssignment.
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """

    -- Compile all the interventions data and pull in their corresponding 
    -- intervention program data if it exists
    SELECT 
        i.OffenderInterventionProgramId,
        i.OffenderInterventionId,
        i.OffenderCd,
        i.InterventionLocationId,
        ip.InterventionProgram,
        i.Intervention,
        i.InterventionRequired,
        ip.InterventionProgramRequired,
        i.ReferringStaffId,
        DATE(i.InterventionStartDt) AS InterventionStartDt,
        DATE(i.InterventionEndDt) AS InterventionEndDt,
        i.InterventionClosureType
    FROM {IA_DOC_Interventions} i
    LEFT JOIN {IA_DOC_InterventionPrograms} ip
        USING(OffenderCd, OffenderInterventionProgramId)

    UNION ALL

    -- Compile all the intervention programs data for intervention programs
    -- that don't have a corresponding intervention record
    SELECT 
        ip.OffenderInterventionProgramId,
        CAST(NULL AS STRING) AS OffenderInterventionId,
        ip.OffenderCd,
        ip.InterventionProgramLocationId,
        ip.InterventionProgram,
        CAST(NULL AS STRING) AS Intervention,
        CAST(NULL AS STRING) AS InterventionRequired,
        ip.InterventionProgramRequired,
        ip.ReferringStaffId,
        DATE(ip.InterventionProgramStartDt) AS InterventionProgramStartDt,
        DATE(ip.InterventionProgramEndDt) AS InterventionProgramEndDt,
        ip.InterventionClosureType
    FROM {IA_DOC_InterventionPrograms} ip
    LEFT JOIN {IA_DOC_Interventions} i
        USING(OffenderCd, OffenderInterventionProgramId)
    WHERE OffenderInterventionId IS NULL

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ia",
    ingest_view_name="program_assignment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
