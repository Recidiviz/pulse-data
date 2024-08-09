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
"""Query containing MDOC supervision violation information from COMS."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """,

    -- For each violation incident, string agg all violation types ofr all corresponding charges
    charges AS (
        SELECT 
            Violation_Incident_Id,
            LTRIM(Offender_Number, '0') AS Offender_Number,
            STRING_AGG(DISTINCT Technical_Non_Technical ORDER BY Technical_Non_Technical) as charge_violation_types
        FROM {COMS_Violation_Incident_Charges} charges
        WHERE Technical_Non_Technical <> 'Predates COMS as System of Record'
          AND Disposition NOT IN ('Not Guilty', 'Dismissed')
        GROUP BY 1,2
    ),

    -- join all other violation information (for both parole and probation) with charges
    probation_and_violation_joined_incidents AS (
        SELECT 
            incidents.Violation_Incident_Id,
            LTRIM(incidents.Offender_Number, '0') AS Offender_Number,
            incidents.Incident_Date,
            -- as of 8/1/24, there are no cases where both charge_violation_types and parole.violation_type is both valued
            COALESCE(charges.charge_violation_types, parole.Violation_Type) AS Violation_Type,
            -- either parole.Investigation_Start_Date or probation.Investigation_Start_Date will be valued depending on whether it's parole or probation
            COALESCE(parole.Investigation_Start_Date, probation.Investigation_Start_Date) as Investigation_Start_Date,
            Supervisor_Decision,
            Supervisor_Decision_Date,
            PV_Specialist_Decision,
            PV_Specialist_Decision_Date
        FROM {COMS_Violation_Incidents} incidents

        -- PAROLE
        LEFT JOIN {COMS_Parole_Violation_Violation_Incidents} parole_incidents ON parole_incidents.Violation_Incident_Id = incidents.Violation_Incident_Id
        LEFT JOIN {COMS_Parole_Violations} parole ON parole.Parole_Violation_Id = parole_incidents.Parole_Violation_Id
        LEFT JOIN {COMS_Parole_Violation_Summary_Decisions} parole_decisions ON parole.Parole_Violation_Id = parole_decisions.Parole_Violation_Id

        -- PROBATION
        LEFT JOIN {COMS_Probation_Violation_Violation_Incidents} probation_incidents ON probation_incidents.Violation_Incident_Id = incidents.Violation_Incident_Id
        LEFT JOIN {COMS_Probation_Violations} probation ON probation.Probation_Violation_Id = probation_incidents.Probation_Violation_Id

        -- CHARGES 
        LEFT JOIN charges ON incidents.Violation_Incident_Id = charges.Violation_Incident_Id
    )

    SELECT 
        DISTINCT 
        Offender_Number, 
        Violation_Incident_Id, 
        Incident_Date, 
        Violation_Type, 
        Investigation_Start_Date,
        Supervisor_Decision,
        Supervisor_Decision_Date,
        PV_Specialist_Decision,
        PV_Specialist_Decision_Date
    FROM probation_and_violation_joined_incidents
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="supervision_violations_coms",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
