# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Query containing supervision period information extracted from the output of the `AssignedStaff` table.
The table contains one row per supervision period.
"""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH officer_names AS (
    SELECT
        REGEXP_REPLACE(StaffID, r'[^A-Z0-9]', '') as SupervisionOfficerID,
        SPLIT(LastName, ',') as SupervisionOfficerLastNameAndSuffix,
        FirstName as SupervisionOfficerFirstName
    FROM {Staff}
),
split_officer_names AS (
    SELECT
        SupervisionOfficerID,
        SupervisionOfficerFirstName,
        SupervisionOfficerLastNameAndSuffix[SAFE_ORDINAL(1)] as SupervisionOfficerLastName,
        REGEXP_REPLACE(SupervisionOfficerLastNameAndSuffix[SAFE_ORDINAL(2)], r'[^A-Z0-9.]', '') as SupervisionOfficerSuffix,
    FROM officer_names
),
clean_up_and_filter AS (
    SELECT 
        OffenderID,
        REGEXP_REPLACE(CaseType, r'[^A-Z0-9]', '') as SupervisionType,
        StartDate,
        NULLIF(EndDate, 'NULL') as EndDate,
        -- Clean up to only include expected characters (i.e. only capitalized letters, or a combination of capitalized 
        -- letters and numbers).
        REGEXP_REPLACE(AssignmentBeginReason, r'[^A-Z]', '') as AdmissionReason,
        REGEXP_REPLACE(AssignmentEndReason, r'[^A-Z]', '') as TerminationReason,
        REGEXP_REPLACE(StaffID, r'[^A-Z0-9]', '') as SupervisionOfficerID,
        REGEXP_REPLACE(AssignmentType, r'[^A-Z0-9]', '') as AssignmentType,
        REGEXP_REPLACE(SiteID, r'[^A-Z0-9]', '') as Site
    FROM {AssignedStaff}
    -- Filter to only the supervision types that are associated with supervision periods.
    WHERE AssignmentType in (
        'PRO', # Probation
        'PAO', # Parole
        'CCC'  # Community Corrections
    )
)
SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY OffenderID ORDER BY StartDate ASC) AS SupervisionPeriodSequenceNumber
FROM clean_up_and_filter LEFT JOIN split_officer_names USING (SupervisionOfficerID)
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_tn",
    ingest_view_name="AssignedStaffSupervisionPeriod",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OffenderID ASC",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
