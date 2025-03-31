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
"""Query containing charge information from the criminal history table."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    SELECT *, 
        ROW_NUMBER() OVER(PARTITION BY COALESCE(Inmate_No, Parole_No) ORDER BY Disposition_Date, Code, Description, Grade, Disposition, Inmate_No, Parole_No) AS seq_no
    FROM (
        SELECT DISTINCT
            Inmate_No, 
            Parole_No,
            Code,
            Description,
            Grade,
            UPPER(Disposition) AS Disposition,
            DATE(Disposition_Date) AS Disposition_Date,
        FROM {Criminal_History} history
        WHERE 
            (UPPER(Disposition) LIKE 'GUILTY%' AND UPPER(Disposition) NOT LIKE '%WITHDRAWN%')
            OR UPPER(Disposition) in ('CONVICTED', 'NOLO CONTENDERE', 'FOUND GUILTY', 'PLEAD GUILTY', 'ACTIVE CASE', 
                                      'ADJUDICATED DELINQUENT', 'TERM CT SUP - ADJUDICATED DELINQUENT', 'ADJUDICATED DEPENDENT', 'JUVENILE ADJUDICATION',
                                      'NO DISPOSITION REPORTED', 'DISPOSITION UNREPORTED-NO FURTHER ACTION')
    )
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="charges_from_crim_hist",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
