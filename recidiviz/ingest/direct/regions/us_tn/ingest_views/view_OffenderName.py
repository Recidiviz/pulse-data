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
"""Query containing offender information."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    WITH normalized_rows AS (
        SELECT
            OffenderID,
            FirstName,
            MiddleName,
            LastName,
            CASE 
                WHEN Race in ('W', 'B', 'A', 'I') THEN Race
                ELSE NULL
            END AS Race,
            CASE 
                WHEN Race in ('H') THEN 'HISPANIC'
                ELSE 'NOT_HISPANIC'
            END AS Ethnicity,
            CASE 
                WHEN Sex in ('F', 'M') THEN Sex
                ELSE NULL
            END AS Sex,
            BirthDate,
            ROW_NUMBER() OVER (PARTITION BY OffenderID ORDER BY LastUpdateDate DESC, CAST(SequenceNumber AS INT64) DESC) AS recency_rank
        FROM
            {OffenderName}
        -- Filter out nicknames.
        Where NameType != 'N'
    )
    SELECT
        OffenderID,
        FirstName,
        MiddleName,
        LastName,
        Race,
        Ethnicity,
        Sex,
        BirthDate
    FROM normalized_rows
    WHERE recency_rank = 1
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_tn",
    ingest_view_name="OffenderName",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OffenderID",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
