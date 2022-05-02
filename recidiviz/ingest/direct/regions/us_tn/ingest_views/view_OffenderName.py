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
            NameType,
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
                -- There are times when updates are made to the rows for a name, but the birthdate is not always carried through with that update, 
                -- which could cause null birthdates to be surfaced when we have the information. This window allows us to keep the most up to date 
                -- name/information entry and still preserve birthdates when they are null.
            FIRST_VALUE(BirthDate IGNORE NULLS) OVER (PARTITION BY OffenderID ORDER BY LastUpdateDate DESC, CAST(SequenceNumber AS INT64) DESC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as BirthDate,
            LastUpdateDate,
            SequenceNumber
        FROM {OffenderName}
    ), 
    filtered_out_nicknames AS (
        -- Ensures that we include nickname rows when looking for the most recent non-null birthdate but 
        -- skip them when we are calculating the row number.
      SELECT *,
      ROW_NUMBER() OVER (PARTITION BY OffenderID ORDER BY LastUpdateDate DESC, CAST(SequenceNumber AS INT64) DESC) AS recency_rank
      FROM normalized_rows
      -- Filter out nicknames.
      WHERE NameType != 'N'
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
    FROM filtered_out_nicknames
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
