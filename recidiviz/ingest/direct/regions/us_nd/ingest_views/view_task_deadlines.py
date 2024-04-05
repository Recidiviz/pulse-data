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
"""Query containing sentence expiration dates for incarceration and supervision sentences."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH supervision_sentence_expiration AS (
  SELECT DISTINCT
    SID AS person_external_id,
    CASE_NUMBER AS sentence_external_id, 
    PAROLE_TO AS eligible_date, 
    RecDate AS update_datetime, 
    DESCRIPTION,
    'SUPERVISION' AS sentence_type
  FROM {docstars_offendercasestable@ALL}
), 
sup_filtered AS (
  SELECT person_external_id, sentence_external_id, eligible_date, update_datetime, description, sentence_type FROM (
    SELECT *, LAG(eligible_date) OVER (PARTITION BY person_external_id ORDER BY update_datetime) AS prev_eligible_date
    FROM supervision_sentence_expiration
  )
  WHERE (prev_eligible_date IS NULL AND eligible_date IS NOT NULL) 
  OR (prev_eligible_date IS NOT NULL AND eligible_date IS NULL) 
  OR (prev_eligible_date != eligible_date)
),
incarceration_sentence_expiration AS (
    SELECT DISTINCT
        REPLACE(REPLACE(OFFENDER_BOOK_ID,',',''), '.00', '') AS person_external_id,
        CONCAT(REPLACE(REPLACE(OFFENDER_BOOK_ID,',',''), '.00', ''),'-', SENTENCE_SEQ) AS sentence_external_id,
        SENTENCE_EXPIRY_DATE AS eligible_date,
        COALESCE(MODIFY_DATETIME, 
            -- If the sentence expiration date has never been modified, assume it was
            -- last updated when the sentence was imposed
            EFFECTIVE_DATE,
            -- This is a catch-all that is always the date of the last system migration
            -- in ND when all records were "Created", 2014-12-06.
            CREATE_DATETIME) AS update_datetime,
        CAST(NULL AS STRING) AS DESCRIPTION,
        'INCARCERATION' AS sentence_type
    FROM {elite_offendersentences@ALL} sentences
),
inc_filtered AS (
  SELECT person_external_id, sentence_external_id, eligible_date, update_datetime, description, sentence_type FROM (
    SELECT *, LAG(eligible_date) OVER (PARTITION BY person_external_id ORDER BY update_datetime) AS prev_eligible_date
    FROM incarceration_sentence_expiration
  )
  WHERE (prev_eligible_date IS NULL AND eligible_date IS NOT NULL) 
  OR (prev_eligible_date IS NOT NULL AND eligible_date IS NULL) 
  OR (prev_eligible_date != eligible_date)
)

SELECT * FROM sup_filtered
UNION ALL 
SELECT * FROM inc_filtered
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="task_deadlines",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
