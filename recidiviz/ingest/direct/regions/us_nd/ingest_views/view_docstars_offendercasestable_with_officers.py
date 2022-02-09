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
"""Query containing cases table with officers information."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    UPDATE_DATETIME_PARAM_NAME,
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH cases_with_terminating_officers AS (
  SELECT {{docstars_offendercasestable}}.*,
       TERMINATING_OFFICER as terminating_officer_id,
       {{docstars_officers}}.LNAME AS terminating_officer_lname, 
       {{docstars_officers}}.FNAME AS terminating_officer_fname, 
       {{docstars_officers}}.SITEID AS terminating_officer_siteid,
  FROM {{docstars_offendercasestable}}
  LEFT JOIN {{docstars_officers}}
  ON (TERMINATING_OFFICER = OFFICER)
),
ranked_term_dates AS (
  SELECT
    SID,
    TERM_DATE,
    ROW_NUMBER() OVER (
      PARTITION BY SID
      ORDER BY IFNULL(PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', TERM_DATE), @{UPDATE_DATETIME_PARAM_NAME}) DESC
    ) AS rn
  FROM {{docstars_offendercasestable}}
),
most_recent_term_date_by_sid AS (
  SELECT
    SID,
    TERM_DATE
  FROM
    ranked_term_dates
  WHERE
    rn = 1
),
offendercases_with_terminating_and_recent_pos AS (
  SELECT cases_with_terminating_officers.*,
        {{docstars_officers}}.OFFICER AS recent_officer_id, 
        {{docstars_officers}}.LNAME AS recent_officer_lname, 
        {{docstars_officers}}.FNAME AS recent_officer_fname, 
        {{docstars_officers}}.SITEID AS recent_officer_siteid,
        CASE
            -- Only set the supervision level for either
            -- the currently open period or the most recently closed one
            WHEN IFNULL(cases_with_terminating_officers.TERM_DATE, 'NULL')
                = IFNULL(most_recent_term_date_by_sid.TERM_DATE, 'NULL')
            -- Pick the supervision level override if there is one
                THEN COALESCE(SUPER_OVERRIDE, SUP_LVL)
            -- Set supervision level to null if not the most recent period 
            ELSE NULL
        END AS current_supervision_level
  FROM cases_with_terminating_officers
  LEFT JOIN {{docstars_offenders}}
  ON (cases_with_terminating_officers.SID = {{docstars_offenders}}.SID)
  LEFT JOIN {{docstars_officers}}
  ON ({{docstars_offenders}}.AGENT = {{docstars_officers}}.OFFICER)
  LEFT JOIN most_recent_term_date_by_sid
  ON (cases_with_terminating_officers.SID = most_recent_term_date_by_sid.SID)
)
SELECT * FROM offendercases_with_terminating_and_recent_pos
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_nd",
    ingest_view_name="docstars_offendercasestable_with_officers",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="CASE_NUMBER",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
