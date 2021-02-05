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
"""Query containing board action information extracted from multiple PADOC files, corresponding to condition codes
'RESCR', 'RESCR6', 'RESCR9', 'RESCR12'."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import \
    DirectIngestPreProcessedIngestViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


VIEW_QUERY_TEMPLATE = """
WITH distinct_codes AS(
  SELECT
    ParoleNumber,
    ParoleCountID,
    BdActionID,
    -- There is an uncommon case (~5 examples) in which there are two distinct condition codes for a given action. We take the max of those condition codes to avoid nondeterminism.--
    MAX(dbo_ConditionCode.CndConditionCode) AS CndConditionCode
  FROM
    {dbo_ConditionCode} dbo_ConditionCode
  WHERE
    dbo_ConditionCode.CndConditionCode IN ('RESCR', 'RESCR6', 'RESCR9', 'RESCR12')
  GROUP BY
    ParoleNumber,
    ParoleCountID,
    BdActionID
),
sci_actions AS (
  SELECT
     ParoleNumber,
     ParoleCountID,
     BdActionID,
     CONCAT(BdActEntryDateYear, BdActEntryDateMonth, BdActEntryDateDay) as ActionDate,
     CndConditionCode
  FROM
    {dbo_BoardAction} dbo_BoardAction
  INNER JOIN
    distinct_codes 
  USING 
    (ParoleNumber, ParoleCountID, BdActionID)
)
SELECT * 
FROM sci_actions
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region='us_pa',
    ingest_view_name='board_action',
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols='ParoleNumber, ActionDate',
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
