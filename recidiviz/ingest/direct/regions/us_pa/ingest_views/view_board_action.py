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

from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import \
    DirectIngestPreProcessedIngestViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


VIEW_QUERY_TEMPLATE = """
WITH board_action_codes AS (
  SELECT
    dbo_BdActionType.ParoleNumber,
    dbo_BoardAction.BdActEntryDateMonth,
    dbo_BoardAction.BdActEntryDateDay,
    dbo_BoardAction.BdActEntryDateYear,
    dbo_ConditionCode.CndConditionCode
  FROM
    {dbo_BoardAction} dbo_BoardAction
  INNER JOIN 
    ({dbo_BdActionType} dbo_BdActionType
      INNER JOIN
        {dbo_ConditionCode} dbo_ConditionCode
      ON 
        dbo_BdActionType.BdActionID = dbo_ConditionCode.BdActionID
      AND 
        dbo_BdActionType.ParoleCountID = dbo_ConditionCode.ParoleCountID
      AND
        dbo_BdActionType.ParoleNumber = dbo_ConditionCode.ParoleNumber
    )
  ON 
    dbo_BoardAction.BdActionID = dbo_BdActionType.BdActionID 
  AND 
    dbo_BoardAction.ParoleNumber = dbo_BdActionType.ParoleNumber 
  AND 
    dbo_BoardAction.ParoleCountID = dbo_BdActionType.ParoleCountID
), 
sci_actions AS (
  -- Distict because it's common for there to be multiple entries for the same person with the same CndConditionCode on the same day that list various conditions in raw text --
  SELECT
    DISTINCT ParoleNumber,
             CONCAT(BdActEntryDateYear, BdActEntryDateMonth, BdActEntryDateDay) as ActionDate,
             CndConditionCode,
             inmate_number,
             control_number
  FROM board_action_codes 
  LEFT JOIN
    -- TODO(#5641): Update this query to remove dependence on dbo_tblSearchInmateInfo - we should not be using control
    -- or inmate numbers (DOC ids) in ingest views for supervision data.
    {dbo_tblSearchInmateInfo}
  ON 
    ParoleNumber = parole_board_num
  WHERE
    CndConditionCode IN ('RESCR', 'RESCR6', 'RESCR9', 'RESCR12')
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
