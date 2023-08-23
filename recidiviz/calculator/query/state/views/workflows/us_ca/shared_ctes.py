# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""CTE Logic that is shared across US_CA Workflows queries."""

US_CA_MOST_RECENT_AGENT_DATA = """
  SELECT DISTINCT
    BadgeNumber,
    ParoleRegion,
    ParoleDistrict,
    ParoleUnit,
    AgentClassification,
  FROM `{project_id}.{us_ca_raw_data_dataset}.AgentParole`
  WHERE BadgeNumber IS NOT NULL
  QUALIFY file_id = FIRST_VALUE(file_id) OVER (
    ORDER BY update_datetime DESC
  )
"""

US_CA_MOST_RECENT_CLIENT_DATA = """
  SELECT
    OffenderId,
    Cdcno,
    ParoleRegion,
    ParoleDistrict,
    ParoleUnit,
    BadgeNumber,
    PARSE_DATE("%m/%d/%Y", EarnedDischargeDate) AS EarnedDischargeDate,
    PARSE_DATE("%m/%d/%Y", ControllingDischargeDate) AS ControllingDischargeDate
  FROM `{project_id}.{us_ca_raw_data_dataset}.PersonParole`
  WHERE BadgeNumber IS NOT NULL
  QUALIFY file_id = FIRST_VALUE(file_id) OVER (
    ORDER BY update_datetime DESC
  )
"""
