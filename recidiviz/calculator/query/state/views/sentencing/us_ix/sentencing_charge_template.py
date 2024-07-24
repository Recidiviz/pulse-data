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
"""View logic to prepare US_IX Sentencing charge data for PSI tools"""

US_IX_SENTENCING_CHARGE_TEMPLATE = """
    SELECT DISTINCT 
        most_severe_description AS charge,
        "US_IX" AS state_code
    FROM `{project_id}.{sessions_dataset}.sentence_imposed_group_summary_materialized`
    WHERE state_code = "US_IX" and most_severe_description IS NOT NULL
"""
