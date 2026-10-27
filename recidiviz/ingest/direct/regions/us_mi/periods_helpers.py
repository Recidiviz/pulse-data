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
"""This module has reusable strings for various MI specific query fragments and constants referenced in multiple spots in our codebase."""
from datetime import date

# Date when Michigan migrated from OMNI to COMS for supervision data
COMS_MIGRATION_DATE_2023 = date(2023, 8, 14)

# Date when Michigan migrated from OMNI to COMS for all remaining data
COMS_MIGRATION_DATE_2026 = date(2026, 10, 26)

ACTIVE_SUPERVISION_STATUSES = """
      'Parole',
      'Delayed Sentence',
      'Interstate Compact Parole',
      'Interstate Compact Probation',
      'Probation'
"""

# TODO(MI-8599): Determine what to do about Pending MDOC Custody status based on real data
ACTIVE_INCARCERATION_STATUSES = """
      'Prison'
"""

migration_date_2023 = f"DATE({COMS_MIGRATION_DATE_2023.year},{COMS_MIGRATION_DATE_2023.month},{COMS_MIGRATION_DATE_2023.day})"

migration_date_2026 = f"DATE({COMS_MIGRATION_DATE_2026.year},{COMS_MIGRATION_DATE_2026.month},{COMS_MIGRATION_DATE_2026.day})"


def identify_tiny_spans_dates(cte_name: str) -> str:
    """Helper for identifying relevant dates for tiny spans"""
    return f"""
        SELECT DISTINCT offender_number, 
                        cte_period_start AS dte
        FROM {cte_name}

        UNION ALL

        SELECT DISTINCT offender_number, 
                        cte_period_end AS dte
        FROM {cte_name}
    """


def join_ctes_onto_tiny_spans(cte_name: str) -> str:
    """Helper for joining on attribute CTEs onto tiny spans"""
    return f"""
    left join {cte_name} 
        ON {cte_name}.offender_number = tiny_spans.offender_number
        AND {cte_name}.cte_period_start <= tiny_spans.period_start
        AND tiny_spans.period_end <= {cte_name}.cte_period_end
    """


# Offender numbers in COMS come with a leading zero but do not come with a leading zero in OMNI
PAD_OFFENDER_NUMBER_FROM_OMNI = "LPAD(offender_number, 7, '0') AS offender_number"

# For periods, we might not want to use the actual migration date and allow some buffer room
# TODO(MI-8599): Evaluate if this is the right date or whether we can set to COMS_MIGRATION_DATE_2026
MOVEMENTS_CUTOVER_DATE = "DATE(2025,10,22)"

# Magic end date for open periods
OPEN_END_DATE = "DATE(9999,9,9)"

# In COMS, differential files will demarcdate deleted records by sending a row with all values blank
# except for the record id
FILTER_OUT_DELETIONS = "Offender_Number IS NOT NULL"
