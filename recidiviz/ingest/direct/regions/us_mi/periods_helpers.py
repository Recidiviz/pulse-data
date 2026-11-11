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

# Excludes PENDING_MDOC_CUSTODY_STATUS. incarceration_periods_COMS_2026 adds it on top of
# this list, but the staff relationship views only count case managers for Prison.
ACTIVE_INCARCERATION_STATUSES = """
      'Prison'
"""

# Marks someone sentenced to prison who is in jail still awaiting transfer into MDOC custody. Those
# people have no COMS housing record and no incarceration period from OMNI, and the status
# ends with "MDOC Custody Confirmed" once a Prison status takes over.
PENDING_MDOC_CUSTODY_STATUS = "Pending MDOC Custody"

migration_date_2023 = f"DATE({COMS_MIGRATION_DATE_2023.year},{COMS_MIGRATION_DATE_2023.month},{COMS_MIGRATION_DATE_2023.day})"

migration_date_2026 = f"DATE({COMS_MIGRATION_DATE_2026.year},{COMS_MIGRATION_DATE_2026.month},{COMS_MIGRATION_DATE_2026.day})"


def identify_tiny_spans_dates(cte_name: str, id_col: str) -> str:
    """Helper for identifying relevant dates for tiny spans"""
    return f"""
        SELECT DISTINCT {id_col}, 
                        cte_period_start AS dte
        FROM {cte_name}

        UNION ALL

        SELECT DISTINCT {id_col}, 
                        cte_period_end AS dte
        FROM {cte_name}
    """


def join_ctes_onto_tiny_spans(cte_name: str, id_col: str) -> str:
    """Helper for joining on attribute CTEs onto tiny spans"""
    return f"""
    left join {cte_name} 
        ON {cte_name}.{id_col} = tiny_spans.{id_col}
        AND {cte_name}.cte_period_start <= tiny_spans.period_start
        AND tiny_spans.period_end <= {cte_name}.cte_period_end
    """


# Offender numbers in COMS come with a leading zero but do not come with a leading zero in OMNI
PAD_OFFENDER_NUMBER_FROM_OMNI = "LPAD(offender_number, 7, '0') AS offender_number"

# Date that splits OMNI movements from COMS movements for the LEGACY and COMS_2026 period
# views. OMNI stopped recording movements a few days before COMS_MIGRATION_DATE_2026, so
# this date sits ahead of the go-live. Movements entered natively in COMS before this date
# are folded into the LEGACY views.
MOVEMENTS_CUTOVER_DATE = "DATE(2026,10,22)"

# Magic end date for open periods
OPEN_END_DATE = "DATE(9999,9,9)"

# In COMS, differential files will demarcdate deleted records by sending a row with all values blank
# except for the record id
FILTER_OUT_DELETIONS = "Offender_Number IS NOT NULL"
