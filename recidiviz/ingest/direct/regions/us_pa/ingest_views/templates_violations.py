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

"""Helper templates for the US_PA violation queries."""


_VIOLATION_VIEW_QUERY_TEMPLATE = """
SELECT
  parole_number,
  parole_count_id,
  set_id,
  EXTRACT(DATE FROM MIN({parsed_date_column})) as {date_field},
  TO_JSON_STRING(
    ARRAY_AGG(STRUCT(sequence_id, {code_field}) ORDER BY sequence_id)
  ) AS {types_field},
FROM (
  SELECT
    ParoleNumber as parole_number,
    ParoleCountID as parole_count_id,
    SetID as set_id,
    SequenceID as sequence_id,
    SanctionCode as {code_field},
    PARSE_TIMESTAMP("%m/%d/%Y %T", {raw_date_column}) as {parsed_date_column},
  FROM {{dbo_SanctionTracking}}
  WHERE Type = '{sanction_type}'

  UNION ALL

  SELECT
    ParoleNumber as parole_number,
    ParoleCountID as parole_count_id,
    SetID as set_id,
    SequenceID as sequence_id,
    SanctionCode as {code_field},
    PARSE_TIMESTAMP("%m/%d/%Y %T", {raw_date_column}) as {parsed_date_column},
  FROM {{dbo_Hist_SanctionTracking}}
  WHERE Type = '{sanction_type}'
)
GROUP BY parole_number, parole_count_id, set_id
"""


def generate_violation_view_query(date_field: str,
                                  raw_date_column: str,
                                  parsed_date_column: str,
                                  types_field: str,
                                  code_field: str,
                                  sanction_type: str) -> str:
    return _VIOLATION_VIEW_QUERY_TEMPLATE.format(
        date_field=date_field, raw_date_column=raw_date_column, parsed_date_column=parsed_date_column,
        types_field=types_field, code_field=code_field, sanction_type=sanction_type
    )
