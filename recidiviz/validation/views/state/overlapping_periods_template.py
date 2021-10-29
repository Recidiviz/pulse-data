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
"""A query template for a validation to check for overlapping incarceration or supervision periods."""

from typing import Type, Union

from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)
from recidiviz.utils.string import StrictStringFormatter

OVERLAPPING_PERIODS_TEMPLATE = """
  SELECT 
    DISTINCT period.state_code AS region_code, period.{primary_key_field}, period.external_id, period.person_id
  FROM `{{project_id}}.{{state_dataset}}.{table_name}` period
  LEFT OUTER JOIN
  `{{project_id}}.{{state_dataset}}.{table_name}` other_period
  ON period.state_code = other_period.state_code AND period.person_id = other_period.person_id 
    AND period.{primary_key_field} != other_period.{primary_key_field}
    AND period.{start_date_field} <= other_period.{start_date_field} 
    AND (period.{end_date_field} IS NULL OR period.{end_date_field} > other_period.{start_date_field})
    -- Ignore zero-day periods, which are largely ignored by our calc pipelines pipelines
    AND (other_period.{start_date_field} != other_period.{end_date_field})
  WHERE other_period.{primary_key_field} IS NOT NULL
"""


def overlapping_periods_query(
    period_type: Union[Type[StateIncarcerationPeriod], Type[StateSupervisionPeriod]]
) -> str:
    if period_type == StateIncarcerationPeriod:
        start_date_field = "admission_date"
        end_date_field = "release_date"
        table_name = "state_incarceration_period"
    elif period_type == StateSupervisionPeriod:
        start_date_field = "start_date"
        end_date_field = "termination_date"
        table_name = "state_supervision_period"
    else:
        raise ValueError(f"Unexpected period_type [{period_type}]")

    return StrictStringFormatter().format(
        OVERLAPPING_PERIODS_TEMPLATE,
        primary_key_field=period_type.get_primary_key_column_name(),
        start_date_field=start_date_field,
        end_date_field=end_date_field,
        table_name=table_name,
    )
