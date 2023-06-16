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
"""Helpers for building Outliers views"""
from typing import List

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.outliers_configs import OUTLIERS_CONFIGS_BY_STATE


def get_all_config_based_query_filters_query_str() -> str:
    """Returns additional clauses for filtering based on the state-configs"""
    enabled_states = get_outliers_enabled_states()

    additional_filters = []
    for state_code in enabled_states:
        state_exclusions = get_state_specific_config_exclusions(StateCode(state_code))
        additional_filters.extend(state_exclusions)

    return f'AND {" AND ".join(additional_filters)}' if additional_filters else ""


def get_state_specific_config_exclusions(state_code: StateCode) -> List[str]:
    """
    For a given state, use the OutliersConfig object to get exclusions for
    """
    exclusions = []
    state_outliers_config = OUTLIERS_CONFIGS_BY_STATE[state_code]

    if state_outliers_config.unit_of_analysis_to_exclusion:
        unit_of_analysis_exclusions = [
            f"NOT(state_code = '{state_code.value}' AND {f'supervision_{unit_of_analysis.value.lower()}'} IN ({list_to_query_string(exclusions, quoted=True)}))"
            for unit_of_analysis, exclusions in state_outliers_config.unit_of_analysis_to_exclusion.items()
        ]

        exclusions.extend(unit_of_analysis_exclusions)

    return exclusions


def format_state_specific_officer_aggregated_metric_filters() -> str:
    filters = []
    for state_code, config in OUTLIERS_CONFIGS_BY_STATE.items():
        filters.append(
            f"""(
        state_code = '{state_code.value}' {config.supervision_officer_aggregated_metric_filters if config.supervision_officer_aggregated_metric_filters else ""}
    )
"""
        )

    return "    OR\n    ".join(filters)
