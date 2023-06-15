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
"""The configuration objects for Outliers states"""
from typing import Dict

from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.constants import (
    ABSCONSIONS_BENCH_WARRANTS,
    EARLY_DISCHARGE_REQUESTS,
    INCARCERATION_STARTS,
    INCARCERATION_STARTS_AND_INFERRED,
    INCARCERATION_STARTS_TECHNICAL_VIOLATION,
    TASK_COMPLETIONS_FULL_TERM_DISCHARGE,
    TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION,
)
from recidiviz.outliers.types import OutliersConfig

OUTLIERS_CONFIGS_BY_STATE: Dict[StateCode, OutliersConfig] = {
    StateCode.US_IX: OutliersConfig(
        metrics=[
            INCARCERATION_STARTS_TECHNICAL_VIOLATION,
            ABSCONSIONS_BENCH_WARRANTS,
            INCARCERATION_STARTS,
            EARLY_DISCHARGE_REQUESTS,
            TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION,
            TASK_COMPLETIONS_FULL_TERM_DISCHARGE,
        ],
    ),
    StateCode.US_PA: OutliersConfig(
        metrics=[INCARCERATION_STARTS_AND_INFERRED],
        unit_of_analysis_to_exclusion={
            MetricUnitOfAnalysisType.SUPERVISION_DISTRICT: ["FAST", "CO"]
        },
    ),
}
