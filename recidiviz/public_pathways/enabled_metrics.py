# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
# ============================================================================
""" Contains the configuration for which Public Pathways metrics are enabled """

from typing import Dict, List

from recidiviz.calculator.query.state.views.public_pathways.public_pathways_enabled_states import (
    get_public_pathways_enabled_states_for_cloud_sql,
)
from recidiviz.case_triage.shared_pathways.query_builders.metric_query_builder import (
    MetricQueryBuilder,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.public_pathways.metrics.metric_query_builders import (
    ALL_PUBLIC_PATHWAYS_METRICS,
)

ALL_PUBLIC_PATHWAYS_METRICS_BY_STATE_CODE: Dict[StateCode, List[MetricQueryBuilder]] = {
    StateCode(state_code): ALL_PUBLIC_PATHWAYS_METRICS
    for state_code in get_public_pathways_enabled_states_for_cloud_sql()
}

ALL_PUBLIC_PATHWAYS_METRICS_BY_STATE_CODE_BY_NAME = {
    state_code: {metric.name: metric for metric in metrics}
    for state_code, metrics in ALL_PUBLIC_PATHWAYS_METRICS_BY_STATE_CODE.items()
}
