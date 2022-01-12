# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Utils for tests that use pre-processed entities."""
from typing import Dict, List, Optional

from recidiviz.calculator.pipeline.utils.incarceration_period_pre_processing_manager import (
    IncarcerationPreProcessingManager,
)
from recidiviz.calculator.pipeline.utils.pre_processed_incarceration_period_index import (
    PreProcessedIncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_delegate import (
    UsXxIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.supervision_period_utils import (
    standard_date_sort_for_supervision_periods,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)


def default_pre_processed_ip_index_for_tests(
    incarceration_periods: Optional[List[StateIncarcerationPeriod]] = None,
    transfers_are_collapsed: Optional[bool] = None,
    incarceration_delegate: Optional[StateSpecificIncarcerationDelegate] = None,
    ip_id_to_pfi_subtype: Optional[Dict[int, Optional[str]]] = None,
) -> PreProcessedIncarcerationPeriodIndex:
    # Validate  standards that we can expect to be met for all periods in all states
    # at the end of IP pre-processing
    IncarcerationPreProcessingManager.validate_ip_invariants(
        incarceration_periods=incarceration_periods or []
    )

    return PreProcessedIncarcerationPeriodIndex(
        incarceration_periods=incarceration_periods or [],
        transfers_are_collapsed=transfers_are_collapsed or False,
        incarceration_delegate=incarceration_delegate or UsXxIncarcerationDelegate(),
        ip_id_to_pfi_subtype=ip_id_to_pfi_subtype
        or (
            {
                ip.incarceration_period_id: None
                for ip in incarceration_periods
                if ip.incarceration_period_id
            }
            if incarceration_periods
            else {}
        ),
    )


def default_pre_processed_sp_index_for_tests(
    supervision_periods: Optional[List[StateSupervisionPeriod]] = None,
    sort_periods: Optional[bool] = False,
) -> PreProcessedSupervisionPeriodIndex:
    if supervision_periods and sort_periods:
        supervision_periods = standard_date_sort_for_supervision_periods(
            supervision_periods
        )

    return PreProcessedSupervisionPeriodIndex(
        supervision_periods=supervision_periods or []
    )
