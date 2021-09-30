# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""US_ND implementation of the supervision pre-processing delegate"""
from typing import List, Optional

from recidiviz.calculator.pipeline.utils.period_utils import (
    find_last_terminated_period_before_date,
)
from recidiviz.calculator.pipeline.utils.supervision_period_pre_processing_manager import (
    StateSpecificSupervisionPreProcessingDelegate,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod

# Limit the search for previous supervision periods.
LOOKBACK_MONTHS_LIMIT = 1


class UsNdSupervisionPreProcessingDelegate(
    StateSpecificSupervisionPreProcessingDelegate
):
    """US_ND implementation of the supervision pre-processing delegate"""

    def supervision_admission_reason_override(
        self,
        supervision_period: StateSupervisionPeriod,
        supervision_periods: List[StateSupervisionPeriod],
    ) -> Optional[StateSupervisionPeriodAdmissionReason]:
        """Looks at the provided |supervision_period| and all supervision periods for
        this person to return the admission reason for this |supervision_period|.
        This is necessary because we do not currently have a way to ingest ND admission
        reasons for supervision periods.
        """

        if not supervision_period.start_date:
            raise ValueError(
                "Found null supervision_period.start_date while inferring admission reason."
            )
        most_recent_previous_period = find_last_terminated_period_before_date(
            upper_bound_date=supervision_period.start_date,
            periods=supervision_periods,
            maximum_months_proximity=LOOKBACK_MONTHS_LIMIT,
        )
        if not most_recent_previous_period:
            if (
                supervision_period.supervision_period_supervision_type
                == StateSupervisionPeriodSupervisionType.PAROLE
            ):
                # If the person is under parole supervision, the current admission
                # reason should be CONDITIONAL_RELEASE
                return StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE
            if (
                supervision_period.supervision_period_supervision_type
                == StateSupervisionPeriodSupervisionType.PROBATION
            ):
                # If there was not a previous period and the person is under PROBATION
                # supervision, the current admission reason should be COURT_SENTENCE
                return StateSupervisionPeriodAdmissionReason.COURT_SENTENCE
        else:
            if (
                most_recent_previous_period.termination_reason
                == StateSupervisionPeriodTerminationReason.ABSCONSION
            ):
                # If the most recent previous supervision period was an absconsion, the
                # current supervision period's admission reason should be ABSCONSION.
                return StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION
            if (
                most_recent_previous_period.termination_reason
                == StateSupervisionPeriodTerminationReason.REVOCATION
            ):
                # If the most recent previous supervision period was a REVOCATION, the
                # current supervision period's admission reason should be COURT_SENTENCE.
                return StateSupervisionPeriodAdmissionReason.COURT_SENTENCE
            if (
                most_recent_previous_period.supervision_period_supervision_type
                == StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT
                and supervision_period.supervision_period_supervision_type
                == StateSupervisionPeriodSupervisionType.PAROLE
            ):
                # If the supervision type transitioned from HALFWAY_HOUSE to PAROLE, the
                # current supervision period's admission reason should be TRANSFER_WITHIN_STATE.
                return StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE
            if (
                most_recent_previous_period.supervision_period_supervision_type
                == StateSupervisionPeriodSupervisionType.PAROLE
                and supervision_period.supervision_period_supervision_type
                == StateSupervisionPeriodSupervisionType.PROBATION
            ):
                # If the supervision type transitioned from PAROLE to PROBATION, the
                # admission reason should be COURT_SENTENCE.
                return StateSupervisionPeriodAdmissionReason.COURT_SENTENCE
            if (
                most_recent_previous_period.supervising_officer
                and supervision_period.supervising_officer
                and most_recent_previous_period.supervising_officer
                != supervision_period.supervising_officer
            ):
                # If the supervision officer changed between the previous and current
                # supervision period, the admission reason should be TRANSFER_WITHIN_STATE.
                return StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE
            if (
                most_recent_previous_period.supervision_period_supervision_type
                == StateSupervisionPeriodSupervisionType.PROBATION
                and supervision_period.supervision_period_supervision_type
                == StateSupervisionPeriodSupervisionType.PAROLE
            ):
                # If the supervision type transitioned from PROBATION to PAROLE, the
                # admission reason should be INTERNAL_UNKNOWN, since this should be
                # extremely rare.
                return StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN
        return None
