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
"""Contains US_AZ implementation of the StateSpecificViolationDelegate."""


from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.entity.activity.normalized_entities import (
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)

# The default order ranks ABSCONDED above TECHNICAL. In US_AZ that ordering decides
# which *source* wins rather than which violation is genuinely worse, because two
# sources describe the same revocation: the warrant recorded when it was issued, and
# the movement reason recorded when the person was actually readmitted. ADCRR bases its
# own published recidivism reporting on the movement reason, and the two disagree
# heavily in one direction -- ~2,100 revocations since FY2022 carry an "Absconder
# Warrant" but a "Technical Violator" movement reason, against only ~260 the other way.
# Ranking TECHNICAL above ABSCONDED makes the movement reason win those cases, which
# reproduces the absconding/technical split ADCRR publishes (p.49 of the One Year Return
# to Incarceration report) to within ~0.6pp.
#
# NB: this is a source preference expressed as an ordering, not a claim that a technical
# violation is more serious than an absconsion in Arizona. If the balance of those
# disagreements ever shifts, revisit this rather than assuming it still holds.
_US_AZ_VIOLATION_TYPE_SEVERITY_ORDER = [
    StateSupervisionViolationType.FELONY,
    StateSupervisionViolationType.MISDEMEANOR,
    StateSupervisionViolationType.LAW,
    StateSupervisionViolationType.TECHNICAL,
    StateSupervisionViolationType.ABSCONDED,
    StateSupervisionViolationType.MUNICIPAL,
    StateSupervisionViolationType.ESCAPED,
    StateSupervisionViolationType.INTERNAL_UNKNOWN,
    StateSupervisionViolationType.EXTERNAL_UNKNOWN,
]


class UsAzViolationDelegate(StateSpecificViolationDelegate):
    """US_AZ implementation of the StateSpecificViolationDelegate."""

    violation_type_and_subtype_shorthand_ordered_map = [
        (violation_type, violation_type.value, violation_type.value.lower())
        for violation_type in _US_AZ_VIOLATION_TYPE_SEVERITY_ORDER
    ]

    def should_include_response_in_violation_history(
        self,
        response: NormalizedStateSupervisionViolationResponse,
        include_follow_up_responses: bool = False,
    ) -> bool:
        """For US_AZ, we include all responses of type CITATION, VIOLATION_REPORT and PERMANENT_DECISION responses to
        be included in the violation history.
        """
        return response.response_type in (
            StateSupervisionViolationResponseType.VIOLATION_REPORT,
            StateSupervisionViolationResponseType.CITATION,
            StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )
