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
"""US_MO implementation of the supervision delegate"""
# pylint: disable=unused-argument
from datetime import date
from typing import Any, Dict, List, Optional

from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentType,
)
from recidiviz.common.date import DateRange, DateRangeDiff
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod


class UsMoSupervisionDelegate(StateSpecificSupervisionDelegate):
    """US_MO implementation of the supervision delegate"""

    def supervision_types_mutually_exclusive(self) -> bool:
        """In US_MO, people on DUAL supervision are tracked as mutually exclusive from groups of people
        on PAROLE or PROBATION."""
        return True

    def supervision_period_in_supervision_population_in_non_excluded_date_range(
        self,
        supervision_period: StateSupervisionPeriod,
        supervising_officer_external_id: Optional[str],
    ) -> bool:
        """In US_MO, a supervision period should have an active PO to also be included to the
        supervision population.
        """
        return supervising_officer_external_id is not None

    def get_supervising_officer_external_id_for_supervision_period(
        self,
        supervision_period: StateSupervisionPeriod,
        supervision_period_to_agent_associations: Dict[int, Dict[str, Any]],
    ) -> Optional[str]:
        """In US_MO, because supervision periods may not necessarily have persisted
        supervision period ids, we do a date-range based mapping back to original
        ingested periods."""
        most_recent_agent_association = None
        for agent_dict in supervision_period_to_agent_associations.values():
            start_date: Optional[date] = agent_dict["agent_start_date"]
            end_date: Optional[date] = agent_dict["agent_end_date"]

            if not start_date:
                continue

            if DateRangeDiff(
                supervision_period.duration,
                DateRange.from_maybe_open_range(start_date, end_date),
            ).overlapping_range:
                if most_recent_agent_association is None or (
                    start_date > most_recent_agent_association["agent_start_date"]
                ):
                    most_recent_agent_association = agent_dict
                continue

        return (
            most_recent_agent_association["agent_external_id"]
            if most_recent_agent_association
            else None
        )

    def assessment_types_to_include_for_class(
        self, assessment_class: StateAssessmentClass
    ) -> Optional[List[StateAssessmentType]]:
        """In US_MO, only Ohio Risk Assessment System (ORAS) assessment types are
        supported."""
        if assessment_class == StateAssessmentClass.RISK:
            return [
                StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            ]
        return None
