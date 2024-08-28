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
"""Utils for the normalization of state entities for calculations."""
import datetime
from typing import Dict, List, Optional, Tuple, Type

from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.assessment_normalization_manager import (
    AssessmentNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.incarceration_period_normalization_manager import (
    IncarcerationPeriodNormalizationManager,
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.program_assignment_normalization_manager import (
    ProgramAssignmentNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.sentence_normalization_manager import (
    SentenceNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.staff_role_period_normalization_manager import (
    StaffRolePeriodNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_contact_normalization_manager import (
    SupervisionContactNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
    SupervisionPeriodNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_violation_responses_normalization_manager import (
    ViolationResponseNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
from recidiviz.pipelines.utils.period_utils import (
    find_earliest_date_of_period_ending_in_death,
)

# All EntityNormalizationManagers
NORMALIZATION_MANAGERS: List[Type[EntityNormalizationManager]] = [
    IncarcerationPeriodNormalizationManager,
    ProgramAssignmentNormalizationManager,
    SupervisionPeriodNormalizationManager,
    ViolationResponseNormalizationManager,
    AssessmentNormalizationManager,
    SentenceNormalizationManager,
    SupervisionContactNormalizationManager,
    StaffRolePeriodNormalizationManager,
]


# TODO(#10084) Combine incarceration and supervision period normalization
# TODO(#25800) Instantiate incarceration and supervision delegates with their state-specific data
def normalized_periods_for_calculations(
    person_id: int,
    ip_normalization_delegate: StateSpecificIncarcerationNormalizationDelegate,
    sp_normalization_delegate: StateSpecificSupervisionNormalizationDelegate,
    incarceration_periods: List[StateIncarcerationPeriod],
    supervision_periods: List[StateSupervisionPeriod],
    normalized_violation_responses: List[NormalizedStateSupervisionViolationResponse],
    staff_external_id_to_staff_id: Dict[Tuple[str, str], int],
) -> Tuple[
    Tuple[List[StateIncarcerationPeriod], AdditionalAttributesMap],
    Tuple[List[StateSupervisionPeriod], AdditionalAttributesMap],
]:
    """Helper for returning the normalized incarceration and supervision periods for
    calculations.

    DISCLAIMER: IP normalization may rely on normalized StateSupervisionPeriod
    entities for some states. Tread carefully if you are implementing any changes to
    SP normalization that may create circular dependencies between these processes.
    """

    # The normalization functions need to know if this person has any periods that
    # ended because of death to handle any open periods or periods that extend past
    # their death date accordingly.
    earliest_death_date: Optional[
        datetime.date
    ] = find_earliest_date_of_period_ending_in_death(
        periods=supervision_periods + incarceration_periods
    )

    sp_normalization_manager = SupervisionPeriodNormalizationManager(
        person_id=person_id,
        supervision_periods=supervision_periods,
        incarceration_periods=incarceration_periods,
        delegate=sp_normalization_delegate,
        staff_external_id_to_staff_id=staff_external_id_to_staff_id,
        earliest_death_date=earliest_death_date,
    )

    (
        processed_sps,
        additional_sp_attributes,
    ) = (
        sp_normalization_manager.normalized_supervision_periods_and_additional_attributes()
    )

    normalized_sps = convert_entity_trees_to_normalized_versions(
        root_entities=processed_sps,
        normalized_entity_class=NormalizedStateSupervisionPeriod,
        additional_attributes_map=additional_sp_attributes,
    )

    supervision_period_index = NormalizedSupervisionPeriodIndex(
        sorted_supervision_periods=normalized_sps
    )

    ip_normalization_manager = IncarcerationPeriodNormalizationManager(
        incarceration_periods=incarceration_periods,
        normalization_delegate=ip_normalization_delegate,
        normalized_supervision_period_index=supervision_period_index,
        normalized_violation_responses=normalized_violation_responses,
        person_id=person_id,
        earliest_death_date=earliest_death_date,
    )

    (
        processed_ips,
        additional_ip_attributes,
    ) = (
        ip_normalization_manager.normalized_incarceration_periods_and_additional_attributes()
    )

    return (
        (processed_ips, additional_ip_attributes),
        (processed_sps, additional_sp_attributes),
    )
