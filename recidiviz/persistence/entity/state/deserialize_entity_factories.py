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
"""Factories for deserializing entities in state/entities.py from ingested values."""

from typing import Optional, Union

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.enum_parser import EnumParser
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_court_case import (
    StateCourtCaseStatus,
    StateCourtType,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodStatus,
)
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.str_field_utils import normalize_flat_json, parse_days
from recidiviz.persistence.entity.entity_deserialize import (
    EntityFactory,
    EntityFieldConverter,
    entity_deserialize,
)
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    parse_residency_status,
)


class StatePersonExternalIdFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: Optional[Union[str, EnumParser]]
    ) -> entities.StatePersonExternalId:
        return entity_deserialize(
            cls=entities.StatePersonExternalId,
            converter_overrides={},
            defaults={},
            **kwargs
        )


class StatePersonFactory(EntityFactory):
    @staticmethod
    def deserialize(**kwargs: Optional[Union[str, EnumParser]]) -> entities.StatePerson:
        return entity_deserialize(
            cls=entities.StatePerson,
            converter_overrides={
                "residency_status": EntityFieldConverter(str, parse_residency_status),
                "full_name": EntityFieldConverter(str, normalize_flat_json),
            },
            defaults={},
            **kwargs
        )


class StatePersonRaceFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: Optional[Union[str, EnumParser]]
    ) -> entities.StatePersonRace:
        return entity_deserialize(
            cls=entities.StatePersonRace, converter_overrides={}, defaults={}, **kwargs
        )


class StatePersonEthnicityFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: Optional[Union[str, EnumParser]]
    ) -> entities.StatePersonEthnicity:
        return entity_deserialize(
            cls=entities.StatePersonEthnicity,
            converter_overrides={},
            defaults={},
            **kwargs
        )


class StateSentenceGroupFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: Optional[Union[str, EnumParser]]
    ) -> entities.StateSentenceGroup:
        return entity_deserialize(
            cls=entities.StateSentenceGroup,
            converter_overrides={},
            defaults={"status": StateSentenceStatus.PRESENT_WITHOUT_INFO},
            **kwargs
        )


class StateAssessmentFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: Optional[Union[str, EnumParser]]
    ) -> entities.StateAssessment:
        return entity_deserialize(
            cls=entities.StateAssessment, converter_overrides={}, defaults={}, **kwargs
        )


class StatePersonAliasFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: Optional[Union[str, EnumParser]]
    ) -> entities.StatePersonAlias:
        return entity_deserialize(
            cls=entities.StatePersonAlias,
            converter_overrides={
                "full_name": EntityFieldConverter(str, normalize_flat_json),
            },
            defaults={},
            **kwargs
        )


class StateAgentFactory(EntityFactory):
    @staticmethod
    def deserialize(**kwargs: Optional[Union[str, EnumParser]]) -> entities.StateAgent:
        return entity_deserialize(
            cls=entities.StateAgent,
            converter_overrides={
                "full_name": EntityFieldConverter(str, normalize_flat_json),
            },
            defaults={"agent_type": StateAgentType.PRESENT_WITHOUT_INFO},
            **kwargs
        )


class StateChargeFactory(EntityFactory):
    @staticmethod
    def deserialize(**kwargs: Optional[Union[str, EnumParser]]) -> entities.StateCharge:
        return entity_deserialize(
            cls=entities.StateCharge,
            converter_overrides={},
            defaults={"status": ChargeStatus.PRESENT_WITHOUT_INFO},
            **kwargs
        )


class StateCourtCaseFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: Optional[Union[str, EnumParser]]
    ) -> entities.StateCourtCase:
        return entity_deserialize(
            cls=entities.StateCourtCase,
            converter_overrides={},
            defaults={
                "court_type": StateCourtType.PRESENT_WITHOUT_INFO,
                "status": StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            },
            **kwargs
        )


class StateEarlyDischargeFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: Optional[Union[str, EnumParser]]
    ) -> entities.StateEarlyDischarge:
        return entity_deserialize(
            cls=entities.StateEarlyDischarge,
            converter_overrides={},
            defaults={},
            **kwargs
        )


class StateIncarcerationIncidentFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: Optional[Union[str, EnumParser]]
    ) -> entities.StateIncarcerationIncident:
        return entity_deserialize(
            cls=entities.StateIncarcerationIncident,
            converter_overrides={},
            defaults={},
            **kwargs
        )


class StateIncarcerationIncidentOutcomeFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: Optional[Union[str, EnumParser]]
    ) -> entities.StateIncarcerationIncidentOutcome:
        return entity_deserialize(
            cls=entities.StateIncarcerationIncidentOutcome,
            converter_overrides={},
            defaults={},
            **kwargs
        )


class StateIncarcerationSentenceFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: Optional[Union[str, EnumParser]]
    ) -> entities.StateIncarcerationSentence:
        return entity_deserialize(
            cls=entities.StateIncarcerationSentence,
            converter_overrides={
                "min_length_days": EntityFieldConverter(str, parse_days),
                "max_length_days": EntityFieldConverter(str, parse_days),
                # Note: other date fields on this class (initial_time_served_days,
                # good_time_days, earned_time_days) should be formatted as integers
                # (e.g. not 'xxY xxM xxD' format) in the ingest view and will be parsed
                # normally as integers.
            },
            defaults={
                "status": StateSentenceStatus.PRESENT_WITHOUT_INFO,
                "incarceration_type": StateIncarcerationType.STATE_PRISON,
            },
            **kwargs
        )


class StateProgramAssignmentFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: Optional[Union[str, EnumParser]]
    ) -> entities.StateProgramAssignment:
        return entity_deserialize(
            cls=entities.StateProgramAssignment,
            converter_overrides={},
            defaults={
                "participation_status": StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO
            },
            **kwargs
        )


class StateIncarcerationPeriodFactory(EntityFactory):
    """Deserializing factory for StateIncarcerationPeriod."""

    @staticmethod
    def deserialize(
        **kwargs: Optional[Union[str, EnumParser]]
    ) -> entities.StateIncarcerationPeriod:
        ip = entity_deserialize(
            cls=entities.StateIncarcerationPeriod,
            converter_overrides={},
            defaults={
                "incarceration_type": StateIncarcerationType.STATE_PRISON,
                # TODO(#9128): Delete this once we delete status entirely.
                "status": StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
            },
            **kwargs
        )

        # TODO(#9128): Deprecate and delete status entirely
        # Status default based on presence of admission/release dates
        if ip.release_date:
            status_default = StateIncarcerationPeriodStatus.NOT_IN_CUSTODY
        elif ip.admission_date and not ip.release_date:
            status_default = StateIncarcerationPeriodStatus.IN_CUSTODY
        else:
            status_default = StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO

        ip.status = (
            status_default
            if ip.status == StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO
            else ip.status
        )
        return ip


# TODO(#8909): Add factories for remainder of state schema here.
