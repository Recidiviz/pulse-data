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
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_charge import StateChargeStatus
from recidiviz.common.constants.state.state_court_case import (
    StateCourtCaseStatus,
    StateCourtType,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.str_field_utils import normalize_flat_json, parse_days
from recidiviz.persistence.entity.entity_deserialize import (
    DeserializableEntityFieldValue,
    EntityFactory,
    EntityFieldConverter,
    entity_deserialize,
)
from recidiviz.persistence.entity.state import entities


class StatePersonExternalIdFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StatePersonExternalId:
        return entity_deserialize(
            cls=entities.StatePersonExternalId,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StatePersonFactory(EntityFactory):
    @staticmethod
    def deserialize(**kwargs: DeserializableEntityFieldValue) -> entities.StatePerson:
        return entity_deserialize(
            cls=entities.StatePerson,
            converter_overrides={
                "full_name": EntityFieldConverter(str, normalize_flat_json),
            },
            defaults={},
            **kwargs,
        )


class StatePersonRaceFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StatePersonRace:
        return entity_deserialize(
            cls=entities.StatePersonRace, converter_overrides={}, defaults={}, **kwargs
        )


class StatePersonEthnicityFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StatePersonEthnicity:
        return entity_deserialize(
            cls=entities.StatePersonEthnicity,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateAssessmentFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateAssessment:
        return entity_deserialize(
            cls=entities.StateAssessment, converter_overrides={}, defaults={}, **kwargs
        )


class StatePersonAliasFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StatePersonAlias:
        return entity_deserialize(
            cls=entities.StatePersonAlias,
            converter_overrides={
                "full_name": EntityFieldConverter(str, normalize_flat_json),
            },
            defaults={},
            **kwargs,
        )


class StateAgentFactory(EntityFactory):
    @staticmethod
    def deserialize(**kwargs: DeserializableEntityFieldValue) -> entities.StateAgent:
        return entity_deserialize(
            cls=entities.StateAgent,
            converter_overrides={
                "full_name": EntityFieldConverter(str, normalize_flat_json),
            },
            defaults={"agent_type": StateAgentType.PRESENT_WITHOUT_INFO},
            **kwargs,
        )


class StateChargeFactory(EntityFactory):
    @staticmethod
    def deserialize(**kwargs: DeserializableEntityFieldValue) -> entities.StateCharge:
        return entity_deserialize(
            cls=entities.StateCharge,
            converter_overrides={},
            defaults={"status": StateChargeStatus.PRESENT_WITHOUT_INFO},
            **kwargs,
        )


class StateCourtCaseFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateCourtCase:
        return entity_deserialize(
            cls=entities.StateCourtCase,
            converter_overrides={},
            defaults={
                "court_type": StateCourtType.PRESENT_WITHOUT_INFO,
                "status": StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            },
            **kwargs,
        )


class StateEarlyDischargeFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateEarlyDischarge:
        return entity_deserialize(
            cls=entities.StateEarlyDischarge,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateIncarcerationIncidentFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateIncarcerationIncident:
        return entity_deserialize(
            cls=entities.StateIncarcerationIncident,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateIncarcerationIncidentOutcomeFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateIncarcerationIncidentOutcome:
        return entity_deserialize(
            cls=entities.StateIncarcerationIncidentOutcome,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateIncarcerationSentenceFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateIncarcerationSentence:
        return entity_deserialize(
            cls=entities.StateIncarcerationSentence,
            converter_overrides={
                "min_length_days": EntityFieldConverter(str, parse_days),
                "max_length_days": EntityFieldConverter(str, parse_days),
                # Note: other day count fields on this class (initial_time_served_days,
                # good_time_days, earned_time_days) should be formatted as integers
                # (e.g. not 'xxY xxM xxD' format) in the ingest view and will be parsed
                # normally as integers.
            },
            defaults={
                "status": StateSentenceStatus.PRESENT_WITHOUT_INFO,
                "incarceration_type": StateIncarcerationType.STATE_PRISON,
            },
            **kwargs,
        )


class StateSupervisionSentenceFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateSupervisionSentence:
        return entity_deserialize(
            cls=entities.StateSupervisionSentence,
            converter_overrides={
                "min_length_days": EntityFieldConverter(str, parse_days),
                "max_length_days": EntityFieldConverter(str, parse_days),
            },
            defaults={
                "status": StateSentenceStatus.PRESENT_WITHOUT_INFO,
            },
            **kwargs,
        )


class StateProgramAssignmentFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateProgramAssignment:
        return entity_deserialize(
            cls=entities.StateProgramAssignment,
            converter_overrides={},
            defaults={
                "participation_status": StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO
            },
            **kwargs,
        )


class StateIncarcerationPeriodFactory(EntityFactory):
    """Deserializing factory for StateIncarcerationPeriod."""

    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateIncarcerationPeriod:
        ip = entity_deserialize(
            cls=entities.StateIncarcerationPeriod,
            converter_overrides={},
            defaults={
                "incarceration_type": StateIncarcerationType.STATE_PRISON,
            },
            **kwargs,
        )

        return ip


class StateSupervisionPeriodFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateSupervisionPeriod:
        return entity_deserialize(
            cls=entities.StateSupervisionPeriod,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateSupervisionCaseTypeEntryFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateSupervisionCaseTypeEntry:
        return entity_deserialize(
            cls=entities.StateSupervisionCaseTypeEntry,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateSupervisionContactFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateSupervisionContact:
        return entity_deserialize(
            cls=entities.StateSupervisionContact,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateSupervisionViolationFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateSupervisionViolation:
        return entity_deserialize(
            cls=entities.StateSupervisionViolation,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateSupervisionViolationResponseFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateSupervisionViolationResponse:
        return entity_deserialize(
            cls=entities.StateSupervisionViolationResponse,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateSupervisionViolatedConditionEntryFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateSupervisionViolatedConditionEntry:
        return entity_deserialize(
            cls=entities.StateSupervisionViolatedConditionEntry,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateSupervisionViolationResponseDecisionEntryFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateSupervisionViolationResponseDecisionEntry:
        return entity_deserialize(
            cls=entities.StateSupervisionViolationResponseDecisionEntry,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateSupervisionViolationTypeEntryFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateSupervisionViolationTypeEntry:
        return entity_deserialize(
            cls=entities.StateSupervisionViolationTypeEntry,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateEmploymentPeriodFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateEmploymentPeriod:
        return entity_deserialize(
            cls=entities.StateEmploymentPeriod,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateDrugScreenFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateDrugScreen:
        return entity_deserialize(
            cls=entities.StateDrugScreen,
            converter_overrides={},
            defaults={},
            **kwargs,
        )
