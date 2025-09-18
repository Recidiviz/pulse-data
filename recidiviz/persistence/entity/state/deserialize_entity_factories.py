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

from recidiviz.common.constants.state.state_charge import StateChargeStatus
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.str_field_utils import parse_days
from recidiviz.persistence.entity.entity_deserialize import (
    DeserializableEntityFieldValue,
    EntityFactory,
    EntityFieldConverter,
    entity_deserialize,
)
from recidiviz.persistence.entity.state import entities


class StatePersonAddressPeriodFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StatePersonAddressPeriod:
        return entity_deserialize(
            cls=entities.StatePersonAddressPeriod,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StatePersonHousingStatusPeriodFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StatePersonHousingStatusPeriod:
        return entity_deserialize(
            cls=entities.StatePersonHousingStatusPeriod,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


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
            converter_overrides={},
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
            cls=entities.StateAssessment,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StatePersonAliasFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StatePersonAlias:
        return entity_deserialize(
            cls=entities.StatePersonAlias,
            converter_overrides={},
            defaults={},
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
        return entity_deserialize(
            cls=entities.StateIncarcerationPeriod,
            converter_overrides={},
            defaults={
                "incarceration_type": StateIncarcerationType.STATE_PRISON,
            },
            **kwargs,
        )


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


class StateScheduledSupervisionContactFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateScheduledSupervisionContact:
        return entity_deserialize(
            cls=entities.StateScheduledSupervisionContact,
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


class StateTaskDeadlineFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateTaskDeadline:
        return entity_deserialize(
            cls=entities.StateTaskDeadline,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateStaffFactory(EntityFactory):
    @staticmethod
    def deserialize(**kwargs: DeserializableEntityFieldValue) -> entities.StateStaff:
        return entity_deserialize(
            cls=entities.StateStaff,
            converter_overrides={
                # Do not normalize emails - retain input capitalization.
                "email": EntityFieldConverter(str, lambda x: x),
            },
            defaults={},
            **kwargs,
        )


class StateStaffExternalIdFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateStaffExternalId:
        return entity_deserialize(
            cls=entities.StateStaffExternalId,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateStaffRolePeriodFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateStaffRolePeriod:
        return entity_deserialize(
            cls=entities.StateStaffRolePeriod,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateStaffSupervisorPeriodFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateStaffSupervisorPeriod:
        return entity_deserialize(
            cls=entities.StateStaffSupervisorPeriod,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateStaffLocationPeriodFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateStaffLocationPeriod:
        return entity_deserialize(
            cls=entities.StateStaffLocationPeriod,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateStaffCaseloadTypePeriodFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateStaffCaseloadTypePeriod:
        return entity_deserialize(
            cls=entities.StateStaffCaseloadTypePeriod,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateSentenceFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateSentence:
        return entity_deserialize(
            cls=entities.StateSentence,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


# TODO(#26240): Replace StateChargeFactory with this one
class StateChargeV2Factory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateChargeV2:
        return entity_deserialize(
            cls=entities.StateChargeV2,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateSentenceStatusSnapshotFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateSentenceStatusSnapshot:
        return entity_deserialize(
            cls=entities.StateSentenceStatusSnapshot,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateSentenceLengthFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateSentenceLength:
        return entity_deserialize(
            cls=entities.StateSentenceLength,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateSentenceGroupFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateSentenceGroup:
        return entity_deserialize(
            cls=entities.StateSentenceGroup,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StateSentenceGroupLengthFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StateSentenceGroupLength:
        return entity_deserialize(
            cls=entities.StateSentenceGroupLength,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class StatePersonStaffRelationshipPeriodFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.StatePersonStaffRelationshipPeriod:
        return entity_deserialize(
            cls=entities.StatePersonStaffRelationshipPeriod,
            converter_overrides={},
            defaults={},
            **kwargs,
        )
