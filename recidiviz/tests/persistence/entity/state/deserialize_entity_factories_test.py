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
"""Tests for deserialize_entity_factories.py."""
import datetime
import unittest
from datetime import date
from typing import Set

from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_charge import (
    StateChargeClassificationType,
    StateChargeStatus,
    StateChargeV2Status,
)
from recidiviz.common.constants.state.state_drug_screen import (
    StateDrugScreenResult,
    StateDrugScreenSampleType,
)
from recidiviz.common.constants.state.state_early_discharge import (
    StateEarlyDischargeDecision,
    StateEarlyDischargeDecisionStatus,
)
from recidiviz.common.constants.state.state_employment_period import (
    StateEmploymentPeriodEmploymentStatus,
    StateEmploymentPeriodEndReason,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
    StateIncarcerationIncidentType,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_person import (
    StateGender,
    StateRace,
    StateResidencyStatus,
)
from recidiviz.common.constants.state.state_person_address_period import (
    StatePersonAddressType,
)
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.common.constants.state.state_person_housing_status_period import (
    StatePersonHousingStatusType,
)
from recidiviz.common.constants.state.state_person_staff_relationship_period import (
    StatePersonStaffRelationshipType,
)
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_scheduled_supervision_contact import (
    StateScheduledSupervisionContactLocation,
    StateScheduledSupervisionContactMethod,
    StateScheduledSupervisionContactReason,
    StateScheduledSupervisionContactStatus,
    StateScheduledSupervisionContactType,
)
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
)
from recidiviz.common.constants.state.state_shared_enums import (
    StateActingBodyType,
    StateCustodialAuthority,
)
from recidiviz.common.constants.state.state_staff_caseload_type import (
    StateStaffCaseloadType,
)
from recidiviz.common.constants.state.state_staff_role_period import (
    StateStaffRoleSubtype,
    StateStaffRoleType,
)
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactReason,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_violated_condition import (
    StateSupervisionViolatedConditionType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.constants.state.state_system_type import StateSystemType
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import (
    NormalizedSerializableJSON,
    SerializableJSON,
)
from recidiviz.persistence.entity.entity_utils import (
    get_all_entity_classes_in_module,
    get_all_entity_factory_classes_in_module,
)
from recidiviz.persistence.entity.state import deserialize_entity_factories, entities


class TestDeserializeEntityFactories(unittest.TestCase):
    """Tests for deserialize_entity_factories.py."""

    def test_factories_defined_for_all_classes(self) -> None:
        """Tests that an entity factory has been added for every entity."""
        factory_classes = get_all_entity_factory_classes_in_module(
            deserialize_entity_factories
        )
        found_entity_class_names: Set[str] = set()
        for factory_class in factory_classes:
            entity_class_name = factory_class.__name__[: -len("Factory")]
            deserialize_return_type = factory_class.deserialize.__annotations__[
                "return"
            ]

            self.assertEqual(entity_class_name, deserialize_return_type.__name__)
            self.assertNotIn(entity_class_name, found_entity_class_names)
            found_entity_class_names.add(entity_class_name)

        entity_classes = get_all_entity_classes_in_module(entities)
        expected_entity_class_names = {c.__name__ for c in entity_classes}

        extra_classes = found_entity_class_names.difference(expected_entity_class_names)
        self.assertEqual(set(), extra_classes)
        missing_classes = expected_entity_class_names.difference(
            found_entity_class_names
        )
        self.assertEqual(set(), missing_classes)

    def test_has_tests_for_all_factories(self) -> None:
        """Tests that a unittest has been added to this class for every expected entity
        factory.
        """
        test_names = {t for t in dir(self) if t.startswith("test")}

        expected_tests = {
            f"test_deserialize_{entity_cls.__name__}"
            for entity_cls in get_all_entity_classes_in_module(entities)
        }

        missing_tests = expected_tests.difference(test_names)
        if missing_tests:
            self.fail(f"Found missing expected tests: {missing_tests}")

    def test_deserialize_StatePerson(self) -> None:
        result = deserialize_entity_factories.StatePersonFactory.deserialize(
            state_code="us_xx",
            gender=StateGender.MALE,
            gender_raw_text="MALE",
            full_name=NormalizedSerializableJSON(full_name="full NAME"),
            birthdate="12-31-1999",
            current_address="NNN\n  STREET \t ZIP",
            residency_status=StateResidencyStatus.PERMANENT,
        )

        # Assert
        expected_result = entities.StatePerson.new_with_defaults(
            gender=StateGender.MALE,
            gender_raw_text="MALE",
            full_name='{"full_name": "FULL NAME"}',
            birthdate=date(year=1999, month=12, day=31),
            current_address="NNN STREET ZIP",
            residency_status=StateResidencyStatus.PERMANENT,
            state_code="US_XX",
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StatePersonAddressPeriod(self) -> None:
        result = (
            deserialize_entity_factories.StatePersonAddressPeriodFactory.deserialize(
                state_code="US_XX",
                address_line_1="123 SANTA STREET",
                address_line_2="APT 4",
                address_city="NORTH POLE",
                address_zip="10000",
                address_county="GLACIER COUNTY",
                address_type=StatePersonAddressType.PHYSICAL_RESIDENCE,
                address_start_date="2020-01-01",
            )
        )

        # Assert
        expected_result = entities.StatePersonAddressPeriod.new_with_defaults(
            state_code="US_XX",
            address_line_1="123 SANTA STREET",
            address_line_2="APT 4",
            address_city="NORTH POLE",
            address_zip="10000",
            address_county="GLACIER COUNTY",
            address_type=StatePersonAddressType.PHYSICAL_RESIDENCE,
            address_start_date=date(2020, 1, 1),
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StatePersonHousingStatusPeriod(self) -> None:
        result = deserialize_entity_factories.StatePersonHousingStatusPeriodFactory.deserialize(
            state_code="US_XX",
            housing_status_start_date=date(year=2006, month=7, day=2),
            housing_status_end_date=date(year=2007, month=7, day=2),
            housing_status_type=StatePersonHousingStatusType.PERMANENT_RESIDENCE,
        )

        # Assert
        expected_result = entities.StatePersonHousingStatusPeriod.new_with_defaults(
            state_code="US_XX",
            housing_status_start_date=date(year=2006, month=7, day=2),
            housing_status_end_date=date(year=2007, month=7, day=2),
            housing_status_type=StatePersonHousingStatusType.PERMANENT_RESIDENCE,
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StatePersonExternalId(self) -> None:
        result = deserialize_entity_factories.StatePersonExternalIdFactory.deserialize(
            external_id="123a",
            id_type="state_id",
            state_code="us_xx",
        )

        # Assert
        expected_result = entities.StatePersonExternalId(
            external_id="123A",
            id_type="STATE_ID",
            state_code="US_XX",
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateIncarcerationSentence(self) -> None:
        result = (
            deserialize_entity_factories.StateIncarcerationSentenceFactory.deserialize(
                status=StateSentenceStatus.SUSPENDED,
                status_raw_text="S",
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                incarceration_type_raw_text="SP",
                external_id="INCARCERATION_ID",
                date_imposed="7/2/2006",
                effective_date="1/2/2006",
                projected_min_release_date="4/2/2111",
                projected_max_release_date="7/2/2111",
                parole_eligibility_date="4/2/2111",
                county_code="CO",
                min_length_days="90D",
                max_length_days="180D",
                is_life="False",
                is_capital_punishment="False",
                parole_possible="true",
                initial_time_served_days="60",  # Units in days
                good_time_days="365",  # Units in days
                earned_time_days=None,
                state_code="us_xx",
            )
        )

        # Assert
        expected_result = entities.StateIncarcerationSentence(
            status=StateSentenceStatus.SUSPENDED,
            status_raw_text="S",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="SP",
            external_id="INCARCERATION_ID",
            date_imposed=date(year=2006, month=7, day=2),
            effective_date=date(year=2006, month=1, day=2),
            projected_min_release_date=date(year=2111, month=4, day=2),
            projected_max_release_date=date(year=2111, month=7, day=2),
            parole_eligibility_date=date(year=2111, month=4, day=2),
            completion_date=None,
            state_code="US_XX",
            county_code="CO",
            min_length_days=90,
            max_length_days=180,
            is_life=False,
            is_capital_punishment=False,
            parole_possible=True,
            initial_time_served_days=60,
            good_time_days=365,
            earned_time_days=None,
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateProgramAssignment(self) -> None:
        result = deserialize_entity_factories.StateProgramAssignmentFactory.deserialize(
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            participation_status_raw_text="IP",
            external_id="PROGRAM_ASSIGNMENT_ID",
            referral_date="1/2/2111",
            start_date="1/3/2111",
            discharge_date="1/4/2111",
            program_id="PROGRAM_ID",
            program_location_id="LOCATION_ID",
            state_code="us_xx",
            referral_metadata=SerializableJSON(key1="val1", key2="val2"),
        )

        # Assert
        expected_result = entities.StateProgramAssignment(
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            participation_status_raw_text="IP",
            external_id="PROGRAM_ASSIGNMENT_ID",
            referral_date=date(year=2111, month=1, day=2),
            start_date=date(year=2111, month=1, day=3),
            discharge_date=date(year=2111, month=1, day=4),
            program_id="PROGRAM_ID",
            program_location_id="LOCATION_ID",
            state_code="US_XX",
            referral_metadata='{"key1": "val1", "key2": "val2"}',
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateSupervisionContact(self) -> None:
        result = (
            deserialize_entity_factories.StateSupervisionContactFactory.deserialize(
                external_id="CONTACT_ID",
                contact_type=StateSupervisionContactType.DIRECT,
                contact_type_raw_text="D",
                status=StateSupervisionContactStatus.COMPLETED,
                status_raw_text="C",
                contact_reason=StateSupervisionContactReason.GENERAL_CONTACT,
                contact_reason_raw_text="GC",
                location=StateSupervisionContactLocation.RESIDENCE,
                location_raw_text="R",
                contact_date="1/2/1111",
                state_code="us_xx",
                verified_employment="True",
                resulted_in_arrest="False",
                supervision_contact_metadata='{"MEASURABLE": "YES"}',
            )
        )

        # Assert
        expected_result = entities.StateSupervisionContact(
            external_id="CONTACT_ID",
            status=StateSupervisionContactStatus.COMPLETED,
            status_raw_text="C",
            contact_type=StateSupervisionContactType.DIRECT,
            contact_type_raw_text="D",
            contact_date=date(year=1111, month=1, day=2),
            state_code="US_XX",
            contact_reason=StateSupervisionContactReason.GENERAL_CONTACT,
            contact_reason_raw_text="GC",
            location=StateSupervisionContactLocation.RESIDENCE,
            location_raw_text="R",
            verified_employment=True,
            resulted_in_arrest=False,
            supervision_contact_metadata='{"MEASURABLE": "YES"}',
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateScheduledSupervisionContact(self) -> None:
        update_datetime = datetime.datetime.now()
        result = deserialize_entity_factories.StateScheduledSupervisionContactFactory.deserialize(
            external_id="SCHEDULED_CONTACT_ID",
            contact_type=StateScheduledSupervisionContactType.DIRECT,
            contact_type_raw_text="D",
            status=StateScheduledSupervisionContactStatus.SCHEDULED,
            status_raw_text="C",
            contact_reason=StateScheduledSupervisionContactReason.GENERAL_CONTACT,
            contact_reason_raw_text="GC",
            location=StateScheduledSupervisionContactLocation.RESIDENCE,
            location_raw_text="R",
            scheduled_contact_date=date(2020, 1, 2),
            state_code="us_xx",
            contacting_staff_external_id="1",
            contacting_staff_external_id_type="STAFF_ID",
            contact_method=StateScheduledSupervisionContactMethod.IN_PERSON,
            contact_method_raw_text="FACE",
            update_datetime=update_datetime,
            contact_meeting_address="123 BIKINI BOTTOM",
            sequence_num=1,
        )

        # Assert
        expected_result = entities.StateScheduledSupervisionContact(
            external_id="SCHEDULED_CONTACT_ID",
            status=StateScheduledSupervisionContactStatus.SCHEDULED,
            status_raw_text="C",
            contact_type=StateScheduledSupervisionContactType.DIRECT,
            contact_type_raw_text="D",
            scheduled_contact_date=date(year=2020, month=1, day=2),
            state_code="US_XX",
            contact_reason=StateScheduledSupervisionContactReason.GENERAL_CONTACT,
            contact_reason_raw_text="GC",
            location=StateScheduledSupervisionContactLocation.RESIDENCE,
            location_raw_text="R",
            contacting_staff_external_id="1",
            contacting_staff_external_id_type="STAFF_ID",
            contact_method=StateScheduledSupervisionContactMethod.IN_PERSON,
            contact_method_raw_text="FACE",
            contact_meeting_address="123 BIKINI BOTTOM",
            update_datetime=update_datetime,
            sequence_num=1,
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateSupervisionCaseTypeEntry(self) -> None:
        result = deserialize_entity_factories.StateSupervisionCaseTypeEntryFactory.deserialize(
            state_code="us_xx",
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            case_type_raw_text="DV",
        )

        # Assert
        expected_result = entities.StateSupervisionCaseTypeEntry(
            state_code="US_XX",
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            case_type_raw_text="DV",
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateSupervisionSentence(self) -> None:
        result = (
            deserialize_entity_factories.StateSupervisionSentenceFactory.deserialize(
                status=StateSentenceStatus.SUSPENDED,
                status_raw_text="S",
                supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                supervision_type_raw_text="PR",
                external_id="SENTENCE_ID",
                date_imposed="2000-12-13",
                effective_date="20010101",
                completion_date="1/2/2011",
                projected_completion_date="1/2/2012",
                is_life=True,
                county_code="CO",
                min_length_days="90D",
                max_length_days="180D",
                state_code="us_xx",
            )
        )

        # Assert
        expected_result = entities.StateSupervisionSentence(
            status=StateSentenceStatus.SUSPENDED,
            status_raw_text="S",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            supervision_type_raw_text="PR",
            external_id="SENTENCE_ID",
            date_imposed=date(year=2000, month=12, day=13),
            effective_date=date(year=2001, month=1, day=1),
            completion_date=date(year=2011, month=1, day=2),
            projected_completion_date=date(year=2012, month=1, day=2),
            is_life=True,
            state_code="US_XX",
            county_code="CO",
            min_length_days=90,
            max_length_days=180,
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateSupervisionViolationTypeEntry(self) -> None:
        result = deserialize_entity_factories.StateSupervisionViolationTypeEntryFactory.deserialize(
            state_code="us_xx",
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="T",
        )

        # Assert
        expected_result = entities.StateSupervisionViolationTypeEntry(
            state_code="US_XX",
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="T",
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StatePersonRace(self) -> None:
        result = deserialize_entity_factories.StatePersonRaceFactory.deserialize(
            race=StateRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
            race_raw_text="SAMOAN",
            state_code="us_xx",
        )

        # Assert
        expected_result = entities.StatePersonRace(
            race=StateRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
            race_raw_text="SAMOAN",
            state_code="US_XX",
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateAssessment(self) -> None:
        result = deserialize_entity_factories.StateAssessmentFactory.deserialize(
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text="RISK",
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text="LSIR",
            external_id="ASSESSMENT_ID",
            assessment_date="1/2/2011",
            state_code="us_xx",
            assessment_score="17",
            assessment_level=StateAssessmentLevel.MEDIUM,
            assessment_level_raw_text="MED",
            assessment_metadata=NormalizedSerializableJSON(
                high_score_domains='["a", "c", "q"]'
            ),
        )

        # Assert
        expected_result = entities.StateAssessment(
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text="RISK",
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text="LSIR",
            assessment_level=StateAssessmentLevel.MEDIUM,
            assessment_level_raw_text="MED",
            external_id="ASSESSMENT_ID",
            assessment_date=date(year=2011, month=1, day=2),
            state_code="US_XX",
            assessment_score=17,
            assessment_metadata='{"high_score_domains": "[\\"A\\", \\"C\\", \\"Q\\"]"}',
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateSupervisionViolation(self) -> None:
        result = (
            deserialize_entity_factories.StateSupervisionViolationFactory.deserialize(
                external_id="VIOLATION_ID",
                violation_date="1/2/2011",
                state_code="us_xx",
                is_violent="false",
            )
        )

        # Assert
        expected_result = entities.StateSupervisionViolation(
            external_id="VIOLATION_ID",
            violation_date=date(year=2011, month=1, day=2),
            state_code="US_XX",
            is_violent=False,
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateEarlyDischarge(self) -> None:
        result = deserialize_entity_factories.StateEarlyDischargeFactory.deserialize(
            external_id="id1",
            request_date="2010/07/01",
            decision_date="2010/08/01",
            decision=StateEarlyDischargeDecision.REQUEST_DENIED,
            decision_raw_text="DEN",
            decision_status=StateEarlyDischargeDecisionStatus.DECIDED,
            decision_status_raw_text="D",
            deciding_body_type=StateActingBodyType.COURT,
            deciding_body_type_raw_text="C",
            requesting_body_type=StateActingBodyType.SUPERVISION_OFFICER,
            requesting_body_type_raw_text="OF",
            state_code="us_xx",
            county_code="cty",
        )

        # Assert
        expected_result = entities.StateEarlyDischarge(
            external_id="ID1",
            request_date=date(year=2010, month=7, day=1),
            decision_date=date(year=2010, month=8, day=1),
            decision=StateEarlyDischargeDecision.REQUEST_DENIED,
            decision_raw_text="DEN",
            decision_status=StateEarlyDischargeDecisionStatus.DECIDED,
            decision_status_raw_text="D",
            deciding_body_type=StateActingBodyType.COURT,
            deciding_body_type_raw_text="C",
            requesting_body_type=StateActingBodyType.SUPERVISION_OFFICER,
            requesting_body_type_raw_text="OF",
            state_code="US_XX",
            county_code="CTY",
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateSupervisionViolatedConditionEntry(self) -> None:
        result = deserialize_entity_factories.StateSupervisionViolatedConditionEntryFactory.deserialize(
            state_code="us_xx",
            condition=StateSupervisionViolatedConditionType.INTERNAL_UNKNOWN,
            condition_raw_text="sober",
        )

        # Assert
        expected_result = entities.StateSupervisionViolatedConditionEntry(
            state_code="US_XX",
            condition=StateSupervisionViolatedConditionType.INTERNAL_UNKNOWN,
            condition_raw_text="SOBER",
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateSupervisionViolationResponse(self) -> None:
        result = deserialize_entity_factories.StateSupervisionViolationResponseFactory.deserialize(
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="P",
            response_subtype="SUBTYPE",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PB",
            deciding_staff_external_id="abc",
            deciding_staff_external_id_type="US_XX_STAFF_ID",
            external_id="RESPONSE_ID",
            response_date="1/2/2011",
            state_code="us_xx",
            is_draft="True",
        )

        # Assert
        expected_result = entities.StateSupervisionViolationResponse(
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="P",
            response_subtype="SUBTYPE",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PB",
            deciding_staff_external_id="ABC",
            deciding_staff_external_id_type="US_XX_STAFF_ID",
            external_id="RESPONSE_ID",
            response_date=date(year=2011, month=1, day=2),
            state_code="US_XX",
            is_draft=True,
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateIncarcerationPeriod(self) -> None:
        result = deserialize_entity_factories.StateIncarcerationPeriodFactory.deserialize(
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="P",
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="REV",
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            release_reason_raw_text="SS",
            external_id="INCARCERATION_ID",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            specialized_purpose_for_incarceration_raw_text="60DAY",
            admission_date="1/2/2111",
            release_date="2/2/2112",
            state_code="us_xx",
            county_code="bis",
            facility="The Prison",
            housing_unit="CB4",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="SP",
        )

        # Assert
        expected_result = entities.StateIncarcerationPeriod(
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="P",
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="REV",
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            release_reason_raw_text="SS",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            specialized_purpose_for_incarceration_raw_text="60DAY",
            external_id="INCARCERATION_ID",
            admission_date=date(year=2111, month=1, day=2),
            release_date=date(year=2112, month=2, day=2),
            state_code="US_XX",
            county_code="BIS",
            facility="THE PRISON",
            housing_unit="CB4",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="SP",
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateCharge(self) -> None:
        result = deserialize_entity_factories.StateChargeFactory.deserialize(
            status=StateChargeStatus.ACQUITTED,
            status_raw_text="ACQ",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="AA",
            offense_type="OTHER",
            is_violent="False",
            is_sex_offense="False",
            external_id="CHARGE_ID",
            offense_date="1/2/2111",
            date_charged="1/10/2111",
            state_code="us_xx",
            ncic_code="4801",
            statute="ab54.21c",
            description="CONSPIRACY",
            attempted="False",
            counts="4",
            charge_notes="Have I told you about that time I saw Janelle Monae "
            "open for of Montreal at the 9:30 Club?",
            is_controlling="True",
            charging_entity="SCOTUS",
        )

        # Assert
        expected_result = entities.StateCharge(
            status=StateChargeStatus.ACQUITTED,
            status_raw_text="ACQ",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="F",
            classification_subtype="AA",
            offense_type="OTHER",
            is_violent=False,
            is_sex_offense=False,
            external_id="CHARGE_ID",
            offense_date=date(year=2111, month=1, day=2),
            date_charged=date(year=2111, month=1, day=10),
            state_code="US_XX",
            ncic_code="4801",
            statute="AB54.21C",
            description="CONSPIRACY",
            attempted=False,
            counts=4,
            charge_notes="HAVE I TOLD YOU ABOUT THAT TIME I SAW JANELLE MONAE "
            "OPEN FOR OF MONTREAL AT THE 9:30 CLUB?",
            is_controlling=True,
            charging_entity="SCOTUS",
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateSupervisionViolationResponseDecisionEntry(self) -> None:
        result = deserialize_entity_factories.StateSupervisionViolationResponseDecisionEntryFactory.deserialize(
            state_code="us_xx",
            decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            decision_raw_text="PR",
        )

        # Assert
        expected_result = entities.StateSupervisionViolationResponseDecisionEntry(
            state_code="US_XX",
            decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            decision_raw_text="PR",
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateIncarcerationIncidentOutcome(self) -> None:
        result = deserialize_entity_factories.StateIncarcerationIncidentOutcomeFactory.deserialize(
            external_id="INCIDENT_OUTCOME_ID",
            outcome_type=StateIncarcerationIncidentOutcomeType.PRIVILEGE_LOSS,
            outcome_type_raw_text="LCP",
            date_effective="1/2/2019",
            hearing_date="12/29/2018",
            report_date="12/30/2019",
            state_code="us_xx",
            outcome_description="Loss of Commissary Privileges",
            punishment_length_days="45",
        )

        # Assert
        expected_result = entities.StateIncarcerationIncidentOutcome(
            external_id="INCIDENT_OUTCOME_ID",
            outcome_type=StateIncarcerationIncidentOutcomeType.PRIVILEGE_LOSS,
            outcome_type_raw_text="LCP",
            date_effective=date(year=2019, month=1, day=2),
            hearing_date=date(year=2018, month=12, day=29),
            report_date=date(year=2019, month=12, day=30),
            state_code="US_XX",
            outcome_description="LOSS OF COMMISSARY PRIVILEGES",
            punishment_length_days=45,
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateSupervisionPeriod(self) -> None:
        result = deserialize_entity_factories.StateSupervisionPeriodFactory.deserialize(
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_type_raw_text="PAR",
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            admission_reason_raw_text="CR",
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            termination_reason_raw_text="D",
            supervision_level=None,
            external_id="SUPERVISION_ID",
            start_date="1/2/2111",
            termination_date="2/2/2112",
            state_code="us_xx",
            county_code="bis",
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            custodial_authority_raw_text="SUP",
            supervision_site="07-CENTRAL",
            conditions="CURFEW, DRINKING",
        )

        # Assert
        expected_result = entities.StateSupervisionPeriod(
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_type_raw_text="PAR",
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            admission_reason_raw_text="CR",
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            termination_reason_raw_text="D",
            supervision_level=None,
            supervision_level_raw_text=None,
            external_id="SUPERVISION_ID",
            start_date=date(year=2111, month=1, day=2),
            termination_date=date(year=2112, month=2, day=2),
            state_code="US_XX",
            county_code="BIS",
            supervision_site="07-CENTRAL",
            conditions="CURFEW, DRINKING",
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            custodial_authority_raw_text="SUP",
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateIncarcerationIncident(self) -> None:
        result = (
            deserialize_entity_factories.StateIncarcerationIncidentFactory.deserialize(
                external_id="INCIDENT_ID",
                incident_type=StateIncarcerationIncidentType.CONTRABAND,
                incident_type_raw_text="C",
                incident_date="1/2/1111",
                state_code="us_xx",
                facility="Alcatraz",
                location_within_facility="13B",
                incident_details=NormalizedSerializableJSON(
                    notes="Inmate was told to be quiet and would not comply",
                ),
            )
        )

        # Assert
        expected_result = entities.StateIncarcerationIncident(
            external_id="INCIDENT_ID",
            incident_type=StateIncarcerationIncidentType.CONTRABAND,
            incident_type_raw_text="C",
            incident_date=date(year=1111, month=1, day=2),
            state_code="US_XX",
            facility="ALCATRAZ",
            location_within_facility="13B",
            incident_details='{"notes": "INMATE WAS TOLD TO BE QUIET AND WOULD NOT COMPLY"}',
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StatePersonAlias(self) -> None:
        result = deserialize_entity_factories.StatePersonAliasFactory.deserialize(
            state_code="us_xx",
            alias_type=StatePersonAliasType.GIVEN_NAME,
            alias_type_raw_text="G",
            full_name=NormalizedSerializableJSON(full_name="full NAME"),
        )

        # Assert
        expected_result = entities.StatePersonAlias(
            state_code="US_XX",
            alias_type=StatePersonAliasType.GIVEN_NAME,
            alias_type_raw_text="G",
            full_name='{"full_name": "FULL NAME"}',
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateEmploymentPeriod(self) -> None:
        result = deserialize_entity_factories.StateEmploymentPeriodFactory.deserialize(
            state_code="us_xx",
            external_id="12356",
            start_date="2022-05-08",
            end_date="2022-05-10",
            last_verified_date="2022-05-01",
            employment_status=StateEmploymentPeriodEmploymentStatus.EMPLOYED_PART_TIME,
            employment_status_raw_text="PT",
            end_reason=StateEmploymentPeriodEndReason.QUIT,
            end_reason_raw_text="Personal",
            employer_name="Acme, Inc.",
            employer_address="123 Fake St, Anytown, XX, 00000",
            job_title=None,
        )

        # Assert
        expected_result = entities.StateEmploymentPeriod(
            state_code="US_XX",
            external_id="12356",
            start_date=datetime.date(2022, 5, 8),
            end_date=datetime.date(2022, 5, 10),
            last_verified_date=datetime.date(2022, 5, 1),
            employment_status=StateEmploymentPeriodEmploymentStatus.EMPLOYED_PART_TIME,
            employment_status_raw_text="PT",
            end_reason=StateEmploymentPeriodEndReason.QUIT,
            end_reason_raw_text="PERSONAL",
            employer_name="ACME, INC.",
            employer_address="123 FAKE ST, ANYTOWN, XX, 00000",
            job_title=None,
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateDrugScreen(self) -> None:
        result = deserialize_entity_factories.StateDrugScreenFactory.deserialize(
            state_code="us_xx",
            external_id="12356",
            drug_screen_date="2022-05-08",
            drug_screen_result=StateDrugScreenResult.NEGATIVE,
            drug_screen_result_raw_text="DRUN",
            sample_type=StateDrugScreenSampleType.BREATH,
            sample_type_raw_text="BREATH",
            drug_screen_metadata='{"DrugType": "meth"}',
        )

        # Assert
        expected_result = entities.StateDrugScreen(
            state_code="US_XX",
            external_id="12356",
            drug_screen_date=datetime.date(2022, 5, 8),
            drug_screen_result=StateDrugScreenResult.NEGATIVE,
            drug_screen_result_raw_text="DRUN",
            sample_type=StateDrugScreenSampleType.BREATH,
            sample_type_raw_text="BREATH",
            drug_screen_metadata='{"DRUGTYPE": "METH"}',
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateTaskDeadline(self) -> None:
        update_datetime = datetime.datetime.now()
        result = deserialize_entity_factories.StateTaskDeadlineFactory.deserialize(
            state_code="us_xx",
            eligible_date="2022-05-01",
            due_date="2022-05-08",
            update_datetime=update_datetime,
            task_type=StateTaskType.DRUG_SCREEN,
            task_type_raw_text="DRU",
            task_subtype="MY_SUBTYPE",
            task_metadata=NormalizedSerializableJSON(sentence_external_id="a123"),
        )

        # Assert
        expected_result = entities.StateTaskDeadline(
            state_code="US_XX",
            eligible_date=datetime.date(2022, 5, 1),
            due_date=datetime.date(2022, 5, 8),
            update_datetime=update_datetime,
            task_type=StateTaskType.DRUG_SCREEN,
            task_type_raw_text="DRU",
            task_subtype="MY_SUBTYPE",
            task_metadata='{"sentence_external_id": "A123"}',
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateTaskDeadline_datetime_parsing(self) -> None:
        update_datetime = datetime.datetime.now()
        result = deserialize_entity_factories.StateTaskDeadlineFactory.deserialize(
            state_code="us_xx",
            eligible_date="2022-05-01",
            due_date="2022-05-08",
            update_datetime=update_datetime.isoformat(),
            task_type=StateTaskType.DRUG_SCREEN,
            task_type_raw_text="DRU",
            task_subtype="MY_SUBTYPE",
        )

        # Assert
        expected_result = entities.StateTaskDeadline(
            state_code="US_XX",
            eligible_date=datetime.date(2022, 5, 1),
            due_date=datetime.date(2022, 5, 8),
            update_datetime=update_datetime,
            task_type=StateTaskType.DRUG_SCREEN,
            task_type_raw_text="DRU",
            task_subtype="MY_SUBTYPE",
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateStaff(self) -> None:
        result = deserialize_entity_factories.StateStaffFactory.deserialize(
            state_code="us_xx",
            full_name=NormalizedSerializableJSON(full_name="full NAME"),
            email="Full.Name@domain.org",
        )

        # Assert
        expected_result = entities.StateStaff.new_with_defaults(
            full_name='{"full_name": "FULL NAME"}',
            state_code="US_XX",
            email="Full.Name@domain.org",
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateStaffExternalId(self) -> None:
        result = deserialize_entity_factories.StateStaffExternalIdFactory.deserialize(
            external_id="123a",
            id_type="state_id",
            state_code="us_xx",
        )

        # Assert
        expected_result = entities.StateStaffExternalId(
            external_id="123A",
            id_type="STATE_ID",
            state_code="US_XX",
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateStaffRolePeriod(self) -> None:
        result = deserialize_entity_factories.StateStaffRolePeriodFactory.deserialize(
            external_id="123a",
            state_code="us_xx",
            start_date="2022-05-08",
            end_date="2022-05-10",
            role_type=StateStaffRoleType.SUPERVISION_OFFICER,
            role_type_raw_text="Officer",
            role_subtype=StateStaffRoleSubtype.SUPERVISION_OFFICER_SUPERVISOR,
            role_subtype_raw_text="Supervisor",
        )

        # Assert
        expected_result = entities.StateStaffRolePeriod(
            external_id="123A",
            state_code="US_XX",
            start_date=datetime.date(2022, 5, 8),
            end_date=datetime.date(2022, 5, 10),
            role_type=StateStaffRoleType.SUPERVISION_OFFICER,
            role_type_raw_text="OFFICER",
            role_subtype=StateStaffRoleSubtype.SUPERVISION_OFFICER_SUPERVISOR,
            role_subtype_raw_text="SUPERVISOR",
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateStaffSupervisorPeriod(self) -> None:
        result = (
            deserialize_entity_factories.StateStaffSupervisorPeriodFactory.deserialize(
                external_id="123a",
                state_code="us_xx",
                start_date="2022-05-08",
                end_date="2022-05-10",
                supervisor_staff_external_id="abc",
                supervisor_staff_external_id_type="US_XX_STAFF_ID",
            )
        )

        # Assert
        expected_result = entities.StateStaffSupervisorPeriod(
            state_code="US_XX",
            external_id="123A",
            start_date=datetime.date(2022, 5, 8),
            end_date=datetime.date(2022, 5, 10),
            supervisor_staff_external_id="ABC",
            supervisor_staff_external_id_type="US_XX_STAFF_ID",
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateStaffLocationPeriod(self) -> None:
        result = (
            deserialize_entity_factories.StateStaffLocationPeriodFactory.deserialize(
                external_id="123a",
                state_code="us_xx",
                start_date="2022-05-08",
                end_date="2022-05-10",
                location_external_id="abc123",
            )
        )

        # Assert
        expected_result = entities.StateStaffLocationPeriod(
            state_code="US_XX",
            external_id="123A",
            start_date=datetime.date(2022, 5, 8),
            end_date=datetime.date(2022, 5, 10),
            location_external_id="ABC123",
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateStaffCaseloadTypePeriod(self) -> None:
        result = deserialize_entity_factories.StateStaffCaseloadTypePeriodFactory.deserialize(
            state_code="us_xx",
            external_id="123A",
            start_date="2022-05-08",
            end_date="2022-05-10",
            caseload_type=StateStaffCaseloadType.ADMINISTRATIVE_SUPERVISION,
            caseload_type_raw_text="ADMINSTRATIVE",
        )

        # Assert
        expected_result = entities.StateStaffCaseloadTypePeriod(
            state_code="US_XX",
            external_id="123A",
            start_date=datetime.date(2022, 5, 8),
            end_date=datetime.date(2022, 5, 10),
            caseload_type=StateStaffCaseloadType.ADMINISTRATIVE_SUPERVISION,
            caseload_type_raw_text="ADMINSTRATIVE",
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateSentence(self) -> None:
        result = deserialize_entity_factories.StateSentenceFactory.deserialize(
            state_code="us_xx",
            sentence_type=StateSentenceType.STATE_PRISON,
            external_id="123A",
            imposed_date="2022-05-08",
        )

        # Assert
        expected_result = entities.StateSentence(
            state_code="US_XX",
            sentence_type=StateSentenceType.STATE_PRISON,
            external_id="123A",
            imposed_date=datetime.date(2022, 5, 8),
        )

        self.assertEqual(expected_result, result)

    def test_deserialize_StateChargeV2(self) -> None:
        # TODO(#26240): Replace StateChargeFactory with this one
        result = deserialize_entity_factories.StateChargeV2Factory.deserialize(
            charge_v2_id=1,
            external_id="CHARGE-EXTERNAL-ID",
            state_code="US_XX",
            status=StateChargeV2Status.CONVICTED,
            offense_date="2023-01-01",
            date_charged="2023-02-12",
            ncic_code="NCIC-CODE",
            statute="THE STATUTE",
        )
        expected_result = entities.StateChargeV2(
            charge_v2_id=1,
            external_id="CHARGE-EXTERNAL-ID",
            state_code="US_XX",
            status=StateChargeV2Status.CONVICTED,
            offense_date=datetime.date(2023, 1, 1),
            date_charged=datetime.date(2023, 2, 12),
            ncic_code="NCIC-CODE",
            statute="THE STATUTE",
        )
        self.assertEqual(expected_result, result)

    def test_deserialize_StateSentenceStatusSnapshot(self) -> None:
        result = (
            deserialize_entity_factories.StateSentenceStatusSnapshotFactory.deserialize(
                state_code="US_XX",
                status=StateSentenceStatus.SERVING,
                status_update_datetime="2023-01-01",
                sequence_num=None,
            )
        )
        expected_result = entities.StateSentenceStatusSnapshot(
            state_code="US_XX",
            status=StateSentenceStatus.SERVING,
            status_update_datetime=datetime.datetime(2023, 1, 1),
            sequence_num=None,
        )
        self.assertEqual(expected_result, result)

    def test_deserialize_StateSentenceLength(self) -> None:
        result = deserialize_entity_factories.StateSentenceLengthFactory.deserialize(
            state_code="US_XX",
            length_update_datetime="2023-01-01",
            sequence_num=None,
        )
        expected_result = entities.StateSentenceLength(
            state_code="US_XX",
            length_update_datetime=datetime.datetime(2023, 1, 1),
            sequence_num=None,
        )
        self.assertEqual(expected_result, result)

    def test_deserialize_StateSentenceGroup(self) -> None:
        result = deserialize_entity_factories.StateSentenceGroupFactory.deserialize(
            state_code="US_XX",
            external_id="SENTENCE GROUP",
        )
        expected_result = entities.StateSentenceGroup(
            state_code="US_XX",
            external_id="SENTENCE GROUP",
        )
        self.assertEqual(expected_result, result)

    def test_deserialize_StateSentenceGroupLength(self) -> None:
        result = (
            deserialize_entity_factories.StateSentenceGroupLengthFactory.deserialize(
                state_code="US_XX",
                group_update_datetime="2023-01-01",
                sequence_num=None,
            )
        )
        expected_result = entities.StateSentenceGroupLength(
            state_code="US_XX",
            group_update_datetime=datetime.datetime(2023, 1, 1),
            sequence_num=None,
        )
        self.assertEqual(expected_result, result)

    def test_deserialize_StatePersonStaffRelationshipPeriod(self) -> None:
        result = deserialize_entity_factories.StatePersonStaffRelationshipPeriodFactory.deserialize(
            state_code="US_XX",
            relationship_start_date=datetime.date(2021, 1, 1),
            relationship_end_date_exclusive="2023-01-01",
            system_type=StateSystemType.INCARCERATION,
            relationship_type=StatePersonStaffRelationshipType.CASE_MANAGER,
            associated_staff_external_id="ABC123",
            associated_staff_external_id_type="US_XX_STAFF_ID",
            relationship_priority=1,
        )
        expected_result = entities.StatePersonStaffRelationshipPeriod(
            state_code=StateCode.US_XX.value,
            relationship_start_date=datetime.date(2021, 1, 1),
            relationship_end_date_exclusive=datetime.date(2023, 1, 1),
            system_type=StateSystemType.INCARCERATION,
            relationship_type=StatePersonStaffRelationshipType.CASE_MANAGER,
            associated_staff_external_id="ABC123",
            associated_staff_external_id_type="US_XX_STAFF_ID",
            relationship_priority=1,
        )
        self.assertEqual(expected_result, result)
