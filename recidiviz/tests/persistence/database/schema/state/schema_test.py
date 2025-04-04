# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for state-specific SQLAlchemy enums."""
# TODO(#29284) Test the Enums defined through recidiviz.common.constants.state without "state schema"
import unittest
from datetime import date

from recidiviz.common.constants.state import (
    enum_canonical_strings,
    state_assessment,
    state_case_type,
    state_charge,
    state_incarceration,
    state_incarceration_incident,
    state_incarceration_period,
    state_person,
    state_person_address_period,
    state_person_alias,
    state_person_housing_status_period,
    state_person_staff_relationship_period,
    state_program_assignment,
    state_sentence,
    state_shared_enums,
    state_staff_caseload_type,
    state_staff_role_period,
    state_supervision_contact,
    state_supervision_period,
    state_supervision_sentence,
    state_supervision_violated_condition,
    state_supervision_violation,
    state_supervision_violation_response,
    state_system_type,
    state_task_deadline,
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
from recidiviz.common.constants.state.state_person import (
    StateGender,
    StateResidencyStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    get_all_table_classes_in_schema,
    is_association_table,
)
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.state import entities
from recidiviz.tests.persistence.database.schema.schema_test import TestSchemaEnums


class TestStateSchema(unittest.TestCase):
    """Tests for the schema defined in state/schema.py"""

    def test_schema_compiles(self) -> None:
        """Simple test that will crash if the state schema is defined in a way that can't be resolved by SQLAlchemy"""

        _ = schema.StatePerson(
            state_code=StateCode.US_XX.value,
            person_id=1,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

    def test_schema_entities_module_parity(self) -> None:
        sqlalchemy_tables = get_all_table_classes_in_schema(SchemaType.STATE)
        sqlalchemy_tables_by_name = {t.name: t for t in sqlalchemy_tables}
        bq_table_schemas = get_bq_schema_for_entities_module(entities)

        for table_id, table_bq_schema in bq_table_schemas.items():
            if table_id not in sqlalchemy_tables_by_name:
                raise ValueError(f"No table {table_id} defined in state/schema.py")

            bq_column_names = {f.name for f in table_bq_schema}
            sqlalchemy_table = sqlalchemy_tables_by_name[table_id]

            sqlalchemy_column_names = {c.name for c in sqlalchemy_table.columns}

            if is_association_table(table_id):
                # We don't expect SQLAlchemy association tables to have a state_code column
                sqlalchemy_column_names.add("state_code")

            if missing_columns := bq_column_names - sqlalchemy_column_names:
                raise ValueError(
                    f"Table [{table_id}] is missing these columns from the "
                    f"state/schema.py definition: {missing_columns}"
                )


class TestStateSchemaEnums(TestSchemaEnums):
    """Tests for validating state schema enums are defined correctly"""

    # Test case ensuring enum values match between persistence layer enums and
    # schema enums

    def testPersistenceAndSchemaEnumsMatch(self) -> None:
        # Mapping between name of schema enum and persistence layer enum. This
        # map controls which pairs of enums are tested.
        #
        # If a schema enum does not correspond to a persistence layer enum,
        # it should be mapped to None.
        state_enums_mapping = {
            "state_assessment_class": state_assessment.StateAssessmentClass,
            "state_assessment_level": state_assessment.StateAssessmentLevel,
            "state_assessment_type": state_assessment.StateAssessmentType,
            "state_charge_classification_type": state_charge.StateChargeClassificationType,
            "state_charge_status": state_charge.StateChargeStatus,
            "state_drug_screen_result": StateDrugScreenResult,
            "state_drug_screen_sample_type": StateDrugScreenSampleType,
            "state_sentence_status": state_sentence.StateSentenceStatus,
            "state_supervision_sentence_supervision_type": state_supervision_sentence.StateSupervisionSentenceSupervisionType,
            "state_acting_body_type": state_shared_enums.StateActingBodyType,
            "state_custodial_authority": state_shared_enums.StateCustodialAuthority,
            "state_early_discharge_decision": StateEarlyDischargeDecision,
            "state_early_discharge_decision_status": StateEarlyDischargeDecisionStatus,
            "state_employment_period_employment_status": StateEmploymentPeriodEmploymentStatus,
            "state_employment_period_end_reason": StateEmploymentPeriodEndReason,
            "state_incarceration_type": state_incarceration.StateIncarcerationType,
            "state_incarceration_period_admission_reason": state_incarceration_period.StateIncarcerationPeriodAdmissionReason,
            "state_incarceration_period_release_reason": state_incarceration_period.StateIncarcerationPeriodReleaseReason,
            "state_incarceration_period_custody_level": state_incarceration_period.StateIncarcerationPeriodCustodyLevel,
            "state_incarceration_period_housing_unit_type": state_incarceration_period.StateIncarcerationPeriodHousingUnitType,
            "state_incarceration_period_housing_unit_category": state_incarceration_period.StateIncarcerationPeriodHousingUnitCategory,
            "state_gender": state_person.StateGender,
            "state_race": state_person.StateRace,
            "state_ethnicity": state_person.StateEthnicity,
            "state_residency_status": state_person.StateResidencyStatus,
            "state_person_address_type": state_person_address_period.StatePersonAddressType,
            "state_person_housing_status_type": state_person_housing_status_period.StatePersonHousingStatusType,
            "state_person_alias_type": state_person_alias.StatePersonAliasType,
            "state_person_staff_relationship_type": state_person_staff_relationship_period.StatePersonStaffRelationshipType,
            "state_supervision_period_admission_reason": state_supervision_period.StateSupervisionPeriodAdmissionReason,
            "state_supervision_period_supervision_type": state_supervision_period.StateSupervisionPeriodSupervisionType,
            "state_supervision_period_termination_reason": state_supervision_period.StateSupervisionPeriodTerminationReason,
            "state_supervision_level": state_supervision_period.StateSupervisionLevel,
            "state_incarceration_incident_type": state_incarceration_incident.StateIncarcerationIncidentType,
            "state_incarceration_incident_outcome_type": state_incarceration_incident.StateIncarcerationIncidentOutcomeType,
            "state_incarceration_incident_severity": state_incarceration_incident.StateIncarcerationIncidentSeverity,
            "state_specialized_purpose_for_incarceration": state_incarceration_period.StateSpecializedPurposeForIncarceration,
            "state_program_assignment_participation_status": state_program_assignment.StateProgramAssignmentParticipationStatus,
            "state_staff_role_type": state_staff_role_period.StateStaffRoleType,
            "state_staff_role_subtype": state_staff_role_period.StateStaffRoleSubtype,
            "state_staff_caseload_type": state_staff_caseload_type.StateStaffCaseloadType,
            "state_supervision_case_type": state_case_type.StateSupervisionCaseType,
            "state_supervision_contact_location": state_supervision_contact.StateSupervisionContactLocation,
            "state_supervision_contact_reason": state_supervision_contact.StateSupervisionContactReason,
            "state_supervision_contact_status": state_supervision_contact.StateSupervisionContactStatus,
            "state_supervision_contact_type": state_supervision_contact.StateSupervisionContactType,
            "state_supervision_violated_condition_type": state_supervision_violated_condition.StateSupervisionViolatedConditionType,
            "state_supervision_contact_method": state_supervision_contact.StateSupervisionContactMethod,
            "state_supervision_violation_type": state_supervision_violation.StateSupervisionViolationType,
            "state_supervision_violation_severity": state_supervision_violation.StateSupervisionViolationSeverity,
            "state_supervision_violation_response_type": state_supervision_violation_response.StateSupervisionViolationResponseType,
            "state_supervision_violation_response_decision": state_supervision_violation_response.StateSupervisionViolationResponseDecision,
            "state_supervision_violation_response_deciding_body_type": state_supervision_violation_response.StateSupervisionViolationResponseDecidingBodyType,
            "state_supervision_violation_response_severity": state_supervision_violation_response.StateSupervisionViolationResponseSeverity,
            "state_system_type": state_system_type.StateSystemType,
            "state_task_type": state_task_deadline.StateTaskType,
            "state_sentence_type": state_sentence.StateSentenceType,
            "state_charge_v2_classification_type": state_charge.StateChargeV2ClassificationType,
            "state_charge_v2_status": state_charge.StateChargeV2Status,
            "state_sentencing_authority": state_sentence.StateSentencingAuthority,
        }

        self.check_persistence_and_schema_enums_match(state_enums_mapping, schema)

    def testAllEnumNamesPrefixedWithState(self) -> None:
        for enum in self._get_all_sqlalchemy_enums_in_module(schema):
            self.assertTrue(enum.name.startswith("state_"))

    def testAllEnumsHaveUnknownValues(self) -> None:
        # These enums are allowed to not have the EXTERNAL_UNKNOWN value.
        external_unknown_exceptions = {
            # We expect the state to always know where a sentence originated.
            "state_sentencing_authority",
            # We expect the state to always know what type of relationship a person has
            # with a staff member  if they're telling us about the relationship.
            "state_person_staff_relationship_type",
            # The system is a Recidiviz-specific concept - we don't expect to be getting
            # data from the state that indicates they don't know which system a piece of
            # data belongs to.
            "state_system_type",
        }
        for enum in self._get_all_sqlalchemy_enums_in_module(schema):
            self.assertIn(enum_canonical_strings.internal_unknown, enum.enums)
            if enum.name not in external_unknown_exceptions:
                self.assertIn(enum_canonical_strings.external_unknown, enum.enums)
