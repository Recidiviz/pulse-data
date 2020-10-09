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
import recidiviz.common.constants.state.shared_enums
from recidiviz.common.constants.state import state_assessment, state_charge, \
    state_sentence, state_supervision, state_fine, state_incarceration, \
    state_court_case, state_agent, state_incarceration_period, \
    state_supervision_period, state_incarceration_incident, \
    state_supervision_violation, state_supervision_violation_response, \
    state_parole_decision, state_person_alias, state_program_assignment, state_supervision_contact
from recidiviz.common.constants.state.state_early_discharge import StateEarlyDischargeDecision, \
    StateEarlyDischargeDecisionStatus
from recidiviz.persistence.database.schema import shared_enums
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_utils import \
    get_all_table_classes_in_module, _get_all_database_entities_in_module
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import \
    state_supervision_case_type_entry
from recidiviz.tests.persistence.database.schema.schema_test import (
    TestSchemaEnums,
    TestSchemaTableConsistency)


class TestStateSchemaEnums(TestSchemaEnums):
    """Tests for validating state schema enums are defined correctly"""

    # Test case ensuring enum values match between persistence layer enums and
    # schema enums
    def testPersistenceAndSchemaEnumsMatch(self):
        # Mapping between name of schema enum and persistence layer enum. This
        # map controls which pairs of enums are tested.
        #
        # If a schema enum does not correspond to a persistence layer enum,
        # it should be mapped to None.
        state_enums_mapping = {
            'state_assessment_class': state_assessment.StateAssessmentClass,
            'state_assessment_level': state_assessment.StateAssessmentLevel,
            'state_assessment_type': state_assessment.StateAssessmentType,
            'state_charge_classification_type': state_charge.StateChargeClassificationType,
            'state_sentence_status': state_sentence.StateSentenceStatus,
            'state_supervision_type': state_supervision.StateSupervisionType,
            'state_acting_body_type': recidiviz.common.constants.state.shared_enums.StateActingBodyType,
            'state_early_discharge_decision': StateEarlyDischargeDecision,
            'state_early_discharge_decision_status': StateEarlyDischargeDecisionStatus,
            'state_fine_status': state_fine.StateFineStatus,
            'state_incarceration_type': state_incarceration.StateIncarcerationType,
            'state_court_case_status': state_court_case.StateCourtCaseStatus,
            'state_court_type': state_court_case.StateCourtType,
            'state_agent_type': state_agent.StateAgentType,
            'state_incarceration_period_status': state_incarceration_period.StateIncarcerationPeriodStatus,
            'state_incarceration_facility_security_level':
                state_incarceration_period.StateIncarcerationFacilitySecurityLevel,
            'state_incarceration_period_admission_reason':
                state_incarceration_period.StateIncarcerationPeriodAdmissionReason,
            'state_incarceration_period_release_reason':
                state_incarceration_period.StateIncarcerationPeriodReleaseReason,
            'state_parole_decision_outcome': state_parole_decision.StateParoleDecisionOutcome,
            'state_person_alias_type': state_person_alias.StatePersonAliasType,
            'state_supervision_period_status': state_supervision_period.StateSupervisionPeriodStatus,
            'state_supervision_period_admission_reason': state_supervision_period.StateSupervisionPeriodAdmissionReason,
            'state_supervision_period_supervision_type': state_supervision_period.StateSupervisionPeriodSupervisionType,
            'state_supervision_period_termination_reason':
                state_supervision_period.StateSupervisionPeriodTerminationReason,
            'state_supervision_level': state_supervision_period.StateSupervisionLevel,
            'state_incarceration_incident_type': state_incarceration_incident.StateIncarcerationIncidentType,
            'state_incarceration_incident_outcome_type':
                state_incarceration_incident.StateIncarcerationIncidentOutcomeType,
            'state_specialized_purpose_for_incarceration':
                state_incarceration_period.StateSpecializedPurposeForIncarceration,
            'state_program_assignment_participation_status':
                state_program_assignment.StateProgramAssignmentParticipationStatus,
            'state_program_assignment_discharge_reason': state_program_assignment.StateProgramAssignmentDischargeReason,
            'state_supervision_case_type': state_supervision_case_type_entry.StateSupervisionCaseType,
            'state_supervision_contact_location': state_supervision_contact.StateSupervisionContactLocation,
            'state_supervision_contact_reason': state_supervision_contact.StateSupervisionContactReason,
            'state_supervision_contact_status': state_supervision_contact.StateSupervisionContactStatus,
            'state_supervision_contact_type': state_supervision_contact.StateSupervisionContactType,
            'state_supervision_violation_type': state_supervision_violation.StateSupervisionViolationType,
            'state_supervision_violation_response_type':
                state_supervision_violation_response.StateSupervisionViolationResponseType,
            'state_supervision_violation_response_decision':
                state_supervision_violation_response.StateSupervisionViolationResponseDecision,
            'state_supervision_violation_response_revocation_type':
                state_supervision_violation_response.StateSupervisionViolationResponseRevocationType,
            'state_supervision_violation_response_deciding_body_type':
                state_supervision_violation_response.StateSupervisionViolationResponseDecidingBodyType,
        }

        merged_mapping = {**self.SHARED_ENUMS_TEST_MAPPING,
                          **state_enums_mapping}

        self.check_persistence_and_schema_enums_match(merged_mapping, schema)

    def testAllEnumNamesPrefixedWithState(self):
        shared_enum_ids = \
            [id(enum)
             for enum in self._get_all_sqlalchemy_enums_in_module(shared_enums)]

        for enum in \
                self._get_all_sqlalchemy_enums_in_module(schema):
            if id(enum) in shared_enum_ids:
                continue
            self.assertTrue(enum.name.startswith('state_'))


class TestStateSchemaTableConsistency(TestSchemaTableConsistency):
    """Test class for validating state schema tables are defined correctly"""

    def testAllTableNamesPrefixedWithState(self):
        for cls in get_all_table_classes_in_module(schema):
            self.assertTrue(cls.name.startswith('state_'))

    def testAllDatabaseEntityNamesPrefixedWithState(self):
        for cls in _get_all_database_entities_in_module(schema):
            self.assertTrue(cls.__name__.startswith('State'))
