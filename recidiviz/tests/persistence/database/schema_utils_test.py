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
"""Tests for schema_utils.py."""
from typing import List
import pytest

from recidiviz.persistence.database.schema_utils import (
    get_all_table_classes,
    get_state_database_entity_with_name,
    _get_all_database_entities_in_module,
    get_non_history_state_database_entities,
)

from recidiviz.persistence.database.schema.aggregate import schema as aggregate_schema
from recidiviz.persistence.database.schema.county import schema as county_schema
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema.operations import schema as operations_schema


def test_get_all_database_entity_classes():
    aggregate_database_entity_names = [
        "CaFacilityAggregate",
        "DcFacilityAggregate",
        "FlCountyAggregate",
        "FlFacilityAggregate",
        "GaCountyAggregate",
        "HiFacilityAggregate",
        "KyFacilityAggregate",
        "NyFacilityAggregate",
        "PaCountyPreSentencedAggregate",
        "PaFacilityPopAggregate",
        "SingleCountAggregate",
        "TnFacilityAggregate",
        "TnFacilityFemaleAggregate",
        "TxCountyAggregate",
    ]
    county_database_entity_names = [
        "Arrest",
        "ArrestHistory",
        "Bond",
        "BondHistory",
        "Booking",
        "BookingHistory",
        "Charge",
        "ChargeHistory",
        "Hold",
        "HoldHistory",
        "Person",
        "PersonHistory",
        "Sentence",
        "SentenceHistory",
        "SentenceRelationship",
        "SentenceRelationshipHistory",
        "ScraperSuccess",
    ]
    state_database_entity_names = [
        "StateAgent",
        "StateAgentHistory",
        "StateAssessment",
        "StateAssessmentHistory",
        "StateBond",
        "StateBondHistory",
        "StateCharge",
        "StateChargeHistory",
        "StateCourtCase",
        "StateCourtCaseHistory",
        "StateEarlyDischarge",
        "StateEarlyDischargeHistory",
        "StateFine",
        "StateFineHistory",
        "StateIncarcerationIncident",
        "StateIncarcerationIncidentHistory",
        "StateIncarcerationIncidentOutcome",
        "StateIncarcerationIncidentOutcomeHistory",
        "StateIncarcerationPeriod",
        "StateIncarcerationPeriodHistory",
        "StateIncarcerationSentence",
        "StateIncarcerationSentenceHistory",
        "StateParoleDecision",
        "StateParoleDecisionHistory",
        "StatePerson",
        "StatePersonAlias",
        "StatePersonAliasHistory",
        "StatePersonEthnicity",
        "StatePersonEthnicityHistory",
        "StatePersonExternalId",
        "StatePersonExternalIdHistory",
        "StatePersonHistory",
        "StatePersonRace",
        "StatePersonRaceHistory",
        "StateProgramAssignment",
        "StateProgramAssignmentHistory",
        "StateSentenceGroup",
        "StateSentenceGroupHistory",
        "StateSupervisionCaseTypeEntry",
        "StateSupervisionCaseTypeEntryHistory",
        "StateSupervisionContact",
        "StateSupervisionContactHistory",
        "StateSupervisionViolatedConditionEntry",
        "StateSupervisionViolatedConditionEntryHistory",
        "StateSupervisionPeriod",
        "StateSupervisionPeriodHistory",
        "StateSupervisionSentence",
        "StateSupervisionSentenceHistory",
        "StateSupervisionViolation",
        "StateSupervisionViolationHistory",
        "StateSupervisionViolationTypeEntry",
        "StateSupervisionViolationTypeEntryHistory",
        "StateSupervisionViolationResponse",
        "StateSupervisionViolationResponseHistory",
        "StateSupervisionViolationResponseDecisionEntry",
        "StateSupervisionViolationResponseDecisionEntryHistory",
    ]
    operations_database_entity_names = [
        "DirectIngestIngestFileMetadata",
        "DirectIngestRawFileMetadata",
    ]

    expected_qualified_names = (
        _prefix_module_name(aggregate_schema.__name__, aggregate_database_entity_names)
        + _prefix_module_name(county_schema.__name__, county_database_entity_names)
        + _prefix_module_name(state_schema.__name__, state_database_entity_names)
        + _prefix_module_name(
            operations_schema.__name__, operations_database_entity_names
        )
    )

    all_database_entity_names = (
        list(_get_all_database_entities_in_module(aggregate_schema))
        + list(_get_all_database_entities_in_module(county_schema))
        + list(_get_all_database_entities_in_module(state_schema))
        + list(_get_all_database_entities_in_module(operations_schema))
    )

    all_database_entity_names = _database_entities_to_qualified_names(
        all_database_entity_names
    )

    assert sorted(all_database_entity_names) == sorted(expected_qualified_names)


def test_get_all_table_classes():
    aggregate_table_names = [
        "ca_facility_aggregate",
        "dc_facility_aggregate",
        "fl_county_aggregate",
        "fl_facility_aggregate",
        "ga_county_aggregate",
        "hi_facility_aggregate",
        "ky_facility_aggregate",
        "ny_facility_aggregate",
        "pa_county_pre_sentenced_aggregate",
        "pa_facility_pop_aggregate",
        "single_count_aggregate",
        "tn_facility_aggregate",
        "tn_facility_female_aggregate",
        "tx_county_aggregate",
    ]
    county_table_names = [
        "arrest",
        "arrest_history",
        "bond",
        "bond_history",
        "booking",
        "booking_history",
        "charge",
        "charge_history",
        "hold",
        "hold_history",
        "person",
        "person_history",
        "scraper_success",
        "sentence",
        "sentence_history",
        "sentence_relationship",
        "sentence_relationship_history",
    ]
    justice_counts_table_names = [
        "source",
        "report",
        "report_table_definition",
        "report_table_instance",
        "cell",
    ]
    state_table_names = [
        "state_agent",
        "state_agent_history",
        "state_assessment",
        "state_assessment_history",
        "state_bond",
        "state_bond_history",
        "state_charge",
        "state_charge_history",
        "state_court_case",
        "state_court_case_history",
        "state_early_discharge",
        "state_early_discharge_history",
        "state_fine",
        "state_fine_history",
        "state_incarceration_incident",
        "state_incarceration_incident_history",
        "state_incarceration_incident_outcome",
        "state_incarceration_incident_outcome_history",
        "state_incarceration_period",
        "state_incarceration_period_history",
        "state_incarceration_sentence",
        "state_incarceration_sentence_history",
        "state_parole_decision",
        "state_parole_decision_history",
        "state_person",
        "state_person_alias",
        "state_person_alias_history",
        "state_person_ethnicity",
        "state_person_ethnicity_history",
        "state_person_external_id",
        "state_person_external_id_history",
        "state_person_history",
        "state_person_race",
        "state_person_race_history",
        "state_program_assignment",
        "state_program_assignment_history",
        "state_sentence_group",
        "state_sentence_group_history",
        "state_supervision_violated_condition_entry",
        "state_supervision_violated_condition_entry_history",
        "state_supervision_period",
        "state_supervision_period_history",
        "state_supervision_sentence",
        "state_supervision_sentence_history",
        "state_supervision_case_type_entry",
        "state_supervision_case_type_entry_history",
        "state_supervision_contact",
        "state_supervision_contact_history",
        "state_supervision_violation",
        "state_supervision_violation_history",
        "state_supervision_violation_type_entry",
        "state_supervision_violation_type_entry_history",
        "state_supervision_violation_response",
        "state_supervision_violation_response_history",
        "state_supervision_violation_response_decision_entry",
        "state_supervision_violation_response_decision_entry_history",
        "state_charge_fine_association",
        "state_charge_incarceration_sentence_association",
        "state_charge_supervision_sentence_association",
        "state_incarceration_sentence_incarceration_period_association",
        "state_incarceration_sentence_supervision_period_association",
        "state_incarceration_period_program_assignment_association",
        "state_parole_decision_decision_agent_association",
        "state_supervision_period_program_assignment_association",
        "state_supervision_period_supervision_contact_association",
        "state_supervision_sentence_incarceration_period_association",
        "state_supervision_sentence_supervision_period_association",
        "state_supervision_violation_response_decision_agent_association",
        "state_supervision_period_supervision_violation_association",
    ]
    operations_table_names = [
        "direct_ingest_ingest_file_metadata",
        "direct_ingest_raw_file_metadata",
    ]

    expected_table_class_names = (
        aggregate_table_names
        + county_table_names
        + justice_counts_table_names
        + state_table_names
        + operations_table_names
    )

    all_table_classes = get_all_table_classes()

    assert sorted(expected_table_class_names) == sorted(
        _table_classes_to_qualified_names(all_table_classes)
    )


def test_get_state_table_class_with_name():
    class_name = "StateSupervisionViolation"

    assert (
        get_state_database_entity_with_name(class_name)
        == state_schema.StateSupervisionViolation
    )


def test_get_state_table_class_with_name_invalid_name():
    class_name = "XXX"

    with pytest.raises(LookupError):
        get_state_database_entity_with_name(class_name)


def test_get_non_history_state_database_entities():
    state_database_entity_names = [
        "StateAgent",
        "StateAssessment",
        "StateBond",
        "StateCharge",
        "StateCourtCase",
        "StateEarlyDischarge",
        "StateFine",
        "StateIncarcerationIncident",
        "StateIncarcerationIncidentOutcome",
        "StateIncarcerationPeriod",
        "StateIncarcerationSentence",
        "StateParoleDecision",
        "StatePerson",
        "StatePersonAlias",
        "StatePersonEthnicity",
        "StatePersonExternalId",
        "StatePersonRace",
        "StateProgramAssignment",
        "StateSentenceGroup",
        "StateSupervisionViolatedConditionEntry",
        "StateSupervisionPeriod",
        "StateSupervisionSentence",
        "StateSupervisionCaseTypeEntry",
        "StateSupervisionContact",
        "StateSupervisionViolation",
        "StateSupervisionViolationTypeEntry",
        "StateSupervisionViolationResponse",
        "StateSupervisionViolationResponseDecisionEntry",
    ]

    expected_database_entity_names = _prefix_module_name(
        state_schema.__name__, state_database_entity_names
    )
    found_database_entity_names = _database_entities_to_qualified_names(
        get_non_history_state_database_entities()
    )
    assert sorted(found_database_entity_names) == sorted(expected_database_entity_names)


def _prefix_module_name(module_name: str, class_name_list: List[str]) -> List[str]:
    return [f"{module_name}.{class_name}" for class_name in class_name_list]


def _database_entities_to_qualified_names(database_entities):
    return [f"{cls.__module__}.{cls.__name__}" for cls in list(database_entities)]


def _table_classes_to_qualified_names(table_classes):
    return [f"{table.name}" for table in list(table_classes)]
