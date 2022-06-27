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

from recidiviz.persistence.database.schema.aggregate import schema as aggregate_schema
from recidiviz.persistence.database.schema.case_triage import (
    schema as case_triage_schema,
)
from recidiviz.persistence.database.schema.county import schema as county_schema
from recidiviz.persistence.database.schema.operations import schema as operations_schema
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema_utils import (
    SchemaType,
    _get_all_database_entities_in_module,
    get_all_table_classes,
    get_database_entity_by_table_name,
    get_state_database_entities,
    get_state_database_entity_with_name,
    schema_type_to_schema_base,
)


def test_get_all_database_entity_classes() -> None:
    aggregate_database_entity_names = [
        "CaFacilityAggregate",
        "CoFacilityAggregate",
        "DcFacilityAggregate",
        "FlCountyAggregate",
        "FlFacilityAggregate",
        "GaCountyAggregate",
        "HiFacilityAggregate",
        "InCountyAggregate",
        "KyFacilityAggregate",
        "MaFacilityAggregate",
        "NyFacilityAggregate",
        "PaCountyPreSentencedAggregate",
        "PaFacilityPopAggregate",
        "SingleCountAggregate",
        "TnFacilityAggregate",
        "TnFacilityFemaleAggregate",
        "TxCountyAggregate",
        "WvFacilityAggregate",
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
        "StateAssessment",
        "StateCharge",
        "StateCourtCase",
        "StateDrugScreen",
        "StateEmploymentPeriod",
        "StateEarlyDischarge",
        "StateIncarcerationIncident",
        "StateIncarcerationIncidentOutcome",
        "StateIncarcerationPeriod",
        "StateIncarcerationSentence",
        "StatePerson",
        "StatePersonAlias",
        "StatePersonEthnicity",
        "StatePersonExternalId",
        "StatePersonRace",
        "StateProgramAssignment",
        "StateSupervisionCaseTypeEntry",
        "StateSupervisionContact",
        "StateSupervisionViolatedConditionEntry",
        "StateSupervisionPeriod",
        "StateSupervisionSentence",
        "StateSupervisionViolation",
        "StateSupervisionViolationTypeEntry",
        "StateSupervisionViolationResponse",
        "StateSupervisionViolationResponseDecisionEntry",
    ]
    operations_database_entity_names = [
        "DirectIngestViewMaterializationMetadata",
        "DirectIngestRawFileMetadata",
        "DirectIngestSftpFileMetadata",
        "DirectIngestInstancePauseStatus",
        "DirectIngestInstanceStatus",
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

    all_database_entity_names = _database_entities_to_qualified_names(  # type: ignore[assignment]
        all_database_entity_names
    )

    assert sorted(all_database_entity_names) == sorted(expected_qualified_names)  # type: ignore[type-var]


def test_get_all_table_classes() -> None:
    aggregate_table_names = [
        "ca_facility_aggregate",
        "co_facility_aggregate",
        "dc_facility_aggregate",
        "fl_county_aggregate",
        "fl_facility_aggregate",
        "ga_county_aggregate",
        "hi_facility_aggregate",
        "in_county_aggregate",
        "ky_facility_aggregate",
        "ma_facility_aggregate",
        "ny_facility_aggregate",
        "pa_county_pre_sentenced_aggregate",
        "pa_facility_pop_aggregate",
        "single_count_aggregate",
        "tn_facility_aggregate",
        "tn_facility_female_aggregate",
        "tx_county_aggregate",
        "wv_facility_aggregate",
    ]
    case_triage_table_names = [
        "etl_clients",
        "etl_officers",
        "etl_opportunities",
        "etl_client_events",
        "case_update_actions",
        "client_info",
        "officer_notes",
        "opportunity_deferrals",
        "dashboard_user_restrictions",
        "officer_metadata",
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
        "user_account",
        "datapoint",
        "datapoint_history",
    ]
    pathways_table_names = [
        "liberty_to_prison_transitions",
        "prison_to_supervision_transitions",
        "supervision_to_prison_transitions",
    ]
    state_table_names = [
        "state_agent",
        "state_assessment",
        "state_charge",
        "state_court_case",
        "state_drug_screen",
        "state_employment_period",
        "state_early_discharge",
        "state_incarceration_incident",
        "state_incarceration_incident_outcome",
        "state_incarceration_period",
        "state_incarceration_sentence",
        "state_person",
        "state_person_alias",
        "state_person_ethnicity",
        "state_person_external_id",
        "state_person_race",
        "state_program_assignment",
        "state_supervision_violated_condition_entry",
        "state_supervision_period",
        "state_supervision_sentence",
        "state_supervision_case_type_entry",
        "state_supervision_contact",
        "state_supervision_violation",
        "state_supervision_violation_type_entry",
        "state_supervision_violation_response",
        "state_supervision_violation_response_decision_entry",
        "state_charge_incarceration_sentence_association",
        "state_charge_supervision_sentence_association",
        "state_supervision_violation_response_decision_agent_association",
    ]
    operations_table_names = [
        "direct_ingest_view_materialization_metadata",
        "direct_ingest_raw_file_metadata",
        "direct_ingest_sftp_file_metadata",
        "direct_ingest_instance_pause_status",
        "direct_ingest_instance_status",
    ]

    expected_table_class_names = (
        aggregate_table_names
        + case_triage_table_names
        + county_table_names
        + justice_counts_table_names
        + operations_table_names
        + pathways_table_names
        + state_table_names
    )

    all_table_classes = get_all_table_classes()

    assert sorted(expected_table_class_names) == sorted(
        _table_classes_to_qualified_names(all_table_classes)
    )


def test_get_state_table_class_with_name() -> None:
    class_name = "StateSupervisionViolation"

    assert (
        get_state_database_entity_with_name(class_name)
        == state_schema.StateSupervisionViolation
    )


def test_get_state_table_class_with_name_invalid_name() -> None:
    class_name = "XXX"

    with pytest.raises(LookupError):
        get_state_database_entity_with_name(class_name)


def test_get_state_database_entities() -> None:
    state_database_entity_names = [
        "StateAgent",
        "StateAssessment",
        "StateCharge",
        "StateCourtCase",
        "StateDrugScreen",
        "StateEmploymentPeriod",
        "StateEarlyDischarge",
        "StateIncarcerationIncident",
        "StateIncarcerationIncidentOutcome",
        "StateIncarcerationPeriod",
        "StateIncarcerationSentence",
        "StatePerson",
        "StatePersonAlias",
        "StatePersonEthnicity",
        "StatePersonExternalId",
        "StatePersonRace",
        "StateProgramAssignment",
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
        get_state_database_entities()
    )
    assert sorted(found_database_entity_names) == sorted(expected_database_entity_names)


def _prefix_module_name(module_name: str, class_name_list: List[str]) -> List[str]:
    return [f"{module_name}.{class_name}" for class_name in class_name_list]


def _database_entities_to_qualified_names(database_entities) -> List[str]:
    return [f"{cls.__module__}.{cls.__name__}" for cls in list(database_entities)]


def _table_classes_to_qualified_names(table_classes) -> List[str]:
    return [f"{table.name}" for table in list(table_classes)]


def test_schema_type_to_schema_base() -> None:
    schema_bases = [
        # Shouldn't crash for any schema
        schema_type_to_schema_base(schema_type)
        for schema_type in SchemaType
    ]

    # Shouldn't return duplicate schemas
    assert len(set(schema_bases)) == len(schema_bases)


def test_get_database_entity_by_table_name() -> None:

    assert (
        get_database_entity_by_table_name(case_triage_schema, "etl_clients")
        == case_triage_schema.ETLClient
    )

    with pytest.raises(ValueError, match="Could not find model with table named foo"):
        get_database_entity_by_table_name(case_triage_schema, "foo")
