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
import unittest
from typing import List

from recidiviz.persistence.database.schema.case_triage import (
    schema as case_triage_schema,
)
from recidiviz.persistence.database.schema.operations import schema as operations_schema
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema_utils import (
    SchemaType,
    _get_all_database_entities_in_module,
    get_all_table_classes,
    get_database_entity_by_table_name,
    get_state_database_association_with_names,
    get_state_database_entities,
    get_state_database_entity_with_name,
    schema_type_to_schema_base,
)


def _prefix_module_name(module_name: str, class_name_list: List[str]) -> List[str]:
    return [f"{module_name}.{class_name}" for class_name in class_name_list]


def _database_entities_to_qualified_names(database_entities) -> List[str]:
    return [f"{cls.__module__}.{cls.__name__}" for cls in list(database_entities)]


def _table_classes_to_qualified_names(table_classes) -> List[str]:
    return [f"{table.name}" for table in list(table_classes)]


class TestSchemaUtils(unittest.TestCase):
    """Unit tests for schema_utils.py"""

    def test_get_all_database_entity_classes(self) -> None:
        state_database_entity_names = [
            "StateAgent",
            "StateAssessment",
            "StateCharge",
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
            "StateTaskDeadline",
        ]
        operations_database_entity_names = [
            "DirectIngestViewMaterializationMetadata",
            "DirectIngestRawFileMetadata",
            "DirectIngestSftpFileMetadata",
            "DirectIngestInstanceStatus",
        ]

        expected_qualified_names = _prefix_module_name(
            state_schema.__name__, state_database_entity_names
        ) + _prefix_module_name(
            operations_schema.__name__, operations_database_entity_names
        )

        all_database_entity_names = list(
            _get_all_database_entities_in_module(state_schema)
        ) + list(_get_all_database_entities_in_module(operations_schema))

        all_database_entity_names = _database_entities_to_qualified_names(  # type: ignore[assignment]
            all_database_entity_names
        )

        self.assertEqual(sorted(all_database_entity_names), sorted(expected_qualified_names))  # type: ignore[type-var]

    def test_get_all_table_classes(self) -> None:
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
            "roster",
            "user_override",
            "state_role_permissions",
            "permissions_override",
        ]
        justice_counts_table_names = [
            "source",
            "report",
            "report_table_definition",
            "report_table_instance",
            "cell",
            "user_account",
            "agency_user_account_association",
            "datapoint",
            "datapoint_history",
            "spreadsheet",
            "agency_setting",
        ]
        pathways_table_names = [
            "metric_metadata",
            "liberty_to_prison_transitions",
            "prison_population_over_time",
            "prison_population_by_dimension",
            "prison_to_supervision_transitions",
            "prison_population_person_level",
            "prison_population_projection",
            "supervision_population_over_time",
            "supervision_population_by_dimension",
            "supervision_population_projection",
            "supervision_to_liberty_transitions",
            "supervision_to_prison_transitions",
        ]
        state_table_names = [
            "state_agent",
            "state_assessment",
            "state_charge",
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
            "state_task_deadline",
            "state_charge_incarceration_sentence_association",
            "state_charge_supervision_sentence_association",
            "state_supervision_violation_response_decision_agent_association",
        ]
        operations_table_names = [
            "direct_ingest_view_materialization_metadata",
            "direct_ingest_raw_file_metadata",
            "direct_ingest_sftp_file_metadata",
            "direct_ingest_instance_status",
        ]

        expected_table_class_names = (
            case_triage_table_names
            + justice_counts_table_names
            + operations_table_names
            + pathways_table_names
            + state_table_names
        )

        all_table_classes = get_all_table_classes()

        self.assertEqual(
            sorted(expected_table_class_names),
            sorted(_table_classes_to_qualified_names(all_table_classes)),
        )

    def test_get_state_table_class_with_name(self) -> None:
        class_name = "StateSupervisionViolation"

        self.assertEqual(
            get_state_database_entity_with_name(class_name),
            state_schema.StateSupervisionViolation,
        )

    def test_get_state_table_class_with_name_invalid_name(self) -> None:
        class_name = "XXX"

        with self.assertRaises(LookupError):
            get_state_database_entity_with_name(class_name)

    def test_get_state_database_entities(self) -> None:
        state_database_entity_names = [
            "StateAgent",
            "StateAssessment",
            "StateCharge",
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
            "StateTaskDeadline",
        ]

        expected_database_entity_names = _prefix_module_name(
            state_schema.__name__, state_database_entity_names
        )
        found_database_entity_names = _database_entities_to_qualified_names(
            get_state_database_entities()
        )
        self.assertEqual(
            sorted(found_database_entity_names), sorted(expected_database_entity_names)
        )

    def test_schema_type_to_schema_base(self) -> None:
        schema_bases = [
            # Shouldn't crash for any schema
            schema_type_to_schema_base(schema_type)
            for schema_type in SchemaType
        ]

        # Shouldn't return duplicate schemas
        self.assertEqual(len(set(schema_bases)), len(schema_bases))

    def test_get_database_entity_by_table_name(self) -> None:

        assert (
            get_database_entity_by_table_name(case_triage_schema, "etl_clients")
            == case_triage_schema.ETLClient
        )

        with self.assertRaisesRegex(
            ValueError, ".*Could not find model with table named foo.*"
        ):
            get_database_entity_by_table_name(case_triage_schema, "foo")

    def test_get_state_database_association_with_names(self) -> None:
        charge_incarceration_sentence_association = (
            get_state_database_association_with_names(
                "StateCharge", "StateIncarcerationSentence"
            )
        )
        self.assertEqual(
            charge_incarceration_sentence_association,
            state_schema.state_charge_incarceration_sentence_association_table,
        )
