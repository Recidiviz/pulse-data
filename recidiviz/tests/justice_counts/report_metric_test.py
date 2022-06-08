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
"""This class implements tests for Justice Counts ReportMetric class."""

from typing import Any, Dict
from unittest import TestCase

import recidiviz.justice_counts.metrics.law_enforcement as law_enforcement_metric_definitions
from recidiviz.common.constants.justice_counts import ContextKey
from recidiviz.justice_counts.dimensions.law_enforcement import (
    LawEnforcementStaffType,
    OffenseType,
    SheriffBudgetType,
)
from recidiviz.justice_counts.dimensions.person import GenderRestricted
from recidiviz.justice_counts.metrics import law_enforcement
from recidiviz.justice_counts.metrics.metric_definition import CallsRespondedOptions
from recidiviz.justice_counts.metrics.report_metric import (
    ReportedAggregatedDimension,
    ReportedContext,
    ReportMetric,
)
from recidiviz.tests.justice_counts.utils import JusticeCountsSchemaTestObjects


class TestJusticeCountsReportMetric(TestCase):
    """Implements tests for the Justice Counts ReportMetric class."""

    def setUp(self) -> None:
        self.test_schema_objects = JusticeCountsSchemaTestObjects()
        self.reported_budget = self.test_schema_objects.reported_budget_metric
        self.reported_calls_for_service = (
            self.test_schema_objects.reported_calls_for_service_metric
        )

    def test_init(self) -> None:
        self.assertEqual(
            self.reported_budget.metric_definition.display_name, "Annual Budget"
        )

    def test_value_validation(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "Not all dimension instances belong to the same class"
        ):
            ReportMetric(
                key=law_enforcement.annual_budget.key,
                value=100000,
                aggregated_dimensions=[
                    ReportedAggregatedDimension(
                        dimension_to_value={
                            SheriffBudgetType.DETENTION: 50000,
                            GenderRestricted.FEMALE: 100,
                        }
                    )
                ],
            )

    def test_context_validation(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "The required context ContextKey.ALL_CALLS_OR_CALLS_RESPONDED is missing",
        ):
            ReportMetric(
                key=law_enforcement.calls_for_service.key,
                value=100000,
                contexts=[],
                aggregated_dimensions=[],
                enforce_required_fields=True,
            )

        with self.assertRaisesRegex(
            ValueError,
            r"The context ContextKey.ALL_CALLS_OR_CALLS_RESPONDED is reported as a <enum 'CallsRespondedOptions'> but typed as a \(<class 'str'>,\).",
        ):
            ReportMetric(
                key=law_enforcement.calls_for_service.key,
                value=100000,
                contexts=[
                    ReportedContext(
                        key=ContextKey.ALL_CALLS_OR_CALLS_RESPONDED,
                        value=CallsRespondedOptions.ALL_CALLS,
                    )
                ],
                aggregated_dimensions=[],
            )

    def test_aggregated_dimensions_validation(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "The following required dimensions are missing: {'metric/law_enforcement/calls_for_service/type'}",
        ):
            ReportMetric(
                key=law_enforcement.calls_for_service.key,
                value=100000,
                contexts=[
                    ReportedContext(
                        key=ContextKey.ALL_CALLS_OR_CALLS_RESPONDED,
                        value=CallsRespondedOptions.ALL_CALLS.value,
                    )
                ],
                aggregated_dimensions=[],
                enforce_required_fields=True,
            )

    def test_budget_report_metric_json(self) -> None:
        self.maxDiff = None
        reported_metric = self.test_schema_objects.get_reported_budget_metric()
        self.assertEqual(
            reported_metric.to_json(),
            {
                "key": reported_metric.key,
                "system": "LAW_ENFORCEMENT",
                "display_name": "Annual Budget",
                "reporting_note": None,
                "description": "Measures the total annual budget (in dollars) of the agency.",
                "definitions": [],
                "category": "CAPACITY AND COST",
                "value": 100000,
                "unit": "USD",
                "label": "Annual Budget",
                "contexts": [
                    {
                        "key": "PRIMARY_FUNDING_SOURCE",
                        "display_name": "Please describe your primary funding source.",
                        "reporting_note": None,
                        "required": False,
                        "type": "TEXT",
                        "value": "government",
                        "multiple_choice_options": [],
                    },
                    {
                        "key": "ADDITIONAL_CONTEXT",
                        "display_name": "Additional context",
                        "reporting_note": "Add any additional context that you would like to provide here.",
                        "required": False,
                        "type": "TEXT",
                        "value": None,
                        "multiple_choice_options": [],
                    },
                ],
                "disaggregations": [
                    {
                        "key": "metric/law_enforcement/budget/type",
                        "display_name": "Sheriff Budget Types",
                        "required": False,
                        "should_sum_to_total": False,
                        "helper_text": None,
                        "dimensions": [
                            {
                                "key": "Detention",
                                "label": "Detention",
                                "value": 66666,
                                "reporting_note": "Sheriff Budget: Detention",
                            },
                            {
                                "key": "Patrol",
                                "label": "Patrol",
                                "value": 33334,
                                "reporting_note": "Sheriff Budget: Patrol",
                            },
                        ],
                    }
                ],
            },
        )

    def test_reported_calls_for_service_metric_json(self) -> None:
        reported_metric = (
            self.test_schema_objects.get_reported_calls_for_service_metric()
        )
        metric_definition = law_enforcement_metric_definitions.calls_for_service
        self.assertEqual(
            reported_metric.to_json(),
            {
                "key": reported_metric.key,
                "system": "LAW_ENFORCEMENT",
                "display_name": metric_definition.display_name,
                "reporting_note": metric_definition.reporting_note,
                "description": metric_definition.description,
                "definitions": [
                    d.to_json() for d in metric_definition.definitions or []
                ],
                "category": metric_definition.category.value,
                "value": reported_metric.value,
                "unit": metric_definition.metric_type.unit,
                "label": "Calls for Service",
                "contexts": [
                    {
                        "key": "ALL_CALLS_OR_CALLS_RESPONDED",
                        "display_name": "Does the total value include all calls or just those responded to?",
                        "reporting_note": None,
                        "required": True,
                        "type": "MULTIPLE_CHOICE",
                        "value": CallsRespondedOptions.ALL_CALLS.value,
                        "multiple_choice_options": [
                            CallsRespondedOptions.ALL_CALLS.value,
                            CallsRespondedOptions.CALLS_RESPONDED.value,
                        ],
                    },
                    {
                        "key": "AGENCIES_AVAILABLE_FOR_RESPONSE",
                        "display_name": "Please list the names of all agencies available for response.",
                        "reporting_note": None,
                        "required": False,
                        "type": "TEXT",
                        "value": None,
                        "multiple_choice_options": [],
                    },
                    {
                        "key": "ADDITIONAL_CONTEXT",
                        "display_name": "Additional context",
                        "reporting_note": "Add any additional context that you would like to provide here.",
                        "required": False,
                        "type": "TEXT",
                        "value": None,
                        "multiple_choice_options": [],
                    },
                ],
                "disaggregations": [
                    {
                        "key": "metric/law_enforcement/calls_for_service/type",
                        "display_name": "Call Types",
                        "required": True,
                        "should_sum_to_total": False,
                        "helper_text": None,
                        "dimensions": [
                            {
                                "key": "Emergency",
                                "label": "Emergency",
                                "value": 20,
                                "reporting_note": "Call: Emergency",
                            },
                            {
                                "key": "Non-emergency",
                                "label": "Non-emergency",
                                "value": 60,
                                "reporting_note": "Call: Non-emergency",
                            },
                            {
                                "key": "Unknown",
                                "label": "Unknown",
                                "value": 20,
                                "reporting_note": "Call: Unknown",
                            },
                        ],
                    }
                ],
            },
        )

    def test_civilian_complaints_sustained_metric_json(self) -> None:
        reported_metric = (
            self.test_schema_objects.get_civilian_complaints_sustained_metric()
        )
        metric_definition = (
            law_enforcement_metric_definitions.civilian_complaints_sustained
        )
        self.assertEqual(
            reported_metric.to_json(),
            {
                "key": reported_metric.key,
                "system": "LAW_ENFORCEMENT",
                "display_name": metric_definition.display_name,
                "reporting_note": metric_definition.reporting_note,
                "description": metric_definition.description,
                "definitions": [
                    {
                        "term": "Complaint",
                        "definition": "One case that represents one or more acts committed by the same officer, or group of officers at the same time and place. Count all complaints, regardless of whether an underlying incident was filed.",
                    },
                    {
                        "term": "Sustained",
                        "definition": "Found to be supported by the evidence, and may or may not result in disciplinary action.",
                    },
                ],
                "category": metric_definition.category.value,
                "value": reported_metric.value,
                "unit": metric_definition.metric_type.unit,
                "label": "Civilian Complaints Sustained",
                "contexts": [
                    {
                        "key": "ADDITIONAL_CONTEXT",
                        "display_name": "Additional context",
                        "reporting_note": "Add any additional context that you would like to provide here.",
                        "required": False,
                        "type": "TEXT",
                        "value": None,
                        "multiple_choice_options": [],
                    },
                ],
                "disaggregations": [],
            },
        )

    def test_total_arrests_metric_json(self) -> None:
        reported_metric = self.test_schema_objects.get_total_arrests_metric()
        metric_definition = law_enforcement_metric_definitions.total_arrests
        self.assertEqual(
            reported_metric.to_json(),
            {
                "key": reported_metric.key,
                "system": "LAW_ENFORCEMENT",
                "display_name": metric_definition.display_name,
                "reporting_note": metric_definition.reporting_note,
                "description": metric_definition.description,
                "definitions": [],
                "category": metric_definition.category.value,
                "value": reported_metric.value,
                "unit": metric_definition.metric_type.unit,
                "label": "Total Arrests",
                "contexts": [
                    {
                        "key": "JURISDICTION_DEFINITION_OF_ARREST",
                        "display_name": "Please provide your jurisdiction's definition of arrest.",
                        "reporting_note": None,
                        "required": True,
                        "type": "TEXT",
                        "value": "it is an arrest",
                        "multiple_choice_options": [],
                    },
                    {
                        "key": "ADDITIONAL_CONTEXT",
                        "display_name": "Additional context",
                        "reporting_note": "Add any additional context that you would like to provide here.",
                        "required": False,
                        "type": "TEXT",
                        "value": "this is a test for additional context",
                        "multiple_choice_options": [],
                    },
                ],
                "disaggregations": [
                    {
                        "key": "metric/law_enforcement/reported_crime/type",
                        "display_name": "Offense Types",
                        "required": True,
                        "should_sum_to_total": False,
                        "helper_text": None,
                        "dimensions": [
                            {
                                "key": "Drug",
                                "label": "Drug",
                                "value": 60,
                                "reporting_note": "Offense: Drug",
                            },
                            {
                                "key": "Person",
                                "label": "Person",
                                "value": 10,
                                "reporting_note": "Offense: Person",
                            },
                            {
                                "key": "Property",
                                "label": "Property",
                                "value": 40,
                                "reporting_note": "Offense: Property",
                            },
                            {
                                "key": "Unknown",
                                "label": "Unknown",
                                "value": 10,
                                "reporting_note": "Offense: Unknown",
                            },
                        ],
                    }
                ],
            },
        )

    def test_arrest_metric_json_to_report_metric(self) -> None:
        metric_definition = law_enforcement_metric_definitions.total_arrests
        response_json = {
            "key": metric_definition.key,
            "value": 100,
            "contexts": [
                {
                    "key": metric_definition.contexts[0].key.value,
                    "value": "definition of arrest",
                    "multiple_choice_options": [],
                }
            ],
            "disaggregations": [
                {
                    "key": OffenseType.dimension_identifier(),
                    "dimensions": [
                        {"key": OffenseType.DRUG.value, "value": 50},
                        {"key": OffenseType.PERSON.value, "value": 50},
                        {"key": OffenseType.PROPERTY.value, "value": 0},
                        {"key": OffenseType.UNKNOWN.value, "value": 0},
                    ],
                }
            ],
        }

        self.assertEqual(
            ReportMetric(
                key=metric_definition.key,
                value=100,
                contexts=[
                    ReportedContext(
                        key=metric_definition.contexts[0].key,
                        value="definition of arrest",
                    )
                ],
                aggregated_dimensions=[
                    ReportedAggregatedDimension(
                        dimension_to_value={
                            OffenseType.DRUG: 50,
                            OffenseType.PERSON: 50,
                            OffenseType.PROPERTY: 0,
                            OffenseType.UNKNOWN: 0,
                        }
                    )
                ],
            ),
            ReportMetric.from_json(json=response_json),
        )

    def test_police_officer_metric_json_to_report_metric(self) -> None:
        metric_definition = law_enforcement_metric_definitions.police_officers
        response_json = {
            "key": metric_definition.key,
            "value": 100,
            "contexts": [
                {
                    "key": metric_definition.contexts[0].key.value,
                    "value": "additional context",
                    "multiple_choice_options": [],
                }
            ],
            "disaggregations": [
                {
                    "key": LawEnforcementStaffType.dimension_identifier(),
                    "dimensions": [
                        {
                            "key": LawEnforcementStaffType.LAW_ENFORCEMENT_OFFICERS.value,
                            "value": 100,
                        },
                        {
                            "key": LawEnforcementStaffType.CIVILIAN_STAFF.value,
                            "value": 50,
                        },
                        {"key": LawEnforcementStaffType.UNKNOWN.value, "value": 0},
                    ],
                }
            ],
        }

        self.assertEqual(
            ReportMetric(
                key=metric_definition.key,
                value=100,
                contexts=[
                    ReportedContext(
                        key=metric_definition.contexts[0].key,
                        value="additional context",
                    )
                ],
                aggregated_dimensions=[
                    ReportedAggregatedDimension(
                        dimension_to_value={
                            LawEnforcementStaffType.LAW_ENFORCEMENT_OFFICERS: 100,
                            LawEnforcementStaffType.CIVILIAN_STAFF: 50,
                            LawEnforcementStaffType.UNKNOWN: 0,
                        }
                    )
                ],
            ),
            ReportMetric.from_json(json=response_json),
        )

    def test_complaints_sustained_metric_json_to_report_metric(self) -> None:
        metric_definition = (
            law_enforcement_metric_definitions.civilian_complaints_sustained
        )
        response_json = {
            "key": metric_definition.key,
            "value": 100,
        }

        self.assertEqual(
            ReportMetric(
                key=metric_definition.key,
                value=100,
                contexts=[],
                aggregated_dimensions=[],
            ),
            ReportMetric.from_json(json=response_json),
        )

    def test_boolean_reported_complaint(self) -> None:
        context_definition = (
            law_enforcement_metric_definitions.calls_for_service.contexts[0]
        )
        response_json = {
            "key": context_definition.key.value,
            "value": CallsRespondedOptions.ALL_CALLS.value,
            "multiple_choice_options": [
                CallsRespondedOptions.ALL_CALLS.value,
                CallsRespondedOptions.CALLS_RESPONDED.value,
            ],
        }

        self.assertEqual(
            ReportedContext.from_json(json=response_json),
            ReportedContext(
                key=ContextKey.ALL_CALLS_OR_CALLS_RESPONDED,
                value=CallsRespondedOptions.ALL_CALLS.value,
            ),
        )

        cleared_input: Dict[str, Any] = {
            "key": context_definition.key.value,
            "value": None,
            "multiple_choice_options": ["Yes", "No"],
        }

        self.assertEqual(
            ReportedContext.from_json(json=cleared_input),
            ReportedContext(key=ContextKey.ALL_CALLS_OR_CALLS_RESPONDED, value=None),
        )
