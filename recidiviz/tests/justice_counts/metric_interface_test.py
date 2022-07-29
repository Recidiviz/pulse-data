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
"""This class implements tests for Justice Counts MetricInterface class."""

from typing import Any, Dict
from unittest import TestCase

import recidiviz.justice_counts.metrics.law_enforcement as law_enforcement_metric_definitions
from recidiviz.common.constants.justice_counts import ContextKey
from recidiviz.justice_counts.dimensions.law_enforcement import (
    CallType,
    LawEnforcementStaffType,
    OffenseType,
)
from recidiviz.justice_counts.dimensions.person import GenderRestricted
from recidiviz.justice_counts.metrics import law_enforcement
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    CallsRespondedOptions,
)
from recidiviz.justice_counts.metrics.metric_interface import (
    DatapointGetRequestEntryPoint,
    MetricAggregatedDimensionData,
    MetricContextData,
    MetricInterface,
)
from recidiviz.tests.justice_counts.utils import JusticeCountsSchemaTestObjects


class TestJusticeCountsMetricInterface(TestCase):
    """Implements tests for the Justice Counts MetricInterface class."""

    def setUp(self) -> None:
        self.test_schema_objects = JusticeCountsSchemaTestObjects()
        self.reported_budget = self.test_schema_objects.reported_budget_metric
        self.reported_calls_for_service = (
            self.test_schema_objects.reported_calls_for_service_metric
        )
        self.maxDiff = None

    def test_init(self) -> None:
        self.assertEqual(
            self.reported_budget.metric_definition.display_name, "Annual Budget"
        )

    def test_dimension_value_validation(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "Not all dimension instances belong to the same class"
        ):
            MetricInterface(
                key=law_enforcement.calls_for_service.key,
                value=100000,
                aggregated_dimensions=[
                    MetricAggregatedDimensionData(
                        dimension_to_value={
                            CallType.EMERGENCY: 50000,
                            GenderRestricted.FEMALE: 100,
                        },
                    )
                ],
            )

    def test_context_validation(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "The required context ContextKey.ALL_CALLS_OR_CALLS_RESPONDED is missing",
        ):
            MetricInterface(
                key=law_enforcement.calls_for_service.key,
                value=100000,
                contexts=[],
                aggregated_dimensions=[],
                enforce_validation=True,
            )

        with self.assertRaisesRegex(
            ValueError,
            r"The context ContextKey.ALL_CALLS_OR_CALLS_RESPONDED is reported as a <enum 'CallsRespondedOptions'> but typed as a \(<class 'str'>,\).",
        ):
            MetricInterface(
                key=law_enforcement.calls_for_service.key,
                value=100000,
                contexts=[
                    MetricContextData(
                        key=ContextKey.ALL_CALLS_OR_CALLS_RESPONDED,
                        value=CallsRespondedOptions.ALL_CALLS,
                    )
                ],
                aggregated_dimensions=[],
                enforce_validation=True,
            )

    def test_aggregated_dimensions_validation(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "The following required dimensions are missing: {'metric/law_enforcement/calls_for_service/type'}",
        ):
            MetricInterface(
                key=law_enforcement.calls_for_service.key,
                value=100000,
                contexts=[
                    MetricContextData(
                        key=ContextKey.ALL_CALLS_OR_CALLS_RESPONDED,
                        value=CallsRespondedOptions.ALL_CALLS.value,
                    )
                ],
                aggregated_dimensions=[],
                enforce_validation=True,
            )

    def test_is_disaggregation_enabled(self) -> None:
        is_disabled = MetricAggregatedDimensionData(
            dimension_to_enabled_status={d: False for d in GenderRestricted},
            dimension_to_value={d: None for d in GenderRestricted},
        )

        json = is_disabled.to_json(
            dimension_definition=AggregatedDimension(
                dimension=GenderRestricted, required=True
            ),
            entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE,
        )

        self.assertEqual(json["enabled"], False)

        is_enabled = MetricAggregatedDimensionData(
            dimension_to_value={d: None for d in GenderRestricted},
            dimension_to_enabled_status={
                d: d is not GenderRestricted.FEMALE for d in GenderRestricted
            },
        )

        json = is_enabled.to_json(
            dimension_definition=AggregatedDimension(
                dimension=GenderRestricted, required=True
            ),
            entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE,
        )

        self.assertEqual(json["enabled"], True)

    def test_budget_report_metric_json(self) -> None:
        reported_metric = self.test_schema_objects.get_reported_budget_metric()
        self.assertEqual(
            reported_metric.to_json(
                entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE
            ),
            {
                "key": reported_metric.key,
                "system": "Law Enforcement",
                "display_name": "Annual Budget",
                "reporting_note": None,
                "description": "Measures the total annual budget (in dollars) of your agency.",
                "definitions": [],
                "category": "CAPACITY AND COST",
                "value": 100000,
                "unit": "USD",
                "label": "Annual Budget",
                "enabled": True,
                "frequency": "ANNUAL",
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
                        "display_name": "Please provide additional context.",
                        "reporting_note": None,
                        "required": False,
                        "type": "TEXT",
                        "value": None,
                        "multiple_choice_options": [],
                    },
                ],
                "disaggregations": [],
            },
        )

    def test_reported_calls_for_service_metric_json(self) -> None:
        reported_metric = (
            self.test_schema_objects.get_reported_calls_for_service_metric()
        )
        metric_definition = law_enforcement_metric_definitions.calls_for_service
        self.assertEqual(
            reported_metric.to_json(
                entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE
            ),
            {
                "key": reported_metric.key,
                "system": "Law Enforcement",
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
                "enabled": True,
                "frequency": "MONTHLY",
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
                        "display_name": "Please provide additional context.",
                        "reporting_note": None,
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
                        "enabled": True,
                        "dimensions": [
                            {
                                "key": "Emergency",
                                "label": "Emergency",
                                "value": 20,
                                "enabled": True,
                            },
                            {
                                "key": "Non-emergency",
                                "label": "Non-emergency",
                                "value": 60,
                                "enabled": True,
                            },
                            {
                                "key": "Unknown",
                                "label": "Unknown",
                                "value": 20,
                                "enabled": True,
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
            reported_metric.to_json(
                entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE
            ),
            {
                "key": reported_metric.key,
                "system": "Law Enforcement",
                "display_name": metric_definition.display_name,
                "reporting_note": metric_definition.reporting_note,
                "description": metric_definition.description,
                "enabled": False,
                "frequency": "ANNUAL",
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
                        "display_name": "Please provide additional context.",
                        "reporting_note": None,
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
            reported_metric.to_json(
                entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE
            ),
            {
                "key": reported_metric.key,
                "system": "Law Enforcement",
                "display_name": metric_definition.display_name,
                "reporting_note": metric_definition.reporting_note,
                "description": metric_definition.description,
                "definitions": [],
                "category": metric_definition.category.value,
                "value": reported_metric.value,
                "unit": metric_definition.metric_type.unit,
                "label": "Total Arrests",
                "enabled": True,
                "frequency": "MONTHLY",
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
                        "display_name": "Please provide additional context.",
                        "reporting_note": None,
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
                        "enabled": True,
                        "dimensions": [
                            {
                                "key": "Drug",
                                "label": "Drug",
                                "value": 60,
                                "enabled": True,
                            },
                            {
                                "key": "Person",
                                "label": "Person",
                                "value": 10,
                                "enabled": True,
                            },
                            {
                                "key": "Property",
                                "label": "Property",
                                "value": 40,
                                "enabled": True,
                            },
                            {
                                "key": "Unknown",
                                "label": "Unknown",
                                "value": 10,
                                "enabled": True,
                            },
                        ],
                    }
                ],
            },
        )

    def test_aggregated_dimension_from_json(self) -> None:
        # When two dimension enabled status changes, the other dimension status' are None
        request_json: Dict[str, Any] = {
            "key": "metric/law_enforcement/reported_crime/type",
            "dimensions": [
                {
                    "key": "Drug",
                    "enabled": True,
                },
                {
                    "key": "Person",
                    "enabled": True,
                },
            ],
        }

        dimension_data = MetricAggregatedDimensionData.from_json(
            json=request_json,
            entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
        )

        self.assertEqual(
            dimension_data.dimension_to_enabled_status,
            {
                d: True if d is OffenseType.DRUG or d is OffenseType.PERSON else None
                for d in OffenseType
            },
        )

        # When one dimension is disabled, the other dimension status' are not effected.
        request_json = {
            "key": "metric/law_enforcement/reported_crime/type",
            "dimensions": [
                {
                    "key": "Person",
                    "enabled": False,
                },
            ],
        }

        dimension_data = MetricAggregatedDimensionData.from_json(
            json=request_json,
            entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
        )

        self.assertEqual(
            dimension_data.dimension_to_enabled_status,
            {d: False if d is OffenseType.PERSON else None for d in OffenseType},
        )

        # When disaggregation is disabled all dimensions are turned off
        request_json = {
            "key": "metric/law_enforcement/reported_crime/type",
            "enabled": False,
        }

        dimension_data = MetricAggregatedDimensionData.from_json(
            json=request_json,
            entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
        )
        self.assertEqual(
            dimension_data.dimension_to_enabled_status,
            {d: False for d in OffenseType},
        )

        # When disaggregation is enabled all dimensions are turned on
        request_json = {
            "key": "metric/law_enforcement/reported_crime/type",
            "enabled": True,
        }

        dimension_data = MetricAggregatedDimensionData.from_json(
            json=request_json,
            entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
        )

        self.assertEqual(
            dimension_data.dimension_to_enabled_status,
            {d: True for d in OffenseType},
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
            MetricInterface(
                key=metric_definition.key,
                value=100,
                contexts=[
                    MetricContextData(
                        key=metric_definition.contexts[0].key,
                        value="definition of arrest",
                    )
                ],
                aggregated_dimensions=[
                    MetricAggregatedDimensionData(
                        dimension_to_value={
                            OffenseType.DRUG: 50,
                            OffenseType.PERSON: 50,
                            OffenseType.PROPERTY: 0,
                            OffenseType.UNKNOWN: 0,
                        }
                    )
                ],
            ),
            MetricInterface.from_json(
                json=response_json,
                entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE,
            ),
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
            MetricInterface(
                key=metric_definition.key,
                value=100,
                contexts=[
                    MetricContextData(
                        key=metric_definition.contexts[0].key,
                        value="additional context",
                    )
                ],
                aggregated_dimensions=[
                    MetricAggregatedDimensionData(
                        dimension_to_value={
                            LawEnforcementStaffType.LAW_ENFORCEMENT_OFFICERS: 100,
                            LawEnforcementStaffType.CIVILIAN_STAFF: 50,
                            LawEnforcementStaffType.UNKNOWN: 0,
                        },
                    )
                ],
                # TODO(#13556) Backend validation needs to match new frontend validation
                # enforce_validation=True
                enforce_validation=False,
            ),
            MetricInterface.from_json(
                json=response_json,
                entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE,
            ),
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
            MetricInterface(
                key=metric_definition.key,
                value=100,
                contexts=[],
                aggregated_dimensions=[],
                # TODO(#13556) Backend validation needs to match new frontend validation
                # enforce_validation=True
                enforce_validation=False,
            ),
            MetricInterface.from_json(
                json=response_json,
                entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE,
            ),
        )

    def test_to_json_disabled_disaggregation(self) -> None:
        metric_definition = law_enforcement.annual_budget
        metric_json = {
            "key": metric_definition.key,
            "enabled": False,
        }
        self.assertEqual(
            MetricInterface.from_json(
                json=metric_json, entry_point=DatapointGetRequestEntryPoint.METRICS_TAB
            ),
            MetricInterface(
                key=metric_definition.key,
                contexts=[],
                value=None,
                aggregated_dimensions=[],
                is_metric_enabled=False,
                enforce_validation=False,
            ),
        )

    def test_to_json_disabled_metric(self) -> None:
        metric_definition = law_enforcement.calls_for_service
        metric_interface = self.test_schema_objects.get_agency_metric_interface(
            is_metric_enabled=False
        )
        self.assertEqual(
            metric_interface.to_json(
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB
            ),
            {
                "key": metric_definition.key,
                "enabled": False,
                "category": metric_definition.category.value,
                "frequency": "MONTHLY",
                "contexts": [],
                "definitions": [
                    {
                        "definition": "One case that represents a request for police "
                        "service generated by the community and "
                        "received through an emergency or "
                        "non-emergency method (911, 311, 988, online "
                        "report). Count all calls for service, "
                        "regardless of whether an underlying incident "
                        "report was filed.",
                        "term": "Calls for service",
                    }
                ],
                "display_name": metric_definition.display_name,
                "description": metric_definition.description,
                "label": "Calls for Service",
                "reporting_note": metric_definition.reporting_note,
                "system": "Law Enforcement",
                "unit": "CALLS",
                "value": None,
                "disaggregations": [
                    {
                        "key": CallType.dimension_identifier(),
                        "enabled": False,
                        "required": True,
                        "helper_text": None,
                        "should_sum_to_total": False,
                        "display_name": "Call Types",
                        "dimensions": [
                            {
                                "enabled": False,
                                "label": CallType.EMERGENCY.value,
                                "key": CallType.EMERGENCY.value,
                            },
                            {
                                "enabled": False,
                                "label": CallType.NON_EMERGENCY.value,
                                "key": CallType.NON_EMERGENCY.value,
                            },
                            {
                                "enabled": False,
                                "label": CallType.UNKNOWN.value,
                                "key": CallType.UNKNOWN.value,
                            },
                        ],
                    }
                ],
            },
        )

    def test_to_json_disabled_dimensions(self) -> None:
        metric_definition = law_enforcement.calls_for_service
        metric_interface = self.test_schema_objects.get_agency_metric_interface(
            is_metric_enabled=True, include_disaggregation=True
        )
        self.assertEqual(
            metric_interface.to_json(
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB
            ),
            {
                "key": metric_definition.key,
                "enabled": True,
                "category": metric_definition.category.value,
                "frequency": "MONTHLY",
                "contexts": [],
                "definitions": [
                    {
                        "definition": "One case that represents a request for police "
                        "service generated by the community and "
                        "received through an emergency or "
                        "non-emergency method (911, 311, 988, online "
                        "report). Count all calls for service, "
                        "regardless of whether an underlying incident "
                        "report was filed.",
                        "term": "Calls for service",
                    }
                ],
                "display_name": metric_definition.display_name,
                "description": metric_definition.description,
                "label": "Calls for Service",
                "reporting_note": metric_definition.reporting_note,
                "system": "Law Enforcement",
                "unit": "CALLS",
                "value": None,
                "disaggregations": [
                    {
                        "key": CallType.dimension_identifier(),
                        "enabled": False,
                        "required": True,
                        "helper_text": None,
                        "should_sum_to_total": False,
                        "display_name": "Call Types",
                        "dimensions": [
                            {
                                "enabled": False,
                                "label": CallType.EMERGENCY.value,
                                "key": CallType.EMERGENCY.value,
                            },
                            {
                                "enabled": False,
                                "label": CallType.NON_EMERGENCY.value,
                                "key": CallType.NON_EMERGENCY.value,
                            },
                            {
                                "enabled": False,
                                "label": CallType.UNKNOWN.value,
                                "key": CallType.UNKNOWN.value,
                            },
                        ],
                    }
                ],
            },
        )

    def test_to_json_prefilled_contexts(self) -> None:
        metric_definition = law_enforcement.calls_for_service
        metric_interface = self.test_schema_objects.get_agency_metric_interface(
            is_metric_enabled=True, include_disaggregation=True, include_contexts=True
        )
        self.assertEqual(
            metric_interface.to_json(
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB
            ),
            {
                "key": metric_definition.key,
                "enabled": True,
                "category": metric_definition.category.value,
                "frequency": "MONTHLY",
                "contexts": [
                    {
                        "display_name": "Please provide additional context.",
                        "key": "ADDITIONAL_CONTEXT",
                        "multiple_choice_options": [],
                        "reporting_note": None,
                        "required": False,
                        "type": "TEXT",
                        "value": "this additional context provides contexts",
                    }
                ],
                "definitions": [
                    {
                        "definition": "One case that represents a request for police "
                        "service generated by the community and "
                        "received through an emergency or "
                        "non-emergency method (911, 311, 988, online "
                        "report). Count all calls for service, "
                        "regardless of whether an underlying incident "
                        "report was filed.",
                        "term": "Calls for service",
                    }
                ],
                "display_name": metric_definition.display_name,
                "description": metric_definition.description,
                "label": "Calls for Service",
                "reporting_note": metric_definition.reporting_note,
                "system": "Law Enforcement",
                "unit": "CALLS",
                "value": None,
                "disaggregations": [
                    {
                        "key": CallType.dimension_identifier(),
                        "enabled": False,
                        "required": True,
                        "helper_text": None,
                        "should_sum_to_total": False,
                        "display_name": "Call Types",
                        "dimensions": [
                            {
                                "enabled": False,
                                "label": CallType.EMERGENCY.value,
                                "key": CallType.EMERGENCY.value,
                            },
                            {
                                "enabled": False,
                                "label": CallType.NON_EMERGENCY.value,
                                "key": CallType.NON_EMERGENCY.value,
                            },
                            {
                                "enabled": False,
                                "label": CallType.UNKNOWN.value,
                                "key": CallType.UNKNOWN.value,
                            },
                        ],
                    }
                ],
            },
        )

    def test_to_json_partially_enabled_disaggregation(self) -> None:
        metric_definition = law_enforcement.calls_for_service
        metric_interface = self.test_schema_objects.get_agency_metric_interface(
            is_metric_enabled=True,
            include_disaggregation=True,
            use_partially_disabled_disaggregation=True,
        )
        self.assertEqual(
            metric_interface.to_json(
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB
            ),
            {
                "key": metric_definition.key,
                "enabled": True,
                "category": metric_definition.category.value,
                "frequency": "MONTHLY",
                "contexts": [],
                "definitions": [
                    {
                        "definition": "One case that represents a request for police "
                        "service generated by the community and "
                        "received through an emergency or "
                        "non-emergency method (911, 311, 988, online "
                        "report). Count all calls for service, "
                        "regardless of whether an underlying incident "
                        "report was filed.",
                        "term": "Calls for service",
                    }
                ],
                "display_name": metric_definition.display_name,
                "description": metric_definition.description,
                "label": "Calls for Service",
                "reporting_note": metric_definition.reporting_note,
                "system": "Law Enforcement",
                "unit": "CALLS",
                "value": None,
                "disaggregations": [
                    {
                        "key": CallType.dimension_identifier(),
                        "enabled": True,
                        "required": True,
                        "helper_text": None,
                        "should_sum_to_total": False,
                        "display_name": "Call Types",
                        "dimensions": [
                            {
                                "enabled": True,
                                "label": CallType.EMERGENCY.value,
                                "key": CallType.EMERGENCY.value,
                            },
                            {
                                "enabled": False,
                                "label": CallType.NON_EMERGENCY.value,
                                "key": CallType.NON_EMERGENCY.value,
                            },
                            {
                                "enabled": False,
                                "label": CallType.UNKNOWN.value,
                                "key": CallType.UNKNOWN.value,
                            },
                        ],
                    }
                ],
            },
        )

    def test_annual_budget_json_to_agency_metric(self) -> None:
        metric_definition = law_enforcement.annual_budget
        metric_json = {
            "key": metric_definition.key,
            "enabled": False,
        }
        self.assertEqual(
            MetricInterface.from_json(
                json=metric_json, entry_point=DatapointGetRequestEntryPoint.METRICS_TAB
            ),
            MetricInterface(
                key=metric_definition.key,
                contexts=[],
                value=None,
                aggregated_dimensions=[],
                is_metric_enabled=False,
                enforce_validation=False,
            ),
        )

    def test_civilian_complaints_json_to_agency_metric(self) -> None:
        metric_definition = law_enforcement.calls_for_service
        metric_json = {
            "key": metric_definition.key,
            "disaggregations": [
                {
                    "key": CallType.dimension_identifier(),
                    "dimensions": [
                        {"key": CallType.UNKNOWN.value, "enabled": False},
                        {"key": CallType.EMERGENCY.value, "enabled": False},
                    ],
                }
            ],
        }
        self.assertEqual(
            MetricInterface.from_json(
                json=metric_json, entry_point=DatapointGetRequestEntryPoint.METRICS_TAB
            ),
            MetricInterface(
                key=metric_definition.key,
                contexts=[],
                value=None,
                aggregated_dimensions=[
                    MetricAggregatedDimensionData(
                        dimension_to_enabled_status={
                            CallType.UNKNOWN: False,
                            CallType.EMERGENCY: False,
                            CallType.NON_EMERGENCY: None,
                        },
                        dimension_to_value=None,
                    )
                ],
                is_metric_enabled=True,
                enforce_validation=False,
            ),
        )

    def test_reported_crime_json_to_agency_metric(self) -> None:
        metric_definition = law_enforcement.reported_crime
        metric_json = {
            "key": metric_definition.key,
            "contexts": [
                {"key": ContextKey.ADDITIONAL_CONTEXT.value, "value": "blah blah"}
            ],
        }
        self.assertEqual(
            MetricInterface.from_json(
                json=metric_json, entry_point=DatapointGetRequestEntryPoint.METRICS_TAB
            ),
            MetricInterface(
                key=metric_definition.key,
                contexts=[
                    MetricContextData(
                        key=ContextKey.ADDITIONAL_CONTEXT, value="blah blah"
                    )
                ],
                value=None,
                aggregated_dimensions=[],
                is_metric_enabled=True,
                enforce_validation=False,
            ),
        )

    def test_total_arrests_json_to_agency_metric(self) -> None:
        metric_definition = law_enforcement.total_arrests
        metric_json = {
            "key": metric_definition.key,
            "disaggregations": [
                {"key": OffenseType.dimension_identifier(), "enabled": False},
            ],
        }
        self.assertEqual(
            MetricInterface.from_json(
                json=metric_json, entry_point=DatapointGetRequestEntryPoint.METRICS_TAB
            ),
            MetricInterface(
                key=metric_definition.key,
                value=None,
                aggregated_dimensions=[
                    MetricAggregatedDimensionData(
                        dimension_to_enabled_status={d: False for d in OffenseType},
                        dimension_to_value=None,
                    )
                ],
                is_metric_enabled=True,
                enforce_validation=False,
            ),
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
            MetricContextData.from_json(json=response_json),
            MetricContextData(
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
            MetricContextData.from_json(json=cleared_input),
            MetricContextData(key=ContextKey.ALL_CALLS_OR_CALLS_RESPONDED, value=None),
        )
