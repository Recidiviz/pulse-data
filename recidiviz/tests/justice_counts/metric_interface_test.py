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

from collections import defaultdict
from typing import Any, DefaultDict, Dict, List
from unittest import TestCase

from recidiviz.common.constants.justice_counts import ContextKey
from recidiviz.justice_counts.dimensions.law_enforcement import CallType
from recidiviz.justice_counts.dimensions.offense import OffenseType
from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.dimensions.prisons import ReleaseType
from recidiviz.justice_counts.includes_excludes.law_enforcement import (
    CallsForServiceEmergencyCallsIncludesExcludes,
    CallsForServiceIncludesExcludes,
    CallsForServiceNonEmergencyCallsIncludesExcludes,
    LawEnforcementArrestsIncludesExcludes,
    LawEnforcementFundingIncludesExcludes,
    LawEnforcementReportedCrimeIncludesExcludes,
)
from recidiviz.justice_counts.includes_excludes.offense import (
    DrugOffenseIncludesExcludes,
    PersonOffenseIncludesExcludes,
    PropertyOffenseIncludesExcludes,
    PublicOrderOffenseIncludesExcludes,
)
from recidiviz.justice_counts.includes_excludes.prisons import (
    PrisonGrievancesIncludesExcludes,
    PrisonReleasesCommunitySupervisionIncludesExcludes,
    PrisonReleasesDeathIncludesExcludes,
    PrisonReleasesNoControlIncludesExcludes,
    PrisonReleasesToParoleIncludesExcludes,
    PrisonReleasesToProbationIncludesExcludes,
)
from recidiviz.justice_counts.metrics import law_enforcement, prisons
from recidiviz.justice_counts.metrics.custom_reporting_frequency import (
    CustomReportingFrequency,
)
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    IncludesExcludesSetting,
)
from recidiviz.justice_counts.metrics.metric_interface import (
    DatapointGetRequestEntryPoint,
    MetricAggregatedDimensionData,
    MetricContextData,
    MetricInterface,
)
from recidiviz.justice_counts.metrics.metric_registry import METRICS_BY_SYSTEM
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import (
    ReportingFrequency,
)
from recidiviz.tests.justice_counts.utils import JusticeCountsSchemaTestObjects
from recidiviz.utils.types import assert_type


class TestMetricInterface(TestCase):
    """Implements tests for the Justice Counts MetricInterface class."""

    def setUp(self) -> None:
        self.test_schema_objects = JusticeCountsSchemaTestObjects()
        self.reported_budget = self.test_schema_objects.funding_metric
        self.reported_calls_for_service = (
            self.test_schema_objects.reported_calls_for_service_metric
        )
        self.maxDiff = None

    def test_init(self) -> None:
        self.assertEqual(self.reported_budget.metric_definition.display_name, "Funding")

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

    def test_funding_metric_json(self) -> None:
        reported_metric = self.test_schema_objects.get_funding_metric()
        self.assertEqual(
            reported_metric.to_json(
                entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE
            ),
            {
                "key": reported_metric.key,
                "system": {
                    "key": "LAW_ENFORCEMENT",
                    "display_name": "Law Enforcement",
                },
                "display_name": "Funding",
                "reporting_note": None,
                "description": "The amount of funding for agency law enforcement activities.",
                "definitions": [],
                "disaggregated_by_supervision_subsystems": None,
                "category": "Capacity and Cost",
                "value": 100000,
                "unit": "USD",
                "label": "Funding",
                "enabled": True,
                "frequency": "ANNUAL",
                "custom_frequency": None,
                "starting_month": None,
                "filenames": ["funding", "funding_by_type"],
                "settings": [],
                "contexts": [
                    {
                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                        "display_name": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                        "reporting_note": None,
                        "required": False,
                        "type": "TEXT",
                        "value": "our metrics are different because xyz",
                        "multiple_choice_options": [],
                    },
                ],
                "disaggregations": [],
                "datapoints": None,
            },
        )

    def test_reported_calls_for_service_metric_json(self) -> None:
        reported_metric = (
            self.test_schema_objects.get_reported_calls_for_service_metric()
        )
        metric_definition = law_enforcement.calls_for_service
        self.assertEqual(
            reported_metric.to_json(
                entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE
            ),
            {
                "key": reported_metric.key,
                "system": {
                    "key": "LAW_ENFORCEMENT",
                    "display_name": "Law Enforcement",
                },
                "display_name": metric_definition.display_name,
                "reporting_note": metric_definition.reporting_note,
                "datapoints": None,
                "description": metric_definition.description,
                "definitions": [
                    d.to_json() for d in metric_definition.definitions or []
                ],
                "disaggregated_by_supervision_subsystems": None,
                "category": metric_definition.category.human_readable_string,
                "value": reported_metric.value,
                "unit": metric_definition.metric_type.unit,
                "label": "Calls for Service",
                "enabled": True,
                "frequency": "MONTHLY",
                "custom_frequency": None,
                "starting_month": None,
                "filenames": ["calls_for_service", "calls_for_service_by_type"],
                "settings": [],
                "contexts": [
                    {
                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                        "display_name": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                        "reporting_note": None,
                        "required": False,
                        "type": "TEXT",
                        "value": "our metrics are different because xyz",
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
                                "datapoints": None,
                                "key": "Emergency Calls",
                                "label": "Emergency Calls",
                                "value": 20,
                                "enabled": True,
                                "description": "The number of calls for police assistance received by the agency that require immediate response.",
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                            },
                            {
                                "datapoints": None,
                                "key": "Non-emergency Calls",
                                "label": "Non-emergency Calls",
                                "value": 60,
                                "enabled": True,
                                "description": "The number of calls for police assistance received by the agency that do not require immediate response.",
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                            },
                            {
                                "datapoints": None,
                                "key": "Unknown Calls",
                                "label": "Unknown Calls",
                                "value": 20,
                                "enabled": True,
                                "description": "The number of calls for police assistance received by the agency of a type that is not known.",
                                "contexts": [],
                            },
                            {
                                "datapoints": None,
                                "key": "Other Calls",
                                "label": "Other Calls",
                                "value": None,
                                "enabled": True,
                                "description": "The number of calls for police assistance received by the agency that are not emergency or non-emergency calls.",
                                "contexts": [
                                    {
                                        "key": "ADDITIONAL_CONTEXT",
                                        "value": None,
                                        "label": "Please describe what data is being included in this breakdown.",
                                    }
                                ],
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
        metric_definition = law_enforcement.civilian_complaints_sustained
        self.assertEqual(
            reported_metric.to_json(
                entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE
            ),
            {
                "key": reported_metric.key,
                "system": {
                    "key": "LAW_ENFORCEMENT",
                    "display_name": "Law Enforcement",
                },
                "display_name": metric_definition.display_name,
                "reporting_note": metric_definition.reporting_note,
                "description": metric_definition.description,
                "enabled": False,
                "frequency": "ANNUAL",
                "custom_frequency": None,
                "starting_month": None,
                "filenames": [
                    "civilian_complaints",
                    "civilian_complaints_by_type",
                ],
                "definitions": [],
                "disaggregated_by_supervision_subsystems": None,
                "category": metric_definition.category.human_readable_string,
                "settings": [],
                "value": reported_metric.value,
                "unit": metric_definition.metric_type.unit,
                "label": "Civilian Complaints Sustained",
                "contexts": [],
                "disaggregations": [],
                "datapoints": None,
            },
        )

    def test_total_arrests_metric_json(self) -> None:
        reported_metric = self.test_schema_objects.get_arrests_metric()
        metric_definition = law_enforcement.arrests
        self.assertEqual(
            reported_metric.to_json(
                entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE
            ),
            {
                "key": reported_metric.key,
                "system": {
                    "key": "LAW_ENFORCEMENT",
                    "display_name": "Law Enforcement",
                },
                "display_name": metric_definition.display_name,
                "reporting_note": metric_definition.reporting_note,
                "description": metric_definition.description,
                "definitions": [],
                "disaggregated_by_supervision_subsystems": None,
                "datapoints": None,
                "category": metric_definition.category.human_readable_string,
                "value": reported_metric.value,
                "unit": metric_definition.metric_type.unit,
                "label": "Arrests",
                "enabled": True,
                "frequency": "MONTHLY",
                "custom_frequency": None,
                "starting_month": None,
                "filenames": [
                    "arrests",
                    "arrests_by_type",
                    "arrests_by_race",
                    "arrests_by_biological_sex",
                ],
                "settings": [],
                "contexts": [
                    {
                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                        "display_name": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                        "reporting_note": None,
                        "required": False,
                        "type": "TEXT",
                        "value": "our metrics are different because xyz",
                        "multiple_choice_options": [],
                    },
                ],
                "disaggregations": [
                    {
                        "key": "metric/offense/type",
                        "display_name": "Offense Types",
                        "required": True,
                        "should_sum_to_total": False,
                        "helper_text": None,
                        "enabled": True,
                        "dimensions": [
                            {
                                "datapoints": None,
                                "key": "Drug Offenses",
                                "label": "Drug Offenses",
                                "value": 60,
                                "enabled": True,
                                "description": "The number of arrests, citations, or summonses made by the agency in which the most serious offense was a drug offense.",
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                            },
                            {
                                "datapoints": None,
                                "key": "Person Offenses",
                                "label": "Person Offenses",
                                "value": 10,
                                "enabled": True,
                                "description": "The number of arrests, citations, or summonses made by the agency in which the most serious offense was a crime against a person.",
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                            },
                            {
                                "datapoints": None,
                                "key": "Property Offenses",
                                "label": "Property Offenses",
                                "value": 40,
                                "enabled": True,
                                "description": "The number of arrests, citations, or summonses made by the agency in which the most serious offense was a property offense.",
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                            },
                            {
                                "datapoints": None,
                                "key": "Public Order Offenses",
                                "label": "Public Order Offenses",
                                "value": 0,
                                "enabled": True,
                                "description": "The number of arrests, citations, or summonses made by the agency in which the most serious offense was a public order offense.",
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                            },
                            {
                                "datapoints": None,
                                "key": "Unknown Offenses",
                                "label": "Unknown Offenses",
                                "value": 10,
                                "enabled": True,
                                "description": "The number of arrests, citations, or summonses made by the agency in which the most serious offense is not known.",
                                "contexts": [],
                            },
                            {
                                "datapoints": None,
                                "key": "Other Offenses",
                                "label": "Other Offenses",
                                "value": 0,
                                "enabled": True,
                                "description": "The number of arrests, citations, or summonses made by the agency in which the most serious offense was another type of crime that was not a person, property, drug, or public order offense.",
                                "contexts": [
                                    {
                                        "key": "ADDITIONAL_CONTEXT",
                                        "value": None,
                                        "label": "Please describe what data is being included in this breakdown.",
                                    }
                                ],
                            },
                        ],
                    }
                ],
            },
        )

    def test_aggregated_dimension_from_json(self) -> None:
        # When two dimension enabled status changes, the other dimension status' are None
        request_json: Dict[str, Any] = {
            "key": "metric/offense/type",
            "dimensions": [
                {
                    "key": "Drug Offenses",
                    "enabled": True,
                },
                {
                    "key": "Person Offenses",
                    "enabled": True,
                },
            ],
        }

        dimension_data = MetricAggregatedDimensionData.from_json(
            json=request_json,
            entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
            disaggregation_definition=assert_type(
                law_enforcement.reported_crime.aggregated_dimensions, list
            )[0],
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
            "key": "metric/offense/type",
            "dimensions": [
                {
                    "key": "Person Offenses",
                    "enabled": False,
                },
            ],
        }

        dimension_data = MetricAggregatedDimensionData.from_json(
            json=request_json,
            entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
            disaggregation_definition=assert_type(
                law_enforcement.reported_crime.aggregated_dimensions, list
            )[0],
        )

        self.assertEqual(
            dimension_data.dimension_to_enabled_status,
            {d: False if d is OffenseType.PERSON else None for d in OffenseType},
        )

        # When disaggregation is disabled all dimensions are turned off
        request_json = {
            "key": "metric/offense/type",
            "enabled": False,
        }

        dimension_data = MetricAggregatedDimensionData.from_json(
            json=request_json,
            entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
            disaggregation_definition=assert_type(
                law_enforcement.reported_crime.aggregated_dimensions, list
            )[0],
        )
        self.assertEqual(
            dimension_data.dimension_to_enabled_status,
            {d: False for d in OffenseType},
        )

        # When disaggregation is enabled all dimensions are turned on
        request_json = {
            "key": "metric/offense/type",
            "enabled": True,
        }

        dimension_data = MetricAggregatedDimensionData.from_json(
            json=request_json,
            entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
            disaggregation_definition=assert_type(
                law_enforcement.reported_crime.aggregated_dimensions, list
            )[0],
        )

        self.assertEqual(
            dimension_data.dimension_to_enabled_status,
            {d: True for d in OffenseType},
        )

    def test_arrest_metric_json_to_report_metric(self) -> None:
        metric_definition = law_enforcement.arrests
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
                        {"key": OffenseType.PUBLIC_ORDER.value, "value": 0},
                        {"key": OffenseType.UNKNOWN.value, "value": 0},
                        {"key": OffenseType.OTHER.value, "value": 0},
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
                            OffenseType.PUBLIC_ORDER: 0,
                            OffenseType.UNKNOWN: 0,
                            OffenseType.OTHER: 0,
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
        metric_definition = law_enforcement.staff
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
            ),
            MetricInterface.from_json(
                json=response_json,
                entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE,
            ),
        )

    def test_complaints_sustained_metric_json_to_report_metric(self) -> None:
        metric_definition = law_enforcement.civilian_complaints_sustained
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
            ),
            MetricInterface.from_json(
                json=response_json,
                entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE,
            ),
        )

    def test_to_json_disabled_disaggregation(self) -> None:
        metric_definition = law_enforcement.funding
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
                includes_excludes_member_to_setting={
                    member: None for member in LawEnforcementFundingIncludesExcludes
                },
            ),
        )

    def test_to_json_disabled_metric(self) -> None:
        metric_interface = self.test_schema_objects.get_agency_metric_interface(
            is_metric_enabled=False
        )
        self.assertEqual(
            metric_interface.to_json(
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB
            ),
            {
                "key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                "system": {"key": "LAW_ENFORCEMENT", "display_name": "Law Enforcement"},
                "display_name": "Calls for Service",
                "description": "The number of calls for police assistance received by the agency.",
                "datapoints": None,
                "disaggregated_by_supervision_subsystems": None,
                "reporting_note": None,
                "value": None,
                "settings": [
                    {
                        "key": "SERVICE_911",
                        "label": "Calls for service received by the agency’s dispatch service via 911",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "SERVICE_311",
                        "label": "Calls for service received by the agency’s dispatch service via 311 or equivalent non-emergency number",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "MUTUAL_AID",
                        "label": "Mutual aid calls for support received by the agency",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "OFFICER_INITIATED",
                        "label": "Officer-initiated calls for service (e.g., traffic stops, foot patrol)",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "OTHER_JURISDICTION",
                        "label": "Calls for service received by another jurisdiction and routed to the agency for response",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "AUTOMATED",
                        "label": "Automated calls for service (e.g., security system)",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "FIRE_SERVICE",
                        "label": "Calls for fire service",
                        "included": "No",
                        "default": "No",
                    },
                    {
                        "key": "EMS",
                        "label": "Calls for EMS service",
                        "included": "No",
                        "default": "No",
                    },
                    {
                        "key": "NON_POLICE_SERVICE",
                        "label": "Calls for other non-police service",
                        "included": "No",
                        "default": "No",
                    },
                ],
                "unit": "CALLS",
                "category": "Operations and Dynamics",
                "label": "Calls for Service",
                "enabled": False,
                "frequency": "MONTHLY",
                "custom_frequency": None,
                "starting_month": None,
                "filenames": ["calls_for_service", "calls_for_service_by_type"],
                "definitions": [],
                "contexts": [],
                "disaggregations": [
                    {
                        "key": "metric/law_enforcement/calls_for_service/type",
                        "helper_text": None,
                        "required": True,
                        "should_sum_to_total": False,
                        "display_name": "Call Types",
                        "dimensions": [
                            {
                                "key": "Emergency Calls",
                                "label": "Emergency Calls",
                                "enabled": False,
                                "datapoints": None,
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "settings": [
                                    {
                                        "key": "PRIORITY",
                                        "label": "Calls that require urgent or priority response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "CODE_2_RESPONSE",
                                        "label": "Calls that require code 2 or higher response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "IMMINENT_THREATS",
                                        "label": "Calls that relate to incidents with imminent threats to life or danger of serious injury",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "ONGOING_OFFENSES",
                                        "label": "Calls that relate to ongoing offenses that involve violence",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "SERIOUS_OFFENSE",
                                        "label": "Calls that relate to a serious offense that has just occurred and reason exists to believe the person suspected of committing the offense is in the area",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "OFFICER_IN_TROUBLE",
                                        "label": "Calls for “officer in trouble” or request for emergency assistance from an officer",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "TRAFFIC",
                                        "label": "Calls that relate to incidents that represent significant hazards to the flow of traffic",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "IN_PROGRESS_INCIDENT",
                                        "label": "Calls that relate to in-progress incidents that could be classified as crimes",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                ],
                                "description": "The number of calls for police assistance received by the agency that require immediate response.",
                            },
                            {
                                "key": "Non-emergency Calls",
                                "label": "Non-emergency Calls",
                                "enabled": False,
                                "datapoints": None,
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "settings": [
                                    {
                                        "key": "ROUTINE_RESPONSE",
                                        "label": "Calls that require routine response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "CODE_1_RESPONSE",
                                        "label": "Calls that require code 1 response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "PATROL_REQUEST",
                                        "label": "Calls for patrol requests",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "ROUTINE_TRANSPORTATION",
                                        "label": "Calls for routine transportation",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "NON_EMERGENCY_SERVICE",
                                        "label": "Calls for non-emergency service",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "CIVILIAN_COMMUNITY_SERVICE",
                                        "label": "Calls routed to civilian community service officers for response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "STOLEN_PROPERTY",
                                        "label": "Calls to take a report of stolen property",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                ],
                                "description": "The number of calls for police assistance received by the agency that do not require immediate response.",
                            },
                            {
                                "key": "Unknown Calls",
                                "label": "Unknown Calls",
                                "enabled": False,
                                "datapoints": None,
                                "contexts": [],
                                "settings": [],
                                "description": "The number of calls for police assistance received by the agency of a type that is not known.",
                            },
                        ],
                        "enabled": False,
                    }
                ],
            },
        )

    def test_to_json_disabled_dimensions(self) -> None:
        metric_interface = self.test_schema_objects.get_agency_metric_interface(
            is_metric_enabled=True, include_disaggregation=True
        )
        self.assertEqual(
            metric_interface.to_json(
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB
            ),
            {
                "key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                "system": {"key": "LAW_ENFORCEMENT", "display_name": "Law Enforcement"},
                "display_name": "Calls for Service",
                "description": "The number of calls for police assistance received by the agency.",
                "datapoints": None,
                "disaggregated_by_supervision_subsystems": None,
                "reporting_note": None,
                "value": None,
                "settings": [
                    {
                        "key": "SERVICE_911",
                        "label": "Calls for service received by the agency’s dispatch service via 911",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "SERVICE_311",
                        "label": "Calls for service received by the agency’s dispatch service via 311 or equivalent non-emergency number",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "MUTUAL_AID",
                        "label": "Mutual aid calls for support received by the agency",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "OFFICER_INITIATED",
                        "label": "Officer-initiated calls for service (e.g., traffic stops, foot patrol)",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "OTHER_JURISDICTION",
                        "label": "Calls for service received by another jurisdiction and routed to the agency for response",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "AUTOMATED",
                        "label": "Automated calls for service (e.g., security system)",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "FIRE_SERVICE",
                        "label": "Calls for fire service",
                        "included": "No",
                        "default": "No",
                    },
                    {
                        "key": "EMS",
                        "label": "Calls for EMS service",
                        "included": "No",
                        "default": "No",
                    },
                    {
                        "key": "NON_POLICE_SERVICE",
                        "label": "Calls for other non-police service",
                        "included": "No",
                        "default": "No",
                    },
                ],
                "unit": "CALLS",
                "category": "Operations and Dynamics",
                "label": "Calls for Service",
                "enabled": True,
                "frequency": "MONTHLY",
                "custom_frequency": None,
                "starting_month": None,
                "filenames": ["calls_for_service", "calls_for_service_by_type"],
                "definitions": [],
                "contexts": [],
                "disaggregations": [
                    {
                        "key": "metric/law_enforcement/calls_for_service/type",
                        "helper_text": None,
                        "required": True,
                        "should_sum_to_total": False,
                        "display_name": "Call Types",
                        "dimensions": [
                            {
                                "key": "Emergency Calls",
                                "label": "Emergency Calls",
                                "enabled": False,
                                "datapoints": None,
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "settings": [
                                    {
                                        "key": "PRIORITY",
                                        "label": "Calls that require urgent or priority response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "CODE_2_RESPONSE",
                                        "label": "Calls that require code 2 or higher response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "IMMINENT_THREATS",
                                        "label": "Calls that relate to incidents with imminent threats to life or danger of serious injury",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "ONGOING_OFFENSES",
                                        "label": "Calls that relate to ongoing offenses that involve violence",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "SERIOUS_OFFENSE",
                                        "label": "Calls that relate to a serious offense that has just occurred and reason exists to believe the person suspected of committing the offense is in the area",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "OFFICER_IN_TROUBLE",
                                        "label": "Calls for “officer in trouble” or request for emergency assistance from an officer",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "TRAFFIC",
                                        "label": "Calls that relate to incidents that represent significant hazards to the flow of traffic",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "IN_PROGRESS_INCIDENT",
                                        "label": "Calls that relate to in-progress incidents that could be classified as crimes",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                ],
                                "description": "The number of calls for police assistance received by the agency that require immediate response.",
                            },
                            {
                                "key": "Non-emergency Calls",
                                "label": "Non-emergency Calls",
                                "enabled": False,
                                "datapoints": None,
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "settings": [
                                    {
                                        "key": "ROUTINE_RESPONSE",
                                        "label": "Calls that require routine response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "CODE_1_RESPONSE",
                                        "label": "Calls that require code 1 response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "PATROL_REQUEST",
                                        "label": "Calls for patrol requests",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "ROUTINE_TRANSPORTATION",
                                        "label": "Calls for routine transportation",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "NON_EMERGENCY_SERVICE",
                                        "label": "Calls for non-emergency service",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "CIVILIAN_COMMUNITY_SERVICE",
                                        "label": "Calls routed to civilian community service officers for response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "STOLEN_PROPERTY",
                                        "label": "Calls to take a report of stolen property",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                ],
                                "description": "The number of calls for police assistance received by the agency that do not require immediate response.",
                            },
                            {
                                "key": "Unknown Calls",
                                "label": "Unknown Calls",
                                "enabled": False,
                                "datapoints": None,
                                "contexts": [],
                                "settings": [],
                                "description": "The number of calls for police assistance received by the agency of a type that is not known.",
                            },
                        ],
                        "enabled": False,
                    }
                ],
            },
        )

    def test_to_json_prefilled_contexts(self) -> None:
        metric_interface = self.test_schema_objects.get_agency_metric_interface(
            is_metric_enabled=True, include_disaggregation=True
        )
        self.assertEqual(
            metric_interface.to_json(
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB
            ),
            {
                "key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                "system": {"key": "LAW_ENFORCEMENT", "display_name": "Law Enforcement"},
                "display_name": "Calls for Service",
                "description": "The number of calls for police assistance received by the agency.",
                "datapoints": None,
                "disaggregated_by_supervision_subsystems": None,
                "reporting_note": None,
                "value": None,
                "settings": [
                    {
                        "key": "SERVICE_911",
                        "label": "Calls for service received by the agency’s dispatch service via 911",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "SERVICE_311",
                        "label": "Calls for service received by the agency’s dispatch service via 311 or equivalent non-emergency number",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "MUTUAL_AID",
                        "label": "Mutual aid calls for support received by the agency",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "OFFICER_INITIATED",
                        "label": "Officer-initiated calls for service (e.g., traffic stops, foot patrol)",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "OTHER_JURISDICTION",
                        "label": "Calls for service received by another jurisdiction and routed to the agency for response",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "AUTOMATED",
                        "label": "Automated calls for service (e.g., security system)",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "FIRE_SERVICE",
                        "label": "Calls for fire service",
                        "included": "No",
                        "default": "No",
                    },
                    {
                        "key": "EMS",
                        "label": "Calls for EMS service",
                        "included": "No",
                        "default": "No",
                    },
                    {
                        "key": "NON_POLICE_SERVICE",
                        "label": "Calls for other non-police service",
                        "included": "No",
                        "default": "No",
                    },
                ],
                "unit": "CALLS",
                "category": "Operations and Dynamics",
                "label": "Calls for Service",
                "enabled": True,
                "frequency": "MONTHLY",
                "custom_frequency": None,
                "starting_month": None,
                "filenames": ["calls_for_service", "calls_for_service_by_type"],
                "definitions": [],
                "contexts": [],
                "disaggregations": [
                    {
                        "key": "metric/law_enforcement/calls_for_service/type",
                        "helper_text": None,
                        "required": True,
                        "should_sum_to_total": False,
                        "display_name": "Call Types",
                        "dimensions": [
                            {
                                "key": "Emergency Calls",
                                "label": "Emergency Calls",
                                "enabled": False,
                                "datapoints": None,
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "settings": [
                                    {
                                        "key": "PRIORITY",
                                        "label": "Calls that require urgent or priority response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "CODE_2_RESPONSE",
                                        "label": "Calls that require code 2 or higher response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "IMMINENT_THREATS",
                                        "label": "Calls that relate to incidents with imminent threats to life or danger of serious injury",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "ONGOING_OFFENSES",
                                        "label": "Calls that relate to ongoing offenses that involve violence",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "SERIOUS_OFFENSE",
                                        "label": "Calls that relate to a serious offense that has just occurred and reason exists to believe the person suspected of committing the offense is in the area",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "OFFICER_IN_TROUBLE",
                                        "label": "Calls for “officer in trouble” or request for emergency assistance from an officer",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "TRAFFIC",
                                        "label": "Calls that relate to incidents that represent significant hazards to the flow of traffic",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "IN_PROGRESS_INCIDENT",
                                        "label": "Calls that relate to in-progress incidents that could be classified as crimes",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                ],
                                "description": "The number of calls for police assistance received by the agency that require immediate response.",
                            },
                            {
                                "key": "Non-emergency Calls",
                                "label": "Non-emergency Calls",
                                "enabled": False,
                                "datapoints": None,
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "settings": [
                                    {
                                        "key": "ROUTINE_RESPONSE",
                                        "label": "Calls that require routine response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "CODE_1_RESPONSE",
                                        "label": "Calls that require code 1 response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "PATROL_REQUEST",
                                        "label": "Calls for patrol requests",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "ROUTINE_TRANSPORTATION",
                                        "label": "Calls for routine transportation",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "NON_EMERGENCY_SERVICE",
                                        "label": "Calls for non-emergency service",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "CIVILIAN_COMMUNITY_SERVICE",
                                        "label": "Calls routed to civilian community service officers for response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "STOLEN_PROPERTY",
                                        "label": "Calls to take a report of stolen property",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                ],
                                "description": "The number of calls for police assistance received by the agency that do not require immediate response.",
                            },
                            {
                                "key": "Unknown Calls",
                                "label": "Unknown Calls",
                                "enabled": False,
                                "datapoints": None,
                                "contexts": [],
                                "settings": [],
                                "description": "The number of calls for police assistance received by the agency of a type that is not known.",
                            },
                        ],
                        "enabled": False,
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
                "category": metric_definition.category.human_readable_string,
                "frequency": "MONTHLY",
                "custom_frequency": None,
                "starting_month": None,
                "filenames": ["calls_for_service", "calls_for_service_by_type"],
                "contexts": [],
                "settings": [
                    {
                        "default": "Yes",
                        "included": "Yes",
                        "key": "SERVICE_911",
                        "label": "Calls for service received by the agency’s dispatch "
                        "service via 911",
                    },
                    {
                        "default": "Yes",
                        "included": "Yes",
                        "key": "SERVICE_311",
                        "label": "Calls for service received by the agency’s dispatch "
                        "service via 311 or equivalent non-emergency number",
                    },
                    {
                        "default": "Yes",
                        "included": "Yes",
                        "key": "MUTUAL_AID",
                        "label": "Mutual aid calls for support received by the agency",
                    },
                    {
                        "default": "Yes",
                        "included": "Yes",
                        "key": "OFFICER_INITIATED",
                        "label": "Officer-initiated calls for service (e.g., traffic "
                        "stops, foot patrol)",
                    },
                    {
                        "default": "Yes",
                        "included": "Yes",
                        "key": "OTHER_JURISDICTION",
                        "label": "Calls for service received by another jurisdiction "
                        "and routed to the agency for response",
                    },
                    {
                        "default": "Yes",
                        "included": "Yes",
                        "key": "AUTOMATED",
                        "label": "Automated calls for service (e.g., security system)",
                    },
                    {
                        "default": "No",
                        "included": "No",
                        "key": "FIRE_SERVICE",
                        "label": "Calls for fire service",
                    },
                    {
                        "default": "No",
                        "included": "No",
                        "key": "EMS",
                        "label": "Calls for EMS service",
                    },
                    {
                        "default": "No",
                        "included": "No",
                        "key": "NON_POLICE_SERVICE",
                        "label": "Calls for other non-police service",
                    },
                ],
                "datapoints": None,
                "definitions": [],
                "disaggregated_by_supervision_subsystems": None,
                "display_name": metric_definition.display_name,
                "description": metric_definition.description,
                "label": "Calls for Service",
                "reporting_note": metric_definition.reporting_note,
                "system": {
                    "key": "LAW_ENFORCEMENT",
                    "display_name": "Law Enforcement",
                },
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
                                "datapoints": None,
                                "enabled": True,
                                "label": CallType.EMERGENCY.value,
                                "key": CallType.EMERGENCY.value,
                                "description": "The number of calls for police assistance received by the agency that require immediate response.",
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "settings": [
                                    {
                                        "key": "PRIORITY",
                                        "label": "Calls that require urgent or priority response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "CODE_2_RESPONSE",
                                        "label": "Calls that require code 2 or higher response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "IMMINENT_THREATS",
                                        "label": "Calls that relate to incidents with imminent threats to life or danger of serious injury",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "ONGOING_OFFENSES",
                                        "label": "Calls that relate to ongoing offenses that involve violence",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "SERIOUS_OFFENSE",
                                        "label": "Calls that relate to a serious offense that has just occurred and reason exists to believe the person suspected of committing the offense is in the area",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "OFFICER_IN_TROUBLE",
                                        "label": "Calls for “officer in trouble” or request for emergency assistance from an officer",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "TRAFFIC",
                                        "label": "Calls that relate to incidents that represent significant hazards to the flow of traffic",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "IN_PROGRESS_INCIDENT",
                                        "label": "Calls that relate to in-progress incidents that could be classified as crimes",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                ],
                            },
                            {
                                "datapoints": None,
                                "enabled": False,
                                "label": CallType.NON_EMERGENCY.value,
                                "key": CallType.NON_EMERGENCY.value,
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "settings": [
                                    {
                                        "key": "ROUTINE_RESPONSE",
                                        "label": "Calls that require routine response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "CODE_1_RESPONSE",
                                        "label": "Calls that require code 1 response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "PATROL_REQUEST",
                                        "label": "Calls for patrol requests",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "ROUTINE_TRANSPORTATION",
                                        "label": "Calls for routine transportation",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "NON_EMERGENCY_SERVICE",
                                        "label": "Calls for non-emergency service",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "CIVILIAN_COMMUNITY_SERVICE",
                                        "label": "Calls routed to civilian community service officers for response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "STOLEN_PROPERTY",
                                        "label": "Calls to take a report of stolen property",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                ],
                                "description": "The number of calls for police assistance received by the agency that do not require immediate response.",
                            },
                            {
                                "datapoints": None,
                                "enabled": False,
                                "label": CallType.UNKNOWN.value,
                                "key": CallType.UNKNOWN.value,
                                "contexts": [],
                                "settings": [],
                                "description": "The number of calls for police assistance received by the agency of a type that is not known.",
                            },
                        ],
                    }
                ],
            },
        )

    def test_to_json_with_datapoints(self) -> None:
        metric_interface = self.test_schema_objects.get_agency_metric_interface(
            is_metric_enabled=False
        )
        aggregate_datapoints_json: List[DatapointJson] = []
        aggregate_datapoints_json.extend(
            [
                {
                    "dimension_display_name": None,
                    "disaggregation_display_name": None,
                    "end_date": "Tue, 01 Nov 2022 00:00:00 GMT",
                    "frequency": "MONTHLY",
                    "id": 14887,
                    "is_published": True,
                    "metric_definition_key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                    "metric_display_name": "Calls for Service",
                    "old_value": None,
                    "report_id": 314,
                    "start_date": "Sat, 01 Oct 2022 00:00:00 GMT",
                    "value": 10,
                },
                {
                    "dimension_display_name": None,
                    "disaggregation_display_name": None,
                    "end_date": "Thu, 01 Dec 2022 00:00:00 GMT",
                    "frequency": "MONTHLY",
                    "id": 14922,
                    "is_published": True,
                    "metric_definition_key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                    "metric_display_name": "Calls for Service",
                    "old_value": None,
                    "report_id": 315,
                    "start_date": "Tue, 01 Nov 2022 00:00:00 GMT",
                    "value": 11,
                },
            ]
        )
        dimension_id_to_dimension_member_to_datapoints_json: DefaultDict[
            str, DefaultDict[str, List[DatapointJson]]
        ] = defaultdict(lambda: defaultdict(list))
        dimension_id_to_dimension_member_to_datapoints_json[
            CallType.dimension_identifier()
        ][CallType.EMERGENCY.name] = [
            {
                "dimension_display_name": "Emergency",
                "disaggregation_display_name": "Call Type",
                "end_date": "Tue, 01 Nov 2022 00:00:00 GMT",
                "frequency": "MONTHLY",
                "id": 14891,
                "is_published": True,
                "metric_definition_key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                "metric_display_name": "Calls for Service",
                "old_value": None,
                "report_id": 314,
                "start_date": "Sat, 01 Oct 2022 00:00:00 GMT",
                "value": 51890,
            }
        ]
        dimension_id_to_dimension_member_to_datapoints_json[
            CallType.dimension_identifier()
        ][CallType.NON_EMERGENCY.name] = [
            {
                "dimension_display_name": "Non-emergency",
                "disaggregation_display_name": "Call Type",
                "end_date": "Tue, 01 Nov 2022 00:00:00 GMT",
                "frequency": "MONTHLY",
                "id": 14892,
                "is_published": True,
                "metric_definition_key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                "metric_display_name": "Calls for Service",
                "old_value": None,
                "report_id": 314,
                "start_date": "Sat, 01 Oct 2022 00:00:00 GMT",
                "value": 66995,
            }
        ]
        dimension_id_to_dimension_member_to_datapoints_json[
            CallType.dimension_identifier()
        ][CallType.UNKNOWN.name] = [
            {
                "dimension_display_name": "Unknown",
                "disaggregation_display_name": "Call Type",
                "end_date": "Tue, 01 Nov 2022 00:00:00 GMT",
                "frequency": "MONTHLY",
                "id": 14893,
                "is_published": True,
                "metric_definition_key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                "metric_display_name": "Calls for Service",
                "old_value": None,
                "report_id": 314,
                "start_date": "Sat, 01 Oct 2022 00:00:00 GMT",
                "value": 24062,
            }
        ]
        self.assertEqual(
            metric_interface.to_json(
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
                aggregate_datapoints_json=aggregate_datapoints_json,
                dimension_id_to_dimension_member_to_datapoints_json=dimension_id_to_dimension_member_to_datapoints_json,
            ),
            {
                "key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                "system": {"key": "LAW_ENFORCEMENT", "display_name": "Law Enforcement"},
                "display_name": "Calls for Service",
                "description": "The number of calls for police assistance received by the agency.",
                "datapoints": [
                    {
                        "dimension_display_name": None,
                        "disaggregation_display_name": None,
                        "end_date": "Tue, 01 Nov 2022 00:00:00 GMT",
                        "frequency": "MONTHLY",
                        "id": 14887,
                        "is_published": True,
                        "metric_definition_key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                        "metric_display_name": "Calls for Service",
                        "old_value": None,
                        "report_id": 314,
                        "start_date": "Sat, 01 Oct 2022 00:00:00 GMT",
                        "value": 10,
                    },
                    {
                        "dimension_display_name": None,
                        "disaggregation_display_name": None,
                        "end_date": "Thu, 01 Dec 2022 00:00:00 GMT",
                        "frequency": "MONTHLY",
                        "id": 14922,
                        "is_published": True,
                        "metric_definition_key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                        "metric_display_name": "Calls for Service",
                        "old_value": None,
                        "report_id": 315,
                        "start_date": "Tue, 01 Nov 2022 00:00:00 GMT",
                        "value": 11,
                    },
                ],
                "disaggregated_by_supervision_subsystems": None,
                "reporting_note": None,
                "value": None,
                "settings": [
                    {
                        "key": "SERVICE_911",
                        "label": "Calls for service received by the agency’s dispatch service via 911",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "SERVICE_311",
                        "label": "Calls for service received by the agency’s dispatch service via 311 or equivalent non-emergency number",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "MUTUAL_AID",
                        "label": "Mutual aid calls for support received by the agency",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "OFFICER_INITIATED",
                        "label": "Officer-initiated calls for service (e.g., traffic stops, foot patrol)",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "OTHER_JURISDICTION",
                        "label": "Calls for service received by another jurisdiction and routed to the agency for response",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "AUTOMATED",
                        "label": "Automated calls for service (e.g., security system)",
                        "included": "Yes",
                        "default": "Yes",
                    },
                    {
                        "key": "FIRE_SERVICE",
                        "label": "Calls for fire service",
                        "included": "No",
                        "default": "No",
                    },
                    {
                        "key": "EMS",
                        "label": "Calls for EMS service",
                        "included": "No",
                        "default": "No",
                    },
                    {
                        "key": "NON_POLICE_SERVICE",
                        "label": "Calls for other non-police service",
                        "included": "No",
                        "default": "No",
                    },
                ],
                "unit": "CALLS",
                "category": "Operations and Dynamics",
                "label": "Calls for Service",
                "enabled": False,
                "frequency": "MONTHLY",
                "custom_frequency": None,
                "starting_month": None,
                "filenames": ["calls_for_service", "calls_for_service_by_type"],
                "definitions": [],
                "contexts": [],
                "disaggregations": [
                    {
                        "key": "metric/law_enforcement/calls_for_service/type",
                        "helper_text": None,
                        "required": True,
                        "should_sum_to_total": False,
                        "display_name": "Call Types",
                        "dimensions": [
                            {
                                "key": "Emergency Calls",
                                "label": "Emergency Calls",
                                "enabled": False,
                                "datapoints": [
                                    {
                                        "dimension_display_name": "Emergency",
                                        "disaggregation_display_name": "Call " "Type",
                                        "end_date": "Tue, 01 Nov "
                                        "2022 "
                                        "00:00:00 "
                                        "GMT",
                                        "frequency": "MONTHLY",
                                        "id": 14891,
                                        "is_published": True,
                                        "metric_definition_key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                                        "metric_display_name": "Calls "
                                        "for "
                                        "Service",
                                        "old_value": None,
                                        "report_id": 314,
                                        "start_date": "Sat, 01 "
                                        "Oct 2022 "
                                        "00:00:00 "
                                        "GMT",
                                        "value": 51890,
                                    }
                                ],
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "settings": [
                                    {
                                        "key": "PRIORITY",
                                        "label": "Calls that require urgent or priority response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "CODE_2_RESPONSE",
                                        "label": "Calls that require code 2 or higher response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "IMMINENT_THREATS",
                                        "label": "Calls that relate to incidents with imminent threats to life or danger of serious injury",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "ONGOING_OFFENSES",
                                        "label": "Calls that relate to ongoing offenses that involve violence",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "SERIOUS_OFFENSE",
                                        "label": "Calls that relate to a serious offense that has just occurred and reason exists to believe the person suspected of committing the offense is in the area",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "OFFICER_IN_TROUBLE",
                                        "label": "Calls for “officer in trouble” or request for emergency assistance from an officer",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "TRAFFIC",
                                        "label": "Calls that relate to incidents that represent significant hazards to the flow of traffic",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "IN_PROGRESS_INCIDENT",
                                        "label": "Calls that relate to in-progress incidents that could be classified as crimes",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                ],
                                "description": "The number of calls for police assistance received by the agency that require immediate response.",
                            },
                            {
                                "key": "Non-emergency Calls",
                                "label": "Non-emergency Calls",
                                "enabled": False,
                                "datapoints": [
                                    {
                                        "dimension_display_name": "Non-emergency",
                                        "disaggregation_display_name": "Call Type",
                                        "end_date": "Tue, 01 Nov 2022 00:00:00 GMT",
                                        "frequency": "MONTHLY",
                                        "id": 14892,
                                        "is_published": True,
                                        "metric_definition_key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                                        "metric_display_name": "Calls for Service",
                                        "old_value": None,
                                        "report_id": 314,
                                        "start_date": "Sat, 01 Oct 2022 00:00:00 GMT",
                                        "value": 66995,
                                    }
                                ],
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "settings": [
                                    {
                                        "key": "ROUTINE_RESPONSE",
                                        "label": "Calls that require routine response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "CODE_1_RESPONSE",
                                        "label": "Calls that require code 1 response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "PATROL_REQUEST",
                                        "label": "Calls for patrol requests",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "ROUTINE_TRANSPORTATION",
                                        "label": "Calls for routine transportation",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "NON_EMERGENCY_SERVICE",
                                        "label": "Calls for non-emergency service",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "CIVILIAN_COMMUNITY_SERVICE",
                                        "label": "Calls routed to civilian community service officers for response",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                    {
                                        "key": "STOLEN_PROPERTY",
                                        "label": "Calls to take a report of stolen property",
                                        "included": "Yes",
                                        "default": "Yes",
                                    },
                                ],
                                "description": "The number of calls for police assistance received by the agency that do not require immediate response.",
                            },
                            {
                                "key": "Unknown Calls",
                                "label": "Unknown Calls",
                                "enabled": False,
                                "datapoints": [
                                    {
                                        "dimension_display_name": "Unknown",
                                        "disaggregation_display_name": "Call Type",
                                        "end_date": "Tue, 01 Nov 2022 00:00:00 GMT",
                                        "frequency": "MONTHLY",
                                        "id": 14893,
                                        "is_published": True,
                                        "metric_definition_key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                                        "metric_display_name": "Calls for Service",
                                        "old_value": None,
                                        "report_id": 314,
                                        "start_date": "Sat, 01 Oct 2022 00:00:00 GMT",
                                        "value": 24062,
                                    }
                                ],
                                "contexts": [],
                                "settings": [],
                                "description": "The number of calls for police assistance received by the agency of a type that is not known.",
                            },
                        ],
                        "enabled": False,
                    }
                ],
            },
        )

    def test_funding_json_to_agency_metric(self) -> None:
        metric_definition = law_enforcement.funding
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
                includes_excludes_member_to_setting={
                    member: None for member in LawEnforcementFundingIncludesExcludes
                },
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
                includes_excludes_member_to_setting={
                    member: None for member in CallsForServiceIncludesExcludes
                },
                aggregated_dimensions=[
                    MetricAggregatedDimensionData(
                        dimension_to_enabled_status={
                            CallType.EMERGENCY: False,
                            CallType.NON_EMERGENCY: None,
                            CallType.OTHER: None,
                            CallType.UNKNOWN: False,
                        },
                        dimension_to_includes_excludes_member_to_setting={
                            CallType.EMERGENCY: {
                                member: None
                                for member in CallsForServiceEmergencyCallsIncludesExcludes
                            },
                            CallType.NON_EMERGENCY: {
                                member: None
                                for member in CallsForServiceNonEmergencyCallsIncludesExcludes
                            },
                            CallType.OTHER: {},
                            CallType.UNKNOWN: {},
                        },
                        dimension_to_value=None,
                    )
                ],
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
                includes_excludes_member_to_setting={
                    member: None
                    for member in LawEnforcementReportedCrimeIncludesExcludes
                },
            ),
        )

    def test_total_arrests_json_to_agency_metric(self) -> None:
        metric_definition = law_enforcement.arrests
        metric_json = {
            "key": metric_definition.key,
            "settings": [],
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
                includes_excludes_member_to_setting={
                    d: None for d in LawEnforcementArrestsIncludesExcludes
                },
                aggregated_dimensions=[
                    MetricAggregatedDimensionData(
                        dimension_to_enabled_status={d: False for d in OffenseType},
                        dimension_to_value=None,
                        dimension_to_includes_excludes_member_to_setting={
                            OffenseType.PERSON: {
                                d: None for d in PersonOffenseIncludesExcludes
                            },
                            OffenseType.PROPERTY: {
                                d: None for d in PropertyOffenseIncludesExcludes
                            },
                            OffenseType.PUBLIC_ORDER: {
                                d: None for d in PublicOrderOffenseIncludesExcludes
                            },
                            OffenseType.DRUG: {
                                d: None for d in DrugOffenseIncludesExcludes
                            },
                            OffenseType.OTHER: {},
                            OffenseType.UNKNOWN: {},
                        },
                    )
                ],
            ),
        )

    def test_disaggregation_includes_excludes_and_contents(self) -> None:
        disaggregation_metric_interface = MetricAggregatedDimensionData(
            dimension_to_enabled_status={d: True for d in ReleaseType},
            dimension_to_value=None,
            dimension_to_includes_excludes_member_to_setting={
                ReleaseType.TO_PROBATION_SUPERVISION: {
                    d: IncludesExcludesSetting.YES
                    for d in PrisonReleasesToProbationIncludesExcludes
                },
                ReleaseType.TO_PAROLE_SUPERVISION: {
                    d: IncludesExcludesSetting.YES
                    for d in PrisonReleasesToParoleIncludesExcludes
                },
                ReleaseType.TO_COMMUNITY_SUPERVISION: {
                    d: IncludesExcludesSetting.YES
                    for d in PrisonReleasesCommunitySupervisionIncludesExcludes
                },
                ReleaseType.NO_CONTROL: {
                    d: IncludesExcludesSetting.YES
                    for d in PrisonReleasesNoControlIncludesExcludes
                },
                ReleaseType.DEATH: {
                    d: IncludesExcludesSetting.YES
                    for d in PrisonReleasesDeathIncludesExcludes
                },
                ReleaseType.UNKNOWN: {},
                ReleaseType.OTHER: {},
            },
            dimension_to_contexts={
                ReleaseType.OTHER: [
                    MetricContextData(
                        ContextKey.ADDITIONAL_CONTEXT,
                        "Please provide additional context.",
                    )
                ],
            },
        )

        disaggregation_json = {
            "key": ReleaseType.dimension_identifier(),
            "enabled": True,
            "required": False,
            "helper_text": None,
            "should_sum_to_total": False,
            "display_name": "Prisons Release Types",
            "dimensions": [
                {
                    "datapoints": None,
                    "enabled": True,
                    "label": ReleaseType.TO_PAROLE_SUPERVISION.value,
                    "key": ReleaseType.TO_PAROLE_SUPERVISION.value,
                    "description": "The number of release events from the agency’s prison jurisdiction to parole supervision.",
                    "settings": [
                        {
                            "key": PrisonReleasesToParoleIncludesExcludes.AFTER_SANCTION.name,
                            "label": PrisonReleasesToParoleIncludesExcludes.AFTER_SANCTION.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonReleasesToParoleIncludesExcludes.ELIGIBLE.name,
                            "label": PrisonReleasesToParoleIncludesExcludes.ELIGIBLE.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonReleasesToParoleIncludesExcludes.COMMUTED_SENTENCE.name,
                            "label": PrisonReleasesToParoleIncludesExcludes.COMMUTED_SENTENCE.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonReleasesToParoleIncludesExcludes.RELEASE_TO_PAROLE.name,
                            "label": PrisonReleasesToParoleIncludesExcludes.RELEASE_TO_PAROLE.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                    ],
                },
                {
                    "datapoints": None,
                    "enabled": True,
                    "label": ReleaseType.TO_PROBATION_SUPERVISION.value,
                    "key": ReleaseType.TO_PROBATION_SUPERVISION.value,
                    "description": "The number of release events from the agency’s prison jurisdiction to probation supervision.",
                    "settings": [
                        {
                            "key": PrisonReleasesToProbationIncludesExcludes.COMPLETED_SENTENCE.name,
                            "label": PrisonReleasesToProbationIncludesExcludes.COMPLETED_SENTENCE.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonReleasesToProbationIncludesExcludes.AFTER_SANCTION.name,
                            "label": PrisonReleasesToProbationIncludesExcludes.AFTER_SANCTION.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonReleasesToProbationIncludesExcludes.SPLIT_SENTENCE.name,
                            "label": PrisonReleasesToProbationIncludesExcludes.SPLIT_SENTENCE.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonReleasesToProbationIncludesExcludes.SHOCK_PROBATION.name,
                            "label": PrisonReleasesToProbationIncludesExcludes.SHOCK_PROBATION.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonReleasesToProbationIncludesExcludes.TRANSFERRED_OUT.name,
                            "label": PrisonReleasesToProbationIncludesExcludes.TRANSFERRED_OUT.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                    ],
                },
                {
                    "datapoints": None,
                    "enabled": True,
                    "label": ReleaseType.TO_COMMUNITY_SUPERVISION.value,
                    "key": ReleaseType.TO_COMMUNITY_SUPERVISION.value,
                    "description": "The number of release events from the agency’s prison jurisdiction to another form of community supervision that is not probation or parole or in the agency’s jurisdiction.",
                    "settings": [
                        {
                            "key": PrisonReleasesCommunitySupervisionIncludesExcludes.RELEASED_TO_OTHER_AGENCY.name,
                            "label": PrisonReleasesCommunitySupervisionIncludesExcludes.RELEASED_TO_OTHER_AGENCY.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonReleasesCommunitySupervisionIncludesExcludes.DUAL_SUPERVISION.name,
                            "label": PrisonReleasesCommunitySupervisionIncludesExcludes.DUAL_SUPERVISION.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                    ],
                },
                {
                    "datapoints": None,
                    "enabled": True,
                    "label": ReleaseType.NO_CONTROL.value,
                    "key": ReleaseType.NO_CONTROL.value,
                    "description": "The number of release events from the agency’s prison jurisdiction with no additional correctional control.",
                    "settings": [
                        {
                            "key": PrisonReleasesNoControlIncludesExcludes.NO_POST_RELEASE.name,
                            "label": PrisonReleasesNoControlIncludesExcludes.NO_POST_RELEASE.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonReleasesNoControlIncludesExcludes.EXONERATION.name,
                            "label": PrisonReleasesNoControlIncludesExcludes.EXONERATION.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                    ],
                },
                {
                    "datapoints": None,
                    "enabled": True,
                    "key": ReleaseType.DEATH.value,
                    "label": ReleaseType.DEATH.value,
                    "description": "The number of release events from the agency’s prison jurisdiction due to death of people in custody.",
                    "settings": [
                        {
                            "key": PrisonReleasesDeathIncludesExcludes.DEATH.name,
                            "label": PrisonReleasesDeathIncludesExcludes.DEATH.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonReleasesDeathIncludesExcludes.DEATH_WHILE_ABSENT.name,
                            "label": PrisonReleasesDeathIncludesExcludes.DEATH_WHILE_ABSENT.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                    ],
                },
                {
                    "datapoints": None,
                    "enabled": True,
                    "label": ReleaseType.UNKNOWN.value,
                    "key": ReleaseType.UNKNOWN.value,
                    "description": "The number of release events from the agency’s prison jurisdiction where the release type is not known.",
                    "settings": [],
                },
                {
                    "datapoints": None,
                    "enabled": True,
                    "label": ReleaseType.OTHER.value,
                    "key": ReleaseType.OTHER.value,
                    "description": "The number of release events from the agency’s prison jurisdiction that are not releases to probation supervision, to parole supervision, to other community supervision, to no additional correctional control, or due to death.",
                    "settings": [],
                    "contexts": [
                        {
                            "key": "ADDITIONAL_CONTEXT",
                            "value": "Please provide additional context.",
                        }
                    ],
                },
            ],
        }

        # TODO(https://github.com/Recidiviz/pulse-data/issues/17055): this should pass once we update metric_disaggregation_data.to_json() with the new dimension_to_contexts field
        # self.assertEqual(
        #     disaggregation_metric_interface.to_json(
        #         dimension_definition=assert_type(
        #             prisons.releases.aggregated_dimensions, list
        #         )[0],
        #         entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
        #     ),
        #     disaggregation_json,
        # )

        self.assertEqual(
            MetricAggregatedDimensionData.from_json(
                json=disaggregation_json,
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
                disaggregation_definition=assert_type(
                    prisons.releases.aggregated_dimensions, list
                )[0],
            ),
            disaggregation_metric_interface,
        )

    def test_metric_includes_excludes_to_json(self) -> None:
        metric_interface = MetricInterface(
            key=prisons.grievances_upheld.key,
            is_metric_enabled=True,
            value=200,
            includes_excludes_member_to_setting={
                PrisonGrievancesIncludesExcludes.UPHELD: IncludesExcludesSetting.NO,
                PrisonGrievancesIncludesExcludes.REMEDY: IncludesExcludesSetting.NO,
                PrisonGrievancesIncludesExcludes.UNSUBSTANTIATED: IncludesExcludesSetting.YES,
                PrisonGrievancesIncludesExcludes.PENDING_RESOLUTION: IncludesExcludesSetting.YES,
                PrisonGrievancesIncludesExcludes.INFORMAL: IncludesExcludesSetting.NO,
                PrisonGrievancesIncludesExcludes.DUPLICATE: IncludesExcludesSetting.NO,
            },
            aggregated_dimensions=[],
            contexts=[],
        )

        metric_interface_json = {
            "key": prisons.grievances_upheld.key,
            "enabled": True,
            "system": {
                "key": "PRISONS",
                "display_name": "Prisons",
            },
            "display_name": prisons.grievances_upheld.display_name,
            "description": prisons.grievances_upheld.description,
            "definitions": [
                {
                    "definition": "A complaint or question filed with the institution by an "
                    "individual incarcerated regarding their experience, with procedures, "
                    "treatment, or interaction with officers.",
                    "term": "Grievance",
                }
            ],
            "disaggregated_by_supervision_subsystems": None,
            "reporting_note": prisons.grievances_upheld.reporting_note,
            "unit": prisons.grievances_upheld.metric_type.unit,
            "category": prisons.grievances_upheld.category.human_readable_string,
            "label": prisons.grievances_upheld.display_name,
            "frequency": prisons.grievances_upheld.reporting_frequencies[0].value,
            "custom_frequency": None,
            "starting_month": None,
            "filenames": ["grievances_upheld", "grievances_upheld_by_type"],
            "value": 200,
            "disaggregations": [],
            "datapoints": None,
            "contexts": [],
            "settings": [
                {
                    "key": PrisonGrievancesIncludesExcludes.UPHELD.name,
                    "label": PrisonGrievancesIncludesExcludes.UPHELD.value,
                    "included": "No",
                    "default": "Yes",
                },
                {
                    "key": PrisonGrievancesIncludesExcludes.REMEDY.name,
                    "label": PrisonGrievancesIncludesExcludes.REMEDY.value,
                    "included": "No",
                    "default": "Yes",
                },
                {
                    "key": PrisonGrievancesIncludesExcludes.UNSUBSTANTIATED.name,
                    "label": PrisonGrievancesIncludesExcludes.UNSUBSTANTIATED.value,
                    "included": "Yes",
                    "default": "No",
                },
                {
                    "key": PrisonGrievancesIncludesExcludes.PENDING_RESOLUTION.name,
                    "label": PrisonGrievancesIncludesExcludes.PENDING_RESOLUTION.value,
                    "included": "Yes",
                    "default": "No",
                },
                {
                    "key": PrisonGrievancesIncludesExcludes.INFORMAL.name,
                    "label": PrisonGrievancesIncludesExcludes.INFORMAL.value,
                    "included": "No",
                    "default": "No",
                },
                {
                    "key": PrisonGrievancesIncludesExcludes.DUPLICATE.name,
                    "label": PrisonGrievancesIncludesExcludes.DUPLICATE.value,
                    "included": "No",
                    "default": "No",
                },
            ],
        }

        self.assertEqual(
            metric_interface.to_json(
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
            ),
            metric_interface_json,
        )
        self.assertEqual(
            MetricInterface.from_json(
                json=metric_interface_json,
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
            ),
            metric_interface,
        )

    def test_custom_reporting_frequency(self) -> None:
        metric_interface = MetricInterface(
            key=law_enforcement.funding.key,
            is_metric_enabled=True,
            custom_reporting_frequency=CustomReportingFrequency(
                frequency=ReportingFrequency.ANNUAL, starting_month=1
            ),
            includes_excludes_member_to_setting={
                member: IncludesExcludesSetting.YES
                for member in LawEnforcementFundingIncludesExcludes
            },
            aggregated_dimensions=[],
            contexts=[],
        )

        metric_interface_json = {
            "key": law_enforcement.funding.key,
            "enabled": True,
            "system": {"display_name": "Law Enforcement", "key": "LAW_ENFORCEMENT"},
            "display_name": law_enforcement.funding.display_name,
            "description": law_enforcement.funding.description,
            "disaggregated_by_supervision_subsystems": None,
            "definitions": [],
            "reporting_note": law_enforcement.funding.reporting_note,
            "unit": law_enforcement.funding.metric_type.unit,
            "category": law_enforcement.funding.category.human_readable_string,
            "label": law_enforcement.funding.display_name,
            "frequency": law_enforcement.funding.reporting_frequencies[0].value,
            "custom_frequency": "ANNUAL",
            "starting_month": 1,
            "filenames": ["funding", "funding_by_type"],
            "value": None,
            "disaggregations": [],
            "contexts": [],
            "settings": [
                {
                    "default": "Yes",
                    "included": "Yes",
                    "key": "FISCAL_YEAR",
                    "label": "Funding for single fiscal year",
                },
                {
                    "default": "Yes",
                    "included": "Yes",
                    "key": "BIENNIUM_FUNDING",
                    "label": "Biennium funding appropriated during the time period",
                },
                {
                    "default": "Yes",
                    "included": "Yes",
                    "key": "MULTI_YEAR_APPROPRIATIONS",
                    "label": "Multi-year appropriations that are appropriated in during the time period",
                },
                {
                    "default": "Yes",
                    "included": "Yes",
                    "key": "STAFF_FUNDING",
                    "label": "Funding for agency staff",
                },
                {
                    "default": "Yes",
                    "included": "Yes",
                    "key": "EQUIPMENT",
                    "label": "Funding for the purchase of law enforcement " "equipment",
                },
                {
                    "default": "Yes",
                    "included": "Yes",
                    "key": "FACILITIES",
                    "label": "Funding for construction of law enforcement "
                    "facilities (e.g., offices, temporary detention "
                    "facilities, garages, etc.)",
                },
                {
                    "default": "Yes",
                    "included": "Yes",
                    "key": "MAINTENANCE",
                    "label": "Funding for the maintenance of law enforcement "
                    "equipment and facilities",
                },
                {
                    "default": "Yes",
                    "included": "Yes",
                    "key": "OTHER",
                    "label": "Funding for other purposes not captured by the listed "
                    "categories",
                },
                {
                    "default": "No",
                    "included": "Yes",
                    "key": "JAIL_OPERATIONS",
                    "label": "Expenses for the operation of jails",
                },
                {
                    "default": "No",
                    "included": "Yes",
                    "key": "SUPERVISION_SERVICES",
                    "label": "Expenses for the operation of community supervision "
                    "services",
                },
                {
                    "default": "No",
                    "included": "Yes",
                    "key": "JUVENILE_JAIL_OPERATIONS",
                    "label": "Expenses for the operation of juvenile jails",
                },
            ],
            "datapoints": None,
        }

        self.assertEqual(
            metric_interface.to_json(
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
            ),
            metric_interface_json,
        )
        self.assertEqual(
            MetricInterface.from_json(
                json=metric_interface_json,
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
            ),
            metric_interface,
        )

    def test_race_and_ethnicity_json(self) -> None:
        aggregated_dimension = MetricAggregatedDimensionData(
            dimension_to_value={dim: 10 for dim in RaceAndEthnicity},
            dimension_to_enabled_status={dim: True for dim in RaceAndEthnicity},
        )

        disaggregation_json = {
            "key": "global/race_and_ethnicity",
            "display_name": "Race / Ethnicities",
            "required": True,
            "should_sum_to_total": False,
            "helper_text": None,
            "enabled": True,
            "dimensions": [
                {
                    "datapoints": None,
                    "enabled": True,
                    "ethnicity": dim.ethnicity,
                    "key": dim.value,
                    "label": dim.value,
                    "race": dim.race,
                    "value": 10,
                    "description": None,
                    "contexts": [],
                }
                for dim in RaceAndEthnicity
            ],
        }
        self.assertEqual(
            aggregated_dimension.to_json(
                entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE,
                dimension_definition=AggregatedDimension(
                    dimension=RaceAndEthnicity, required=True
                ),
            ),
            disaggregation_json,
        )

    def test_get_supervision_metrics(self) -> None:
        # We should always grab all copies of supervision metrics
        # for each system. Which ones are actually enabled will be
        # determined later.
        supervision_metrics = MetricInterface.get_metric_definitions_for_systems(
            {schema.System.SUPERVISION, schema.System.PAROLE},
        )

        self.assertEqual(
            {m.key for m in supervision_metrics},
            {
                m.key
                for m in METRICS_BY_SYSTEM[schema.System.SUPERVISION.value]
                + METRICS_BY_SYSTEM[schema.System.PAROLE.value]
                if m.disabled is False
            },
        )
