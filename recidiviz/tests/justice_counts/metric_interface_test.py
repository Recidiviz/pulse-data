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

import enum
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Optional

from deepdiff import DeepDiff

from recidiviz.common.constants.justice_counts import ContextKey
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.agency_setting import AgencySettingInterface
from recidiviz.justice_counts.dimensions.law_enforcement import CallType
from recidiviz.justice_counts.dimensions.offense import OffenseType
from recidiviz.justice_counts.dimensions.person import (
    CensusRace,
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.dimensions.prisons import ReleaseType
from recidiviz.justice_counts.includes_excludes.law_enforcement import (
    CallsForServiceEmergencyCallsIncludesExcludes,
    CallsForServiceIncludesExcludes,
    CallsForServiceNonEmergencyCallsIncludesExcludes,
    LawEnforcementArrestsIncludesExcludes,
    LawEnforcementFundingPurposeIncludesExcludes,
    LawEnforcementFundingTimeframeIncludesExcludes,
    LawEnforcementReportedCrimeIncludesExcludes,
)
from recidiviz.justice_counts.includes_excludes.offense import (
    DrugOffenseIncludesExcludes,
    PersonOffenseIncludesExcludes,
    PropertyOffenseIncludesExcludes,
    PublicOrderOffenseIncludesExcludes,
)
from recidiviz.justice_counts.includes_excludes.prisons import (
    PrisonExpensesTimeframeAndSpendDownIncludesExcludes,
    PrisonExpensesTypeIncludesExcludes,
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
    Context,
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
from recidiviz.justice_counts.utils.constants import MetricUnit
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import (
    ReportingFrequency,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)
from recidiviz.utils.types import assert_type


class TestMetricInterface(JusticeCountsDatabaseTestCase):
    """Implements tests for the Justice Counts MetricInterface class."""

    def setUp(self) -> None:
        super().setUp()
        self.test_schema_objects = JusticeCountsSchemaTestObjects()
        self.reported_budget = self.test_schema_objects.funding_metric
        self.reported_calls_for_service = (
            self.test_schema_objects.reported_calls_for_service_metric
        )
        self.maxDiff = None
        prison_super_agency = self.test_schema_objects.test_prison_super_agency
        prison_child_agency_A = self.test_schema_objects.test_prison_child_agency_A
        prison_child_agency_B = self.test_schema_objects.test_prison_child_agency_B

        with SessionFactory.using_database(self.database_key) as session:
            session.add_all(
                [
                    prison_super_agency,
                    prison_child_agency_A,
                    prison_child_agency_B,
                ]
            )
            session.commit()
            session.flush()
            self.prison_super_agency_id = prison_super_agency.id
            prison_child_agency_A.super_agency_id = self.prison_super_agency_id
            prison_child_agency_B.super_agency_id = self.prison_super_agency_id

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
                "disaggregated_by_supervision_subsystems": None,
                "category": "Capacity and Costs",
                "value": 100000,
                "unit": MetricUnit.AMOUNT.value,
                "label": "Funding",
                "enabled": True,
                "frequency": "ANNUAL",
                "custom_frequency": None,
                "starting_month": 1,
                "filenames": ["funding", "funding_by_type"],
                "includes_excludes": [],
                "is_includes_excludes_configured": None,
                "contexts": [],
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
                "disaggregated_by_supervision_subsystems": None,
                "category": metric_definition.category.human_readable_string,
                "value": reported_metric.value,
                "unit": "Calls",
                "label": "Calls for Service",
                "enabled": True,
                "frequency": "MONTHLY",
                "custom_frequency": None,
                "starting_month": None,
                "filenames": ["calls_for_service", "calls_for_service_by_type"],
                "includes_excludes": [],
                "is_includes_excludes_configured": None,
                "contexts": [],
                "disaggregations": [
                    {
                        "contexts": [],
                        "key": "metric/law_enforcement/calls_for_service/type",
                        "display_name": "Call Types",
                        "required": True,
                        "should_sum_to_total": False,
                        "helper_text": None,
                        "is_breakdown_configured": None,
                        "enabled": None,
                        "dimensions": [
                            {
                                "datapoints": None,
                                "key": "Emergency Calls",
                                "label": "Emergency Calls",
                                "value": 20,
                                "enabled": None,
                                "description": "The number of calls for police assistance received by the agency that require immediate response.",
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                            },
                            {
                                "datapoints": None,
                                "key": "Non-emergency Calls",
                                "label": "Non-emergency Calls",
                                "value": 60,
                                "enabled": None,
                                "description": "The number of calls for police assistance received by the agency that do not require immediate response.",
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                            },
                            {
                                "datapoints": None,
                                "key": "Unknown Calls",
                                "label": "Unknown Calls",
                                "value": 20,
                                "enabled": None,
                                "description": "The number of calls for police assistance received by the agency of a type that is not known.",
                                "contexts": [
                                    {
                                        "key": "ADDITIONAL_CONTEXT",
                                        "value": None,
                                        "label": "Please describe what data is being included in this breakdown.",
                                    }
                                ],
                            },
                            {
                                "datapoints": None,
                                "key": "Other Calls",
                                "label": "Other Calls",
                                "value": None,
                                "enabled": None,
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
                "starting_month": 1,
                "filenames": [
                    "civilian_complaints",
                    "civilian_complaints_by_type",
                ],
                "disaggregated_by_supervision_subsystems": None,
                "category": metric_definition.category.human_readable_string,
                "includes_excludes": [],
                "is_includes_excludes_configured": None,
                "value": reported_metric.value,
                "unit": "Complaints Sustained",
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
                "disaggregated_by_supervision_subsystems": None,
                "datapoints": None,
                "category": metric_definition.category.human_readable_string,
                "value": reported_metric.value,
                "unit": "Arrest Events",
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
                "includes_excludes": [],
                "is_includes_excludes_configured": None,
                "contexts": [],
                "disaggregations": [
                    {
                        "contexts": [],
                        "key": "metric/offense/type",
                        "display_name": "Offense Types",
                        "required": True,
                        "should_sum_to_total": False,
                        "helper_text": None,
                        "is_breakdown_configured": None,
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
                                        "label": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
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
                                        "label": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
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
                                        "label": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
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
                                        "label": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
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
                                "contexts": [
                                    {
                                        "key": "ADDITIONAL_CONTEXT",
                                        "value": None,
                                        "label": "Please describe what data is being included in this breakdown.",
                                    }
                                ],
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
            "disaggregations": [
                {
                    "key": OffenseType.dimension_identifier(),
                    "contexts": [],
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

    def test_to_storage_json_disabled_metric(self) -> None:
        metric_interface = self.test_schema_objects.get_agency_metric_interface(
            is_metric_enabled=False
        )
        self.assertEqual(
            metric_interface.to_storage_json(),
            {
                "key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                "version": "v1",
                "is_metric_enabled": False,
                "contexts": {},
                "aggregated_dimensions": {
                    "metric/law_enforcement/calls_for_service/type": {
                        "contexts": {},
                        "dimension_to_enabled_status": {
                            "EMERGENCY": False,
                            "NON_EMERGENCY": False,
                            "UNKNOWN": False,
                        },
                        "dimension_to_includes_excludes_member_to_setting": {},
                        "dimension_to_contexts": {},
                        "is_breakdown_configured": None,
                        "dimension_to_includes_excludes_configured_status": {},
                        "dimension_to_other_sub_dimensions": {},
                    }
                },
                "disaggregated_by_supervision_subsystems": None,
                "includes_excludes_member_to_setting": {},
                "custom_reporting_frequency": {
                    "custom_frequency": None,
                    "starting_month": None,
                },
                "reporting_agency_id": None,
                "is_self_reported": None,
                "is_includes_excludes_configured": None,
            },
        )

    def test_to_storage_json_include_contexts(self) -> None:
        metric_interface = self.test_schema_objects.get_agency_metric_interface(
            is_metric_enabled=True, include_contexts=True
        )
        self.assertEqual(
            metric_interface.to_storage_json(),
            {
                "key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                "version": "v1",
                "is_metric_enabled": True,
                "contexts": {
                    "INCLUDES_EXCLUDES_DESCRIPTION": "our metrics are different because xyz"
                },
                "aggregated_dimensions": {},
                "disaggregated_by_supervision_subsystems": None,
                "includes_excludes_member_to_setting": {},
                "custom_reporting_frequency": {
                    "custom_frequency": None,
                    "starting_month": None,
                },
                "reporting_agency_id": None,
                "is_self_reported": None,
                "is_includes_excludes_configured": None,
            },
        )

    def test_to_storage_json_partially_disabled_disaggregation(self) -> None:
        metric_interface = self.test_schema_objects.get_agency_metric_interface(
            is_metric_enabled=True,
            include_disaggregation=True,
            use_partially_disabled_disaggregation=True,
        )
        self.assertEqual(
            metric_interface.to_storage_json(),
            {
                "key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                "version": "v1",
                "is_metric_enabled": True,
                "contexts": {},
                "aggregated_dimensions": {
                    "metric/law_enforcement/calls_for_service/type": {
                        "contexts": {},
                        "dimension_to_enabled_status": {
                            "EMERGENCY": True,
                            "NON_EMERGENCY": False,
                            "UNKNOWN": False,
                        },
                        "dimension_to_includes_excludes_member_to_setting": {},
                        "dimension_to_contexts": {},
                        "is_breakdown_configured": None,
                        "dimension_to_includes_excludes_configured_status": {},
                        "dimension_to_other_sub_dimensions": {},
                    }
                },
                "disaggregated_by_supervision_subsystems": None,
                "includes_excludes_member_to_setting": {},
                "custom_reporting_frequency": {
                    "custom_frequency": None,
                    "starting_month": None,
                },
                "is_includes_excludes_configured": None,
                "reporting_agency_id": None,
                "is_self_reported": None,
            },
        )

    def test_from_storage_json_with_contexts(self) -> None:
        """
        This test checks if the MetricInterface.from_storage_json method parses the JSON
        as expected. We are using the JSON representation of the following method:
        self.test_schema_objects.get_agency_metric_interface(
            is_metric_enabled=True,
            include_contexts=True,
        )
        """
        storage_json = {
            "key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
            "version": "v1",
            "is_metric_enabled": True,
            "contexts": {
                "INCLUDES_EXCLUDES_DESCRIPTION": "our metrics are different because xyz"
            },
            "aggregated_dimensions": {},
            "disaggregated_by_supervision_subsystems": None,
            "includes_excludes_member_to_setting": {},
            "custom_reporting_frequency": {
                "custom_frequency": None,
                "starting_month": None,
            },
            "is_includes_excludes_configured": None,
        }

        # Metric Interface instantiation
        metric_interface = MetricInterface(
            key="LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
            is_metric_enabled=True,
            contexts=[
                MetricContextData(
                    key=ContextKey.INCLUDES_EXCLUDES_DESCRIPTION,
                    value="our metrics are different because xyz",
                )
            ],
            aggregated_dimensions=[
                MetricAggregatedDimensionData(
                    dimension_to_value={
                        CallType.EMERGENCY: None,
                        CallType.NON_EMERGENCY: None,
                        CallType.OTHER: None,
                        CallType.UNKNOWN: None,
                    },
                    dimension_to_enabled_status={
                        CallType.EMERGENCY: None,
                        CallType.NON_EMERGENCY: None,
                        CallType.OTHER: None,
                        CallType.UNKNOWN: None,
                    },
                    dimension_to_includes_excludes_member_to_setting={
                        CallType.EMERGENCY: {},
                        CallType.NON_EMERGENCY: {},
                        CallType.OTHER: {},
                        CallType.UNKNOWN: {},
                    },
                    dimension_to_contexts={},
                    dimension_to_includes_excludes_configured_status={
                        CallType.EMERGENCY: None,
                        CallType.NON_EMERGENCY: None,
                        CallType.OTHER: None,
                        CallType.UNKNOWN: None,
                    },
                    is_breakdown_configured=None,
                )
            ],
            disaggregated_by_supervision_subsystems=None,
            includes_excludes_member_to_setting={},  # Empty as per JSON
            custom_reporting_frequency=CustomReportingFrequency(
                frequency=None, starting_month=None
            ),
            is_includes_excludes_configured=None,
        )
        # DeepDiff compares complex objects and returns the difference between them.
        self.assertFalse(
            DeepDiff(
                metric_interface,
                MetricInterface.from_storage_json(storage_json),
                ignore_order=True,
            )
        )

    def test_from_storage_json_include_disaggregations(self) -> None:
        """
        This test is to check if the MetricInterface.from_storage_json method parses the
        JSON as expected. We are using the JSON representation of the following method:
        self.test_schema_objects.get_agency_metric_interface(
            is_metric_enabled=True,
            include_disaggregation=True,
            use_partially_disabled_disaggregation=True,
        )
        Notice how in the parsed result, the dictionary values are populated as None (or
        an empty dict) for all dimension types even if the JSON representation does not
        contain an entry for that type. This is necessary since we are referencing the
        MetricDefinition of the metric when populating the MetricInterface from the JSON.
        """
        storage_json = {
            "key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
            "version": "v1",
            "is_metric_enabled": True,
            "contexts": {},
            "aggregated_dimensions": {
                "metric/law_enforcement/calls_for_service/type": {
                    "contexts": {},
                    "dimension_to_enabled_status": {
                        "EMERGENCY": True,
                        "NON_EMERGENCY": False,
                        "UNKNOWN": False,
                    },
                    "dimension_to_includes_excludes_member_to_setting": {},
                    "dimension_to_contexts": {},
                    "is_breakdown_configured": None,
                    "dimension_to_includes_excludes_configured_status": {},
                }
            },
            "disaggregated_by_supervision_subsystems": None,
            "includes_excludes_member_to_setting": {},
            "custom_reporting_frequency": {
                "custom_frequency": None,
                "starting_month": None,
            },
            "is_includes_excludes_configured": None,
        }
        metric_interface = MetricInterface(
            key="LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
            is_metric_enabled=True,
            contexts=[
                MetricContextData(
                    key=ContextKey.INCLUDES_EXCLUDES_DESCRIPTION, value=None
                )
            ],
            aggregated_dimensions=[
                MetricAggregatedDimensionData(
                    contexts=[],
                    dimension_to_value={
                        CallType.EMERGENCY: None,
                        CallType.NON_EMERGENCY: None,
                        CallType.OTHER: None,
                        CallType.UNKNOWN: None,
                    },
                    dimension_to_enabled_status={
                        CallType.EMERGENCY: True,
                        CallType.NON_EMERGENCY: False,
                        CallType.OTHER: None,
                        CallType.UNKNOWN: False,
                    },
                    dimension_to_includes_excludes_member_to_setting={
                        CallType.EMERGENCY: {},
                        CallType.NON_EMERGENCY: {},
                        CallType.OTHER: {},
                        CallType.UNKNOWN: {},
                    },
                    dimension_to_contexts={},
                    dimension_to_includes_excludes_configured_status={
                        CallType.EMERGENCY: None,
                        CallType.NON_EMERGENCY: None,
                        CallType.OTHER: None,
                        CallType.UNKNOWN: None,
                    },
                    is_breakdown_configured=None,
                )
            ],
            disaggregated_by_supervision_subsystems=None,
            includes_excludes_member_to_setting={},
            custom_reporting_frequency=CustomReportingFrequency(
                frequency=None, starting_month=None
            ),
            is_includes_excludes_configured=None,
        )

        # DeepDiff compares complex objects and returns the difference between them.
        self.assertFalse(
            DeepDiff(
                metric_interface,
                MetricInterface.from_storage_json(storage_json),
                ignore_order=True,
            )
        )

    def test_to_json_disabled_disaggregation(self) -> None:
        metric_definition = law_enforcement.funding
        metric_json = {
            "key": metric_definition.key,
            "enabled": False,
        }
        includes_excludes_member_to_setting: Dict[
            enum.Enum, Optional[IncludesExcludesSetting]
        ] = {}
        for timeframe_member in LawEnforcementFundingTimeframeIncludesExcludes:
            includes_excludes_member_to_setting[timeframe_member] = None
        for purpose_member in LawEnforcementFundingPurposeIncludesExcludes:
            includes_excludes_member_to_setting[purpose_member] = None
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
                includes_excludes_member_to_setting=includes_excludes_member_to_setting,
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
                "is_includes_excludes_configured": None,
                "key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                "system": {"key": "LAW_ENFORCEMENT", "display_name": "Law Enforcement"},
                "display_name": "Calls for Service",
                "description": "The number of calls for police assistance received by the agency.",
                "datapoints": None,
                "disaggregated_by_supervision_subsystems": None,
                "reporting_note": None,
                "value": None,
                "includes_excludes": [
                    {
                        "description": None,
                        "multiselect": True,
                        "settings": [
                            {
                                "key": "SERVICE_911",
                                "label": "Calls for service received by the agency’s dispatch service via 911",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "SERVICE_311",
                                "label": "Calls for service received by the agency’s dispatch service via 311 or equivalent non-emergency number",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "MUTUAL_AID",
                                "label": "Mutual aid calls for support received by the agency",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "OFFICER_INITIATED",
                                "label": "Officer-initiated calls for service (e.g., traffic stops, foot patrol)",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "OTHER_JURISDICTION",
                                "label": "Calls for service received by another jurisdiction and routed to the agency for response",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "AUTOMATED",
                                "label": "Automated calls for service (e.g., security system)",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "FIRE_SERVICE",
                                "label": "Calls for fire service",
                                "included": None,
                                "default": "No",
                            },
                            {
                                "key": "EMS",
                                "label": "Calls for EMS service",
                                "included": None,
                                "default": "No",
                            },
                            {
                                "key": "NON_POLICE_SERVICE",
                                "label": "Calls for other non-police service",
                                "included": None,
                                "default": "No",
                            },
                        ],
                    },
                ],
                "unit": "Calls",
                "category": "Populations",
                "label": "Calls for Service",
                "enabled": False,
                "frequency": "MONTHLY",
                "custom_frequency": None,
                "starting_month": None,
                "filenames": ["calls_for_service", "calls_for_service_by_type"],
                "contexts": [],
                "disaggregations": [
                    {
                        "contexts": [],
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
                                        "label": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "includes_excludes": [
                                    {
                                        "multiselect": True,
                                        "description": None,
                                        "settings": [
                                            {
                                                "key": "PRIORITY",
                                                "label": "Calls that require urgent or priority response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "CODE_2_RESPONSE",
                                                "label": "Calls that require code 2 or higher response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "IMMINENT_THREATS",
                                                "label": "Calls that relate to incidents with imminent threats to life or danger of serious injury",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "ONGOING_OFFENSES",
                                                "label": "Calls that relate to ongoing offenses that involve violence",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "SERIOUS_OFFENSE",
                                                "label": "Calls that relate to a serious offense that has just occurred and reason exists to believe the person suspected of committing the offense is in the area",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "OFFICER_IN_TROUBLE",
                                                "label": "Calls for “officer in trouble” or request for emergency assistance from an officer",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "TRAFFIC",
                                                "label": "Calls that relate to incidents that represent significant hazards to the flow of traffic",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "IN_PROGRESS_INCIDENT",
                                                "label": "Calls that relate to in-progress incidents that could be classified as crimes",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                        ],
                                    }
                                ],
                                "description": "The number of calls for police assistance received by the agency that require immediate response.",
                                "is_dimension_includes_excludes_configured": None,
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
                                        "label": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "includes_excludes": [
                                    {
                                        "multiselect": True,
                                        "description": None,
                                        "settings": [
                                            {
                                                "key": "ROUTINE_RESPONSE",
                                                "label": "Calls that require routine response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "CODE_1_RESPONSE",
                                                "label": "Calls that require code 1 response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "PATROL_REQUEST",
                                                "label": "Calls for patrol requests",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "ROUTINE_TRANSPORTATION",
                                                "label": "Calls for routine transportation",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "NON_EMERGENCY_SERVICE",
                                                "label": "Calls for non-emergency service",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "CIVILIAN_COMMUNITY_SERVICE",
                                                "label": "Calls routed to civilian community service officers for response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "STOLEN_PROPERTY",
                                                "label": "Calls to take a report of stolen property",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                        ],
                                    },
                                ],
                                "description": "The number of calls for police assistance received by the agency that do not require immediate response.",
                                "is_dimension_includes_excludes_configured": None,
                            },
                            {
                                "key": "Unknown Calls",
                                "label": "Unknown Calls",
                                "enabled": False,
                                "datapoints": None,
                                "contexts": [
                                    {
                                        "key": "ADDITIONAL_CONTEXT",
                                        "value": None,
                                        "label": "Please describe what data is being included in this breakdown.",
                                    }
                                ],
                                "includes_excludes": [],
                                "description": "The number of calls for police assistance received by the agency of a type that is not known.",
                                "is_dimension_includes_excludes_configured": None,
                            },
                        ],
                        "enabled": False,
                        "is_breakdown_configured": None,
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
                "is_includes_excludes_configured": None,
                "key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                "system": {"key": "LAW_ENFORCEMENT", "display_name": "Law Enforcement"},
                "display_name": "Calls for Service",
                "description": "The number of calls for police assistance received by the agency.",
                "datapoints": None,
                "disaggregated_by_supervision_subsystems": None,
                "reporting_note": None,
                "value": None,
                "includes_excludes": [
                    {
                        "description": None,
                        "multiselect": True,
                        "settings": [
                            {
                                "key": "SERVICE_911",
                                "label": "Calls for service received by the agency’s dispatch service via 911",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "SERVICE_311",
                                "label": "Calls for service received by the agency’s dispatch service via 311 or equivalent non-emergency number",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "MUTUAL_AID",
                                "label": "Mutual aid calls for support received by the agency",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "OFFICER_INITIATED",
                                "label": "Officer-initiated calls for service (e.g., traffic stops, foot patrol)",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "OTHER_JURISDICTION",
                                "label": "Calls for service received by another jurisdiction and routed to the agency for response",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "AUTOMATED",
                                "label": "Automated calls for service (e.g., security system)",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "FIRE_SERVICE",
                                "label": "Calls for fire service",
                                "included": None,
                                "default": "No",
                            },
                            {
                                "key": "EMS",
                                "label": "Calls for EMS service",
                                "included": None,
                                "default": "No",
                            },
                            {
                                "key": "NON_POLICE_SERVICE",
                                "label": "Calls for other non-police service",
                                "included": None,
                                "default": "No",
                            },
                        ],
                    },
                ],
                "unit": "Calls",
                "category": "Populations",
                "label": "Calls for Service",
                "enabled": True,
                "frequency": "MONTHLY",
                "custom_frequency": None,
                "starting_month": None,
                "filenames": ["calls_for_service", "calls_for_service_by_type"],
                "contexts": [],
                "disaggregations": [
                    {
                        "contexts": [],
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
                                        "label": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "includes_excludes": [
                                    {
                                        "multiselect": True,
                                        "description": None,
                                        "settings": [
                                            {
                                                "key": "PRIORITY",
                                                "label": "Calls that require urgent or priority response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "CODE_2_RESPONSE",
                                                "label": "Calls that require code 2 or higher response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "IMMINENT_THREATS",
                                                "label": "Calls that relate to incidents with imminent threats to life or danger of serious injury",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "ONGOING_OFFENSES",
                                                "label": "Calls that relate to ongoing offenses that involve violence",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "SERIOUS_OFFENSE",
                                                "label": "Calls that relate to a serious offense that has just occurred and reason exists to believe the person suspected of committing the offense is in the area",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "OFFICER_IN_TROUBLE",
                                                "label": "Calls for “officer in trouble” or request for emergency assistance from an officer",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "TRAFFIC",
                                                "label": "Calls that relate to incidents that represent significant hazards to the flow of traffic",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "IN_PROGRESS_INCIDENT",
                                                "label": "Calls that relate to in-progress incidents that could be classified as crimes",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                        ],
                                    },
                                ],
                                "description": "The number of calls for police assistance received by the agency that require immediate response.",
                                "is_dimension_includes_excludes_configured": None,
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
                                        "label": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "includes_excludes": [
                                    {
                                        "multiselect": True,
                                        "description": None,
                                        "settings": [
                                            {
                                                "key": "ROUTINE_RESPONSE",
                                                "label": "Calls that require routine response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "CODE_1_RESPONSE",
                                                "label": "Calls that require code 1 response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "PATROL_REQUEST",
                                                "label": "Calls for patrol requests",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "ROUTINE_TRANSPORTATION",
                                                "label": "Calls for routine transportation",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "NON_EMERGENCY_SERVICE",
                                                "label": "Calls for non-emergency service",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "CIVILIAN_COMMUNITY_SERVICE",
                                                "label": "Calls routed to civilian community service officers for response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "STOLEN_PROPERTY",
                                                "label": "Calls to take a report of stolen property",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                        ],
                                    },
                                ],
                                "description": "The number of calls for police assistance received by the agency that do not require immediate response.",
                                "is_dimension_includes_excludes_configured": None,
                            },
                            {
                                "key": "Unknown Calls",
                                "label": "Unknown Calls",
                                "enabled": False,
                                "datapoints": None,
                                "contexts": [
                                    {
                                        "key": "ADDITIONAL_CONTEXT",
                                        "value": None,
                                        "label": "Please describe what data is being included in this breakdown.",
                                    }
                                ],
                                "includes_excludes": [],
                                "description": "The number of calls for police assistance received by the agency of a type that is not known.",
                                "is_dimension_includes_excludes_configured": None,
                            },
                        ],
                        "enabled": False,
                        "is_breakdown_configured": None,
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
                "is_includes_excludes_configured": None,
                "key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                "system": {"key": "LAW_ENFORCEMENT", "display_name": "Law Enforcement"},
                "display_name": "Calls for Service",
                "description": "The number of calls for police assistance received by the agency.",
                "datapoints": None,
                "disaggregated_by_supervision_subsystems": None,
                "reporting_note": None,
                "value": None,
                "includes_excludes": [
                    {
                        "description": None,
                        "multiselect": True,
                        "settings": [
                            {
                                "key": "SERVICE_911",
                                "label": "Calls for service received by the agency’s dispatch service via 911",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "SERVICE_311",
                                "label": "Calls for service received by the agency’s dispatch service via 311 or equivalent non-emergency number",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "MUTUAL_AID",
                                "label": "Mutual aid calls for support received by the agency",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "OFFICER_INITIATED",
                                "label": "Officer-initiated calls for service (e.g., traffic stops, foot patrol)",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "OTHER_JURISDICTION",
                                "label": "Calls for service received by another jurisdiction and routed to the agency for response",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "AUTOMATED",
                                "label": "Automated calls for service (e.g., security system)",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "FIRE_SERVICE",
                                "label": "Calls for fire service",
                                "included": None,
                                "default": "No",
                            },
                            {
                                "key": "EMS",
                                "label": "Calls for EMS service",
                                "included": None,
                                "default": "No",
                            },
                            {
                                "key": "NON_POLICE_SERVICE",
                                "label": "Calls for other non-police service",
                                "included": None,
                                "default": "No",
                            },
                        ],
                    },
                ],
                "unit": "Calls",
                "category": "Populations",
                "label": "Calls for Service",
                "enabled": True,
                "frequency": "MONTHLY",
                "custom_frequency": None,
                "starting_month": None,
                "filenames": ["calls_for_service", "calls_for_service_by_type"],
                "contexts": [],
                "disaggregations": [
                    {
                        "contexts": [],
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
                                        "label": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "includes_excludes": [
                                    {
                                        "multiselect": True,
                                        "description": None,
                                        "settings": [
                                            {
                                                "key": "PRIORITY",
                                                "label": "Calls that require urgent or priority response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "CODE_2_RESPONSE",
                                                "label": "Calls that require code 2 or higher response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "IMMINENT_THREATS",
                                                "label": "Calls that relate to incidents with imminent threats to life or danger of serious injury",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "ONGOING_OFFENSES",
                                                "label": "Calls that relate to ongoing offenses that involve violence",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "SERIOUS_OFFENSE",
                                                "label": "Calls that relate to a serious offense that has just occurred and reason exists to believe the person suspected of committing the offense is in the area",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "OFFICER_IN_TROUBLE",
                                                "label": "Calls for “officer in trouble” or request for emergency assistance from an officer",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "TRAFFIC",
                                                "label": "Calls that relate to incidents that represent significant hazards to the flow of traffic",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "IN_PROGRESS_INCIDENT",
                                                "label": "Calls that relate to in-progress incidents that could be classified as crimes",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                        ],
                                    },
                                ],
                                "description": "The number of calls for police assistance received by the agency that require immediate response.",
                                "is_dimension_includes_excludes_configured": None,
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
                                        "label": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "includes_excludes": [
                                    {
                                        "multiselect": True,
                                        "description": None,
                                        "settings": [
                                            {
                                                "key": "ROUTINE_RESPONSE",
                                                "label": "Calls that require routine response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "CODE_1_RESPONSE",
                                                "label": "Calls that require code 1 response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "PATROL_REQUEST",
                                                "label": "Calls for patrol requests",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "ROUTINE_TRANSPORTATION",
                                                "label": "Calls for routine transportation",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "NON_EMERGENCY_SERVICE",
                                                "label": "Calls for non-emergency service",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "CIVILIAN_COMMUNITY_SERVICE",
                                                "label": "Calls routed to civilian community service officers for response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "STOLEN_PROPERTY",
                                                "label": "Calls to take a report of stolen property",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                        ],
                                    },
                                ],
                                "description": "The number of calls for police assistance received by the agency that do not require immediate response.",
                                "is_dimension_includes_excludes_configured": None,
                            },
                            {
                                "key": "Unknown Calls",
                                "label": "Unknown Calls",
                                "enabled": False,
                                "datapoints": None,
                                "contexts": [
                                    {
                                        "key": "ADDITIONAL_CONTEXT",
                                        "value": None,
                                        "label": "Please describe what data is being included in this breakdown.",
                                    }
                                ],
                                "includes_excludes": [],
                                "description": "The number of calls for police assistance received by the agency of a type that is not known.",
                                "is_dimension_includes_excludes_configured": None,
                            },
                        ],
                        "enabled": False,
                        "is_breakdown_configured": None,
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
                "is_includes_excludes_configured": None,
                "key": metric_definition.key,
                "enabled": True,
                "category": metric_definition.category.human_readable_string,
                "frequency": "MONTHLY",
                "custom_frequency": None,
                "starting_month": None,
                "filenames": ["calls_for_service", "calls_for_service_by_type"],
                "contexts": [],
                "includes_excludes": [
                    {
                        "description": None,
                        "multiselect": True,
                        "settings": [
                            {
                                "default": "Yes",
                                "included": None,
                                "key": "SERVICE_911",
                                "label": "Calls for service received by the agency’s dispatch service via 911",
                            },
                            {
                                "default": "Yes",
                                "included": None,
                                "key": "SERVICE_311",
                                "label": "Calls for service received by the agency’s dispatch service via 311 or equivalent non-emergency number",
                            },
                            {
                                "default": "Yes",
                                "included": None,
                                "key": "MUTUAL_AID",
                                "label": "Mutual aid calls for support received by the agency",
                            },
                            {
                                "default": "Yes",
                                "included": None,
                                "key": "OFFICER_INITIATED",
                                "label": "Officer-initiated calls for service (e.g., traffic stops, foot patrol)",
                            },
                            {
                                "default": "Yes",
                                "included": None,
                                "key": "OTHER_JURISDICTION",
                                "label": "Calls for service received by another jurisdiction and routed to the agency for response",
                            },
                            {
                                "default": "Yes",
                                "included": None,
                                "key": "AUTOMATED",
                                "label": "Automated calls for service (e.g., security system)",
                            },
                            {
                                "default": "No",
                                "included": None,
                                "key": "FIRE_SERVICE",
                                "label": "Calls for fire service",
                            },
                            {
                                "default": "No",
                                "included": None,
                                "key": "EMS",
                                "label": "Calls for EMS service",
                            },
                            {
                                "default": "No",
                                "included": None,
                                "key": "NON_POLICE_SERVICE",
                                "label": "Calls for other non-police service",
                            },
                        ],
                    },
                ],
                "datapoints": None,
                "disaggregated_by_supervision_subsystems": None,
                "display_name": metric_definition.display_name,
                "description": metric_definition.description,
                "label": "Calls for Service",
                "reporting_note": metric_definition.reporting_note,
                "system": {
                    "key": "LAW_ENFORCEMENT",
                    "display_name": "Law Enforcement",
                },
                "unit": "Calls",
                "value": None,
                "disaggregations": [
                    {
                        "contexts": [],
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
                                "is_dimension_includes_excludes_configured": None,
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "includes_excludes": [
                                    {
                                        "multiselect": True,
                                        "description": None,
                                        "settings": [
                                            {
                                                "key": "PRIORITY",
                                                "label": "Calls that require urgent or priority response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "CODE_2_RESPONSE",
                                                "label": "Calls that require code 2 or higher response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "IMMINENT_THREATS",
                                                "label": "Calls that relate to incidents with imminent threats to life or danger of serious injury",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "ONGOING_OFFENSES",
                                                "label": "Calls that relate to ongoing offenses that involve violence",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "SERIOUS_OFFENSE",
                                                "label": "Calls that relate to a serious offense that has just occurred and reason exists to believe the person suspected of committing the offense is in the area",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "OFFICER_IN_TROUBLE",
                                                "label": "Calls for “officer in trouble” or request for emergency assistance from an officer",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "TRAFFIC",
                                                "label": "Calls that relate to incidents that represent significant hazards to the flow of traffic",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "IN_PROGRESS_INCIDENT",
                                                "label": "Calls that relate to in-progress incidents that could be classified as crimes",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                        ],
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
                                        "label": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "includes_excludes": [
                                    {
                                        "multiselect": True,
                                        "description": None,
                                        "settings": [
                                            {
                                                "key": "ROUTINE_RESPONSE",
                                                "label": "Calls that require routine response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "CODE_1_RESPONSE",
                                                "label": "Calls that require code 1 response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "PATROL_REQUEST",
                                                "label": "Calls for patrol requests",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "ROUTINE_TRANSPORTATION",
                                                "label": "Calls for routine transportation",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "NON_EMERGENCY_SERVICE",
                                                "label": "Calls for non-emergency service",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "CIVILIAN_COMMUNITY_SERVICE",
                                                "label": "Calls routed to civilian community service officers for response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "STOLEN_PROPERTY",
                                                "label": "Calls to take a report of stolen property",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                        ],
                                    },
                                ],
                                "description": "The number of calls for police assistance received by the agency that do not require immediate response.",
                                "is_dimension_includes_excludes_configured": None,
                            },
                            {
                                "datapoints": None,
                                "enabled": False,
                                "label": CallType.UNKNOWN.value,
                                "key": CallType.UNKNOWN.value,
                                "contexts": [
                                    {
                                        "key": "ADDITIONAL_CONTEXT",
                                        "value": None,
                                        "label": "Please describe what data is being included in this breakdown.",
                                    }
                                ],
                                "includes_excludes": [],
                                "description": "The number of calls for police assistance received by the agency of a type that is not known.",
                                "is_dimension_includes_excludes_configured": None,
                            },
                        ],
                        "is_breakdown_configured": None,
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
                    "agency_name": None,
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
                    "agency_name": None,
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
                "agency_name": None,
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
                "agency_name": None,
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
                "agency_name": None,
            }
        ]
        self.assertEqual(
            metric_interface.to_json(
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
                aggregate_datapoints_json=aggregate_datapoints_json,
                dimension_id_to_dimension_member_to_datapoints_json=dimension_id_to_dimension_member_to_datapoints_json,
            ),
            {
                "is_includes_excludes_configured": None,
                "key": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
                "system": {"key": "LAW_ENFORCEMENT", "display_name": "Law Enforcement"},
                "display_name": "Calls for Service",
                "description": "The number of calls for police assistance received by the agency.",
                "datapoints": [
                    {
                        "agency_name": None,
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
                        "agency_name": None,
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
                "includes_excludes": [
                    {
                        "description": None,
                        "multiselect": True,
                        "settings": [
                            {
                                "key": "SERVICE_911",
                                "label": "Calls for service received by the agency’s dispatch service via 911",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "SERVICE_311",
                                "label": "Calls for service received by the agency’s dispatch service via 311 or equivalent non-emergency number",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "MUTUAL_AID",
                                "label": "Mutual aid calls for support received by the agency",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "OFFICER_INITIATED",
                                "label": "Officer-initiated calls for service (e.g., traffic stops, foot patrol)",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "OTHER_JURISDICTION",
                                "label": "Calls for service received by another jurisdiction and routed to the agency for response",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "AUTOMATED",
                                "label": "Automated calls for service (e.g., security system)",
                                "included": None,
                                "default": "Yes",
                            },
                            {
                                "key": "FIRE_SERVICE",
                                "label": "Calls for fire service",
                                "included": None,
                                "default": "No",
                            },
                            {
                                "key": "EMS",
                                "label": "Calls for EMS service",
                                "included": None,
                                "default": "No",
                            },
                            {
                                "key": "NON_POLICE_SERVICE",
                                "label": "Calls for other non-police service",
                                "included": None,
                                "default": "No",
                            },
                        ],
                    }
                ],
                "unit": "Calls",
                "category": "Populations",
                "label": "Calls for Service",
                "enabled": False,
                "frequency": "MONTHLY",
                "custom_frequency": None,
                "starting_month": None,
                "filenames": ["calls_for_service", "calls_for_service_by_type"],
                "contexts": [],
                "disaggregations": [
                    {
                        "contexts": [],
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
                                        "agency_name": None,
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
                                ],
                                "contexts": [
                                    {
                                        "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                        "value": None,
                                        "label": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "includes_excludes": [
                                    {
                                        "multiselect": True,
                                        "description": None,
                                        "settings": [
                                            {
                                                "key": "PRIORITY",
                                                "label": "Calls that require urgent or priority response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "CODE_2_RESPONSE",
                                                "label": "Calls that require code 2 or higher response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "IMMINENT_THREATS",
                                                "label": "Calls that relate to incidents with imminent threats to life or danger of serious injury",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "ONGOING_OFFENSES",
                                                "label": "Calls that relate to ongoing offenses that involve violence",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "SERIOUS_OFFENSE",
                                                "label": "Calls that relate to a serious offense that has just occurred and reason exists to believe the person suspected of committing the offense is in the area",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "OFFICER_IN_TROUBLE",
                                                "label": "Calls for “officer in trouble” or request for emergency assistance from an officer",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "TRAFFIC",
                                                "label": "Calls that relate to incidents that represent significant hazards to the flow of traffic",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "IN_PROGRESS_INCIDENT",
                                                "label": "Calls that relate to in-progress incidents that could be classified as crimes",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                        ],
                                    },
                                ],
                                "description": "The number of calls for police assistance received by the agency that require immediate response.",
                                "is_dimension_includes_excludes_configured": None,
                            },
                            {
                                "key": "Non-emergency Calls",
                                "label": "Non-emergency Calls",
                                "enabled": False,
                                "datapoints": [
                                    {
                                        "agency_name": None,
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
                                        "label": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                                    },
                                ],
                                "includes_excludes": [
                                    {
                                        "multiselect": True,
                                        "description": None,
                                        "settings": [
                                            {
                                                "key": "ROUTINE_RESPONSE",
                                                "label": "Calls that require routine response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "CODE_1_RESPONSE",
                                                "label": "Calls that require code 1 response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "PATROL_REQUEST",
                                                "label": "Calls for patrol requests",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "ROUTINE_TRANSPORTATION",
                                                "label": "Calls for routine transportation",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "NON_EMERGENCY_SERVICE",
                                                "label": "Calls for non-emergency service",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "CIVILIAN_COMMUNITY_SERVICE",
                                                "label": "Calls routed to civilian community service officers for response",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                            {
                                                "key": "STOLEN_PROPERTY",
                                                "label": "Calls to take a report of stolen property",
                                                "included": None,
                                                "default": "Yes",
                                            },
                                        ],
                                    },
                                ],
                                "description": "The number of calls for police assistance received by the agency that do not require immediate response.",
                                "is_dimension_includes_excludes_configured": None,
                            },
                            {
                                "key": "Unknown Calls",
                                "label": "Unknown Calls",
                                "enabled": False,
                                "datapoints": [
                                    {
                                        "agency_name": None,
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
                                "contexts": [
                                    {
                                        "key": "ADDITIONAL_CONTEXT",
                                        "value": None,
                                        "label": "Please describe what data is being included in this breakdown.",
                                    }
                                ],
                                "includes_excludes": [],
                                "description": "The number of calls for police assistance received by the agency of a type that is not known.",
                                "is_dimension_includes_excludes_configured": None,
                            },
                        ],
                        "enabled": False,
                        "is_breakdown_configured": None,
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
        includes_excludes_member_to_setting: Dict[
            enum.Enum, Optional[IncludesExcludesSetting]
        ] = {}
        for timeframe_member in LawEnforcementFundingTimeframeIncludesExcludes:
            includes_excludes_member_to_setting[timeframe_member] = None
        for purpose_member in LawEnforcementFundingPurposeIncludesExcludes:
            includes_excludes_member_to_setting[purpose_member] = None
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
                includes_excludes_member_to_setting=includes_excludes_member_to_setting,
            ),
        )

    def test_civilian_complaints_json_to_agency_metric(self) -> None:
        metric_definition = law_enforcement.calls_for_service
        metric_json = {
            "key": metric_definition.key,
            "disaggregations": [
                {
                    "key": CallType.dimension_identifier(),
                    "contexts": [],
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
                        is_breakdown_configured=None,
                        dimension_to_includes_excludes_configured_status={
                            CallType.EMERGENCY: None,
                            CallType.UNKNOWN: None,
                        },
                    )
                ],
                is_includes_excludes_configured=None,
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
            dimension_to_includes_excludes_configured_status={
                ReleaseType.TO_PROBATION_SUPERVISION: None,
                ReleaseType.TO_PAROLE_SUPERVISION: None,
                ReleaseType.TO_COMMUNITY_SUPERVISION: None,
                ReleaseType.NO_CONTROL: None,
                ReleaseType.DEATH: None,
                ReleaseType.UNKNOWN: None,
                ReleaseType.OTHER: None,
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
                    "includes_excludes": [
                        {
                            "description": None,
                            "multiselect": True,
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
                    ],
                },
                {
                    "datapoints": None,
                    "enabled": True,
                    "label": ReleaseType.TO_PROBATION_SUPERVISION.value,
                    "key": ReleaseType.TO_PROBATION_SUPERVISION.value,
                    "description": "The number of release events from the agency’s prison jurisdiction to probation supervision.",
                    "includes_excludes": [
                        {
                            "description": None,
                            "multiselect": True,
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
                    ],
                },
                {
                    "datapoints": None,
                    "enabled": True,
                    "label": ReleaseType.TO_COMMUNITY_SUPERVISION.value,
                    "key": ReleaseType.TO_COMMUNITY_SUPERVISION.value,
                    "description": "The number of release events from the agency’s prison jurisdiction to another form of community supervision that is not probation or parole or in the agency’s jurisdiction.",
                    "includes_excludes": [
                        {
                            "description": None,
                            "multiselect": True,
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
                    ],
                },
                {
                    "datapoints": None,
                    "enabled": True,
                    "label": ReleaseType.NO_CONTROL.value,
                    "key": ReleaseType.NO_CONTROL.value,
                    "description": "The number of release events from the agency’s prison jurisdiction with no additional correctional control.",
                    "includes_excludes": [
                        {
                            "description": None,
                            "multiselect": True,
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
                    ],
                },
                {
                    "datapoints": None,
                    "enabled": True,
                    "key": ReleaseType.DEATH.value,
                    "label": ReleaseType.DEATH.value,
                    "description": "The number of release events from the agency’s prison jurisdiction due to death of people in custody.",
                    "includes_excludes": [
                        {
                            "description": None,
                            "multiselect": True,
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
                    ],
                },
                {
                    "datapoints": None,
                    "enabled": True,
                    "label": ReleaseType.UNKNOWN.value,
                    "key": ReleaseType.UNKNOWN.value,
                    "description": "The number of release events from the agency’s prison jurisdiction where the release type is not known.",
                    "includes_excludes": [],
                },
                {
                    "datapoints": None,
                    "enabled": True,
                    "label": ReleaseType.OTHER.value,
                    "key": ReleaseType.OTHER.value,
                    "description": "The number of release events from the agency’s prison jurisdiction that are not releases to probation supervision, to parole supervision, to other community supervision, to no additional correctional control, or due to death.",
                    "includes_excludes": [],
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
        # MetricAggregatedDimensionData.to_json() expects includes_excludes field and not settings field
        # self.assertEqual(
        #     disaggregation_metric_interface.to_json(
        #         dimension_definition=assert_type(
        #             prisons.releases.aggregated_dimensions, list
        #         )[0],
        #         entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
        #     ),
        #     disaggregation_json,
        # )

        disaggregation_json = {
            key: value
            for key, value in disaggregation_json.items()
            if key != "includes_excludes"
        }
        disaggregation_json["settings"] = []
        disaggregation_json["dimensions"][0]["settings"] = [  # type: ignore[index]
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
        ]
        disaggregation_json["dimensions"][1]["settings"] = [  # type: ignore[index]
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
        ]
        disaggregation_json["dimensions"][2]["settings"] = [  # type: ignore[index]
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
        ]
        disaggregation_json["dimensions"][3]["settings"] = [  # type: ignore[index]
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
        ]
        disaggregation_json["dimensions"][4]["settings"] = [  # type: ignore[index]
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
        ]
        disaggregation_json["dimensions"][5]["settings"] = []  # type: ignore[index]
        disaggregation_json["dimensions"][6]["settings"] = []  # type: ignore[index]

        # MetricAggregatedDimensionData.from_json() expects settings field and not includes_excludes field
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
            key=prisons.expenses.key,
            is_metric_enabled=True,
            value=200,
            includes_excludes_member_to_setting={
                PrisonExpensesTimeframeAndSpendDownIncludesExcludes.SINGLE_YEAR: IncludesExcludesSetting.YES,
                PrisonExpensesTimeframeAndSpendDownIncludesExcludes.BIENNNIUM_EXPENSES: IncludesExcludesSetting.YES,
                PrisonExpensesTimeframeAndSpendDownIncludesExcludes.MULTI_YEAR: IncludesExcludesSetting.YES,
                PrisonExpensesTypeIncludesExcludes.PRISON_FACILITY: IncludesExcludesSetting.YES,
                PrisonExpensesTypeIncludesExcludes.OPERATIONS_AND_MAINTENANCE: IncludesExcludesSetting.YES,
                PrisonExpensesTypeIncludesExcludes.CONSTRUCTION: IncludesExcludesSetting.YES,
                PrisonExpensesTypeIncludesExcludes.TREATMENT: IncludesExcludesSetting.YES,
                PrisonExpensesTypeIncludesExcludes.HEALTH_CARE: IncludesExcludesSetting.YES,
                PrisonExpensesTypeIncludesExcludes.FACILITY_STAFF: IncludesExcludesSetting.YES,
                PrisonExpensesTypeIncludesExcludes.ADMINISTRATION_STAFF: IncludesExcludesSetting.YES,
                PrisonExpensesTypeIncludesExcludes.OPERATION: IncludesExcludesSetting.YES,
                PrisonExpensesTypeIncludesExcludes.JAIL_FACILITY: IncludesExcludesSetting.NO,
                PrisonExpensesTypeIncludesExcludes.JUVENILE_JAIL: IncludesExcludesSetting.NO,
                PrisonExpensesTypeIncludesExcludes.NON_PRISON_ACTIVITIES: IncludesExcludesSetting.NO,
                PrisonExpensesTypeIncludesExcludes.LAW_ENFORCEMENT: IncludesExcludesSetting.NO,
            },
            aggregated_dimensions=[],
            contexts=[],
            is_includes_excludes_configured=None,
        )

        metric_interface_json = {
            "key": prisons.expenses.key,
            "enabled": True,
            "system": {
                "key": "PRISONS",
                "display_name": "Prisons",
            },
            "display_name": prisons.expenses.display_name,
            "description": prisons.expenses.description,
            "disaggregated_by_supervision_subsystems": None,
            "unit": "Amount",
            "category": prisons.expenses.category.human_readable_string,
            "label": prisons.expenses.display_name,
            "frequency": prisons.expenses.reporting_frequencies[0].value,
            "custom_frequency": None,
            "starting_month": 1,
            "filenames": ["expenses", "expenses_by_type"],
            "value": 200,
            "disaggregations": [],
            "datapoints": None,
            "contexts": [],
            "reporting_note": None,
            "includes_excludes": [
                {
                    "multiselect": True,
                    "description": "Expenses timeframe and spend-down",
                    "settings": [
                        {
                            "key": PrisonExpensesTimeframeAndSpendDownIncludesExcludes.SINGLE_YEAR.name,
                            "label": PrisonExpensesTimeframeAndSpendDownIncludesExcludes.SINGLE_YEAR.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonExpensesTimeframeAndSpendDownIncludesExcludes.BIENNNIUM_EXPENSES.name,
                            "label": PrisonExpensesTimeframeAndSpendDownIncludesExcludes.BIENNNIUM_EXPENSES.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonExpensesTimeframeAndSpendDownIncludesExcludes.MULTI_YEAR.name,
                            "label": PrisonExpensesTimeframeAndSpendDownIncludesExcludes.MULTI_YEAR.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                    ],
                },
                {
                    "multiselect": True,
                    "description": "Expense type",
                    "settings": [
                        {
                            "key": PrisonExpensesTypeIncludesExcludes.PRISON_FACILITY.name,
                            "label": PrisonExpensesTypeIncludesExcludes.PRISON_FACILITY.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonExpensesTypeIncludesExcludes.OPERATIONS_AND_MAINTENANCE.name,
                            "label": PrisonExpensesTypeIncludesExcludes.OPERATIONS_AND_MAINTENANCE.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonExpensesTypeIncludesExcludes.CONSTRUCTION.name,
                            "label": PrisonExpensesTypeIncludesExcludes.CONSTRUCTION.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonExpensesTypeIncludesExcludes.TREATMENT.name,
                            "label": PrisonExpensesTypeIncludesExcludes.TREATMENT.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonExpensesTypeIncludesExcludes.HEALTH_CARE.name,
                            "label": PrisonExpensesTypeIncludesExcludes.HEALTH_CARE.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonExpensesTypeIncludesExcludes.FACILITY_STAFF.name,
                            "label": PrisonExpensesTypeIncludesExcludes.FACILITY_STAFF.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonExpensesTypeIncludesExcludes.ADMINISTRATION_STAFF.name,
                            "label": PrisonExpensesTypeIncludesExcludes.ADMINISTRATION_STAFF.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonExpensesTypeIncludesExcludes.OPERATION.name,
                            "label": PrisonExpensesTypeIncludesExcludes.OPERATION.value,
                            "included": "Yes",
                            "default": "Yes",
                        },
                        {
                            "key": PrisonExpensesTypeIncludesExcludes.JAIL_FACILITY.name,
                            "label": PrisonExpensesTypeIncludesExcludes.JAIL_FACILITY.value,
                            "included": "No",
                            "default": "No",
                        },
                        {
                            "key": PrisonExpensesTypeIncludesExcludes.JUVENILE_JAIL.name,
                            "label": PrisonExpensesTypeIncludesExcludes.JUVENILE_JAIL.value,
                            "included": "No",
                            "default": "No",
                        },
                        {
                            "key": PrisonExpensesTypeIncludesExcludes.NON_PRISON_ACTIVITIES.name,
                            "label": PrisonExpensesTypeIncludesExcludes.NON_PRISON_ACTIVITIES.value,
                            "included": "No",
                            "default": "No",
                        },
                        {
                            "key": PrisonExpensesTypeIncludesExcludes.LAW_ENFORCEMENT.name,
                            "label": PrisonExpensesTypeIncludesExcludes.LAW_ENFORCEMENT.value,
                            "included": "No",
                            "default": "No",
                        },
                    ],
                },
            ],
            "is_includes_excludes_configured": None,
        }

        # MetricInterface.to_json() expects includes_excludes field and not settings field
        self.assertEqual(
            metric_interface.to_json(
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
            ),
            metric_interface_json,
        )
        metric_interface_json = {
            key: value
            for key, value in metric_interface_json.items()
            if key != "includes_excludes"
        }
        metric_interface_json["settings"] = [
            {
                "key": PrisonExpensesTimeframeAndSpendDownIncludesExcludes.SINGLE_YEAR.name,
                "label": PrisonExpensesTimeframeAndSpendDownIncludesExcludes.SINGLE_YEAR.value,
                "included": "Yes",
                "default": "Yes",
            },
            {
                "key": PrisonExpensesTimeframeAndSpendDownIncludesExcludes.BIENNNIUM_EXPENSES.name,
                "label": PrisonExpensesTimeframeAndSpendDownIncludesExcludes.BIENNNIUM_EXPENSES.value,
                "included": "Yes",
                "default": "Yes",
            },
            {
                "key": PrisonExpensesTimeframeAndSpendDownIncludesExcludes.MULTI_YEAR.name,
                "label": PrisonExpensesTimeframeAndSpendDownIncludesExcludes.MULTI_YEAR.value,
                "included": "Yes",
                "default": "Yes",
            },
            {
                "key": PrisonExpensesTypeIncludesExcludes.PRISON_FACILITY.name,
                "label": PrisonExpensesTypeIncludesExcludes.PRISON_FACILITY.value,
                "included": "Yes",
                "default": "Yes",
            },
            {
                "key": PrisonExpensesTypeIncludesExcludes.OPERATIONS_AND_MAINTENANCE.name,
                "label": PrisonExpensesTypeIncludesExcludes.OPERATIONS_AND_MAINTENANCE.value,
                "included": "Yes",
                "default": "Yes",
            },
            {
                "key": PrisonExpensesTypeIncludesExcludes.CONSTRUCTION.name,
                "label": PrisonExpensesTypeIncludesExcludes.CONSTRUCTION.value,
                "included": "Yes",
                "default": "Yes",
            },
            {
                "key": PrisonExpensesTypeIncludesExcludes.TREATMENT.name,
                "label": PrisonExpensesTypeIncludesExcludes.TREATMENT.value,
                "included": "Yes",
                "default": "Yes",
            },
            {
                "key": PrisonExpensesTypeIncludesExcludes.HEALTH_CARE.name,
                "label": PrisonExpensesTypeIncludesExcludes.HEALTH_CARE.value,
                "included": "Yes",
                "default": "Yes",
            },
            {
                "key": PrisonExpensesTypeIncludesExcludes.FACILITY_STAFF.name,
                "label": PrisonExpensesTypeIncludesExcludes.FACILITY_STAFF.value,
                "included": "Yes",
                "default": "Yes",
            },
            {
                "key": PrisonExpensesTypeIncludesExcludes.ADMINISTRATION_STAFF.name,
                "label": PrisonExpensesTypeIncludesExcludes.ADMINISTRATION_STAFF.value,
                "included": "Yes",
                "default": "Yes",
            },
            {
                "key": PrisonExpensesTypeIncludesExcludes.OPERATION.name,
                "label": PrisonExpensesTypeIncludesExcludes.OPERATION.value,
                "included": "Yes",
                "default": "Yes",
            },
            {
                "key": PrisonExpensesTypeIncludesExcludes.JAIL_FACILITY.name,
                "label": PrisonExpensesTypeIncludesExcludes.JAIL_FACILITY.value,
                "included": "No",
                "default": "No",
            },
            {
                "key": PrisonExpensesTypeIncludesExcludes.JUVENILE_JAIL.name,
                "label": PrisonExpensesTypeIncludesExcludes.JUVENILE_JAIL.value,
                "included": "No",
                "default": "No",
            },
            {
                "key": PrisonExpensesTypeIncludesExcludes.NON_PRISON_ACTIVITIES.name,
                "label": PrisonExpensesTypeIncludesExcludes.NON_PRISON_ACTIVITIES.value,
                "included": "No",
                "default": "No",
            },
            {
                "key": PrisonExpensesTypeIncludesExcludes.LAW_ENFORCEMENT.name,
                "label": PrisonExpensesTypeIncludesExcludes.LAW_ENFORCEMENT.value,
                "included": "No",
                "default": "No",
            },
        ]

        # MetricInterface.from_json() expects settings field and not includes_excludes field
        self.assertEqual(
            MetricInterface.from_json(
                json=metric_interface_json,
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
            ),
            metric_interface,
        )

    def test_custom_reporting_frequency(self) -> None:
        includes_excludes_member_to_setting: Dict[
            enum.Enum, Optional[IncludesExcludesSetting]
        ] = {}
        for timeframe_member in LawEnforcementFundingTimeframeIncludesExcludes:
            includes_excludes_member_to_setting[
                timeframe_member
            ] = IncludesExcludesSetting.YES
        for purpose_member in LawEnforcementFundingPurposeIncludesExcludes:
            includes_excludes_member_to_setting[
                purpose_member
            ] = IncludesExcludesSetting.YES
        metric_interface = MetricInterface(
            key=law_enforcement.funding.key,
            is_metric_enabled=True,
            custom_reporting_frequency=CustomReportingFrequency(
                frequency=ReportingFrequency.ANNUAL, starting_month=1
            ),
            includes_excludes_member_to_setting=includes_excludes_member_to_setting,
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
            "reporting_note": law_enforcement.funding.reporting_note,
            "unit": "Amount",
            "category": law_enforcement.funding.category.human_readable_string,
            "label": law_enforcement.funding.display_name,
            "frequency": law_enforcement.funding.reporting_frequencies[0].value,
            "custom_frequency": "ANNUAL",
            "starting_month": 1,
            "filenames": ["funding", "funding_by_type"],
            "value": None,
            "disaggregations": [],
            "contexts": [],
            "includes_excludes": [
                {
                    "description": "Funding timeframe and spend-down",
                    "multiselect": True,
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
                    ],
                },
                {
                    "multiselect": True,
                    "description": "Funding purpose",
                    "settings": [
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
                            "label": "Funding for the purchase of law enforcement "
                            "equipment",
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
                },
            ],
            "is_includes_excludes_configured": None,
            "datapoints": None,
        }

        # MetricInterface.to_json() expects includes_excludes field and not settings field
        self.assertEqual(
            metric_interface.to_json(
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
            ),
            metric_interface_json,
        )

        # MetricInterface.from_json() expects settings field and not includes_excludes field
        metric_interface_json["settings"] = [
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
                "label": "Funding for the purchase of law enforcement equipment",
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
        ]

        metric_interface_json = {
            key: value
            for key, value in metric_interface_json.items()
            if key != "includes_excludes"
        }
        self.assertEqual(
            MetricInterface.from_json(
                json=metric_interface_json,
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
            ),
            metric_interface,
        )

    def test_race_and_ethnicity_json(self) -> None:
        dimension_member_to_datapoints_json: DefaultDict[
            str, List[DatapointJson]
        ] = defaultdict(list)
        for dimension_member in RaceAndEthnicity:
            dimension_member_to_datapoints_json[dimension_member.name] = [
                {
                    "dimension_display_name": dimension_member.name,
                    "disaggregation_display_name": "Race / Ethnicities",
                    "end_date": "Tue, 01 Nov 2022 00:00:00 GMT",
                    "frequency": "MONTHLY",
                    "id": 14891,
                    "is_published": True,
                    "old_value": None,
                    "report_id": 314,
                    "start_date": "Sat, 01 Oct 2022 00:00:00 GMT",
                    "value": 10,
                    "agency_name": None,
                }
            ]

        aggregated_dimension = MetricAggregatedDimensionData(
            dimension_to_value={dim: 10 for dim in RaceAndEthnicity},
            dimension_to_enabled_status={dim: True for dim in RaceAndEthnicity},
            is_breakdown_configured=None,
        )

        disaggregation_json = {
            "key": "global/race_and_ethnicity",
            "display_name": "Race / Ethnicities",
            "is_breakdown_configured": None,
            "enabled": True,
            "dimensions": [
                {
                    "datapoints": [
                        {
                            "dimension_display_name": dim.name,
                            "disaggregation_display_name": "Race / Ethnicities",
                            "end_date": "Tue, 01 Nov 2022 00:00:00 GMT",
                            "frequency": "MONTHLY",
                            "id": 14891,
                            "is_published": True,
                            "old_value": None,
                            "report_id": 314,
                            "start_date": "Sat, 01 Oct 2022 00:00:00 GMT",
                            "value": 10,
                            "agency_name": None,
                        }
                    ],
                    "enabled": True,
                    "ethnicity": dim.ethnicity,
                    "key": dim.value,
                    "label": dim.value,
                    "race": dim.race,
                    "value": 10,
                    "description": None,
                    "contexts": [
                        {
                            "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                            "display_name": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                            "value": None,
                        }
                    ],
                }
                for dim in RaceAndEthnicity
            ],
            "consolidated_race_ethnicity": {
                "descriptions": {
                    "American Indian or Alaska Native": "The number of arrests, citations, and summonses made by the agency of people whose race is listed as Native American, American Indian, Native Alaskan, or similar. This includes people with origins in the original populations or Tribal groups of North, Central, or South America.",
                    "Asian": "The number of arrests, citations, and summonses made by the agency of people whose race is listed as Asian. This includes people with origins in China, Japan, Korea, Laos, Vietnam, as well as India, Malaysia, the Philippines, and other countries in East and South Asia.",
                    "Black": "The number of arrests, citations, and summonses made by the agency of people whose race is listed as Black or African-American. This includes people with origins in Kenya, Nigeria, Ghana, Ethiopia, or other countries in Sub-Saharan Africa.",
                    "More than one race": "The number of arrests, citations, and summonses made by the agency of people whose race is listed as more than one race, such as White and Black.",
                    "Native Hawaiian or Pacific Islander": "The number of arrests, citations, and summonses made by the agency of people whose race is listed as Native Hawaiian, Pacific Islander, or similar. This includes people with origins in the original populations of Pacific islands such as Hawaii, Samoa, Fiji, Tahiti, or Papua New Guinea.",
                    "Other": "The number of arrests, citations, and summonses made by the agency of people whose race is listed as some other race, not included above.",
                    "White": "The number of arrests, citations, and summonses made by the agency of people whose race is listed as White, Caucasian, or Anglo. This includes people with origins in France, Italy, or other countries in Europe, as well as Israel, Palestine, Egypt, or other countries in the Middle East and North Africa.",
                    "Unknown": "The number of arrests, citations, and summonses made by the agency of people whose race is not known.",
                    "Hispanic or Latino": "The number of arrests, citations, and summonses made by the agency of people whose race and ethnicity are listed as Hispanic or Latino. This includes people with origins in Mexico, Cuba, Puerto Rico, the Dominican Republic, and other Spanish-speaking countries in Central or South America, as well as people with origins in Brazil or other non-Spanish-speaking countries in Central or South America.",
                },
                "data": {
                    "Hispanic or Latino": {
                        "Sat, 01 Oct 2022 00:00:00 GMT - Tue, 01 Nov 2022 00:00:00 GMT": 80
                    },
                    "American Indian or Alaska Native": {
                        "Sat, 01 Oct 2022 00:00:00 GMT - Tue, 01 Nov 2022 00:00:00 GMT": 20
                    },
                    "Asian": {
                        "Sat, 01 Oct 2022 00:00:00 GMT - Tue, 01 Nov 2022 00:00:00 GMT": 20
                    },
                    "Black": {
                        "Sat, 01 Oct 2022 00:00:00 GMT - Tue, 01 Nov 2022 00:00:00 GMT": 20
                    },
                    "More than one race": {
                        "Sat, 01 Oct 2022 00:00:00 GMT - Tue, 01 Nov 2022 00:00:00 GMT": 20
                    },
                    "Native Hawaiian or Pacific Islander": {
                        "Sat, 01 Oct 2022 00:00:00 GMT - Tue, 01 Nov 2022 00:00:00 GMT": 20
                    },
                    "White": {
                        "Sat, 01 Oct 2022 00:00:00 GMT - Tue, 01 Nov 2022 00:00:00 GMT": 20
                    },
                    "Other": {
                        "Sat, 01 Oct 2022 00:00:00 GMT - Tue, 01 Nov 2022 00:00:00 GMT": 20
                    },
                    "Unknown": {
                        "Sat, 01 Oct 2022 00:00:00 GMT - Tue, 01 Nov 2022 00:00:00 GMT": 20
                    },
                },
            },
        }

        self.assertEqual(
            aggregated_dimension.to_json(
                entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE,
                dimension_definition=AggregatedDimension(
                    dimension=RaceAndEthnicity,
                    required=True,
                    contexts=[Context(key=ContextKey.OTHER_RACE_DESCRIPTION, label="")],
                    dimension_to_description={
                        CensusRace.AMERICAN_INDIAN_ALASKAN_NATIVE: "The number of arrests, citations, and summonses made by the agency of people whose race is listed as Native American, American Indian, Native Alaskan, or similar. This includes people with origins in the original populations or Tribal groups of North, Central, or South America.",
                        CensusRace.ASIAN: "The number of arrests, citations, and summonses made by the agency of people whose race is listed as Asian. This includes people with origins in China, Japan, Korea, Laos, Vietnam, as well as India, Malaysia, the Philippines, and other countries in East and South Asia.",
                        CensusRace.BLACK: "The number of arrests, citations, and summonses made by the agency of people whose race is listed as Black or African-American. This includes people with origins in Kenya, Nigeria, Ghana, Ethiopia, or other countries in Sub-Saharan Africa.",
                        CensusRace.MORE_THAN_ONE_RACE: "The number of arrests, citations, and summonses made by the agency of people whose race is listed as more than one race, such as White and Black.",
                        CensusRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER: "The number of arrests, citations, and summonses made by the agency of people whose race is listed as Native Hawaiian, Pacific Islander, or similar. This includes people with origins in the original populations of Pacific islands such as Hawaii, Samoa, Fiji, Tahiti, or Papua New Guinea.",
                        CensusRace.OTHER: "The number of arrests, citations, and summonses made by the agency of people whose race is listed as some other race, not included above.",
                        CensusRace.WHITE: "The number of arrests, citations, and summonses made by the agency of people whose race is listed as White, Caucasian, or Anglo. This includes people with origins in France, Italy, or other countries in Europe, as well as Israel, Palestine, Egypt, or other countries in the Middle East and North Africa.",
                        CensusRace.UNKNOWN: "The number of arrests, citations, and summonses made by the agency of people whose race is not known.",
                        CensusRace.HISPANIC_OR_LATINO: "The number of arrests, citations, and summonses made by the agency of people whose race and ethnicity are listed as Hispanic or Latino. This includes people with origins in Mexico, Cuba, Puerto Rico, the Dominican Republic, and other Spanish-speaking countries in Central or South America, as well as people with origins in Brazil or other non-Spanish-speaking countries in Central or South America.",
                    },
                ),
                dimension_member_to_datapoints_json=dimension_member_to_datapoints_json,
                is_v2=True,
            ),
            disaggregation_json,
        )

    def test_get_supervision_metrics(self) -> None:
        # We should always grab all copies of supervision metrics
        # for each system. Which ones are actually enabled will be
        # determined later.

        # get metric definitions for report
        supervision_metrics = MetricInterface.get_metric_definitions_by_systems(
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

    def test_apply_invariant_for_supervision_subsystem_metric(self) -> None:
        """If this is a supervision subsystem metric, and the metric is not supposed to
        be disaggregated by supervision subsystems, the metric must be disabled.
        """
        metric_interface = MetricInterface(
            key="PAROLE_EXPENSES",
            is_metric_enabled=None,
            disaggregated_by_supervision_subsystems=False,
        )
        metric_interface.post_process_storage_json()
        self.assertEqual(metric_interface.is_metric_enabled, False)

    def test_apply_invariant_for_supervision_metric(self) -> None:
        """A supervision metric interface's disaggregated_by_supervision_subsystems
        field cannot be None.
        """
        metric_interface = MetricInterface(
            key="SUPERVISION_EXPENSES",
            disaggregated_by_supervision_subsystems=None,
        )
        metric_interface.post_process_storage_json()
        self.assertEqual(
            metric_interface.disaggregated_by_supervision_subsystems, False
        )

    def test_dashboard_v2_endpoint(self) -> None:
        reported_metric = self.test_schema_objects.get_arrests_metric()
        reported_metric.reporting_agency_id = self.prison_super_agency_id
        with SessionFactory.using_database(self.database_key) as session:
            prison_super_agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=self.prison_super_agency_id
            )
            AgencySettingInterface.create_or_update_agency_setting(
                session=session,
                agency_id=self.prison_super_agency_id,
                setting_type=schema.AgencySettingType.HOMEPAGE_URL,
                value="test.org",
            )
            self.assertEqual(
                reported_metric.to_json(
                    entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
                    is_v2=True,
                    reporting_agency_id_to_agency={
                        self.prison_super_agency_id: prison_super_agency
                    },
                ),
                {
                    "additional_description": law_enforcement.arrests.additional_description,
                    "category": "Operations and Dynamics",
                    "contexts": [],
                    "custom_frequency": None,
                    "datapoints": None,
                    "description": "The number of arrests, citations, and summonses made by the agency.",
                    "disaggregated_by_supervision_subsystems": None,
                    "disaggregations": [
                        {
                            "dimensions": [
                                {
                                    "contexts": [
                                        {
                                            "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                            "display_name": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                                            "value": None,
                                        }
                                    ],
                                    "datapoints": None,
                                    "description": "The number of arrests, citations, or summonses made by the agency in which the most serious offense was a drug offense.",
                                    "enabled": True,
                                    "includes_excludes": [
                                        {
                                            "description": None,
                                            "multiselect": True,
                                            "settings": [
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "DRUG_VIOLATIONS",
                                                    "label": "Drug/narcotic violations",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "DRUG_EQUIPMENT_VIOLATIONS",
                                                    "label": "Drug equipment violations",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "DRUG_SALES",
                                                    "label": "Drug sales",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "DRUG_DISTRIBUTION",
                                                    "label": "Drug distribution",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "DRUG_MANUFACTURING",
                                                    "label": "Drug manufacturing",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "DRUG_SMUGGLING",
                                                    "label": "Drug smuggling",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "DRUG_PRODUCTION",
                                                    "label": "Drug production",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "DRUG_POSSESSION",
                                                    "label": "Drug possession",
                                                },
                                            ],
                                        }
                                    ],
                                    "is_dimension_includes_excludes_configured": None,
                                    "key": "Drug Offenses",
                                    "label": "Drug Offenses",
                                },
                                {
                                    "contexts": [
                                        {
                                            "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                            "display_name": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                                            "value": None,
                                        }
                                    ],
                                    "datapoints": None,
                                    "description": "The number of arrests, citations, or summonses made by the agency in which the most serious offense was a crime against a person.",
                                    "enabled": True,
                                    "includes_excludes": [
                                        {
                                            "description": None,
                                            "multiselect": True,
                                            "settings": [
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "AGGRAVATED_ASSAULT",
                                                    "label": "Aggravated assault",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "SIMPLE_ASSAULT",
                                                    "label": "Simple assault",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "INTIMIDATION",
                                                    "label": "Intimidation",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "MURDER",
                                                    "label": "Murder and nonnegligent manslaughter",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "MANSLAUGHTER",
                                                    "label": "Negligent manslaughter",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "HUMAN_TRAFFICKING_COMMERCIAL",
                                                    "label": "Human trafficking, commercial sex acts",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "HUMAN_TRAFFICKING_INVOLUNTARY",
                                                    "label": "Human trafficking, involuntary servitude",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "KIDNAPPING",
                                                    "label": "Kidnapping/abduction",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "RAPE",
                                                    "label": "Rape",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "SODOMY",
                                                    "label": "Sodomy",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "SEXUAL_ASSAULT",
                                                    "label": "Sexual assault with an object",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "FONDLING",
                                                    "label": "Fondling",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "INCEST",
                                                    "label": "Incest",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "STATUTORY_RAPE",
                                                    "label": "Statutory rape",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "ROBBERY",
                                                    "label": "Robbery",
                                                },
                                                {
                                                    "default": "No",
                                                    "included": None,
                                                    "key": "JUSTIFIABLE_HOMICIDE",
                                                    "label": "Justifiable homicide",
                                                },
                                            ],
                                        }
                                    ],
                                    "is_dimension_includes_excludes_configured": None,
                                    "key": "Person Offenses",
                                    "label": "Person Offenses",
                                },
                                {
                                    "contexts": [
                                        {
                                            "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                            "display_name": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                                            "value": None,
                                        }
                                    ],
                                    "datapoints": None,
                                    "description": "The number of arrests, citations, or summonses made by the agency in which the most serious offense was a property offense.",
                                    "enabled": True,
                                    "includes_excludes": [
                                        {
                                            "description": None,
                                            "multiselect": True,
                                            "settings": [
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "ARSON",
                                                    "label": "Arson",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "BRIBERY",
                                                    "label": "Bribery",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "BURGLARY",
                                                    "label": "Burglary/breaking and entering",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "COUNTERFEITING",
                                                    "label": "Counterfeiting/forgery",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "VANDALISM",
                                                    "label": "Destruction/damage/vandalism of property",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "EMBEZZLEMENT",
                                                    "label": "Embezzlement",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "EXTORTION",
                                                    "label": "Extortion/blackmail",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "FALSE_PRETENSES",
                                                    "label": "False pretenses/swindle/confidence game",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "CREDIT_CARD",
                                                    "label": "Credit card/automated teller machine fraud",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "IMPERSONATION",
                                                    "label": "Impersonation",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "WELFARE_FRAUD",
                                                    "label": "Welfare fraud",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "WIRE_FRAUD",
                                                    "label": "Wire fraud",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "IDENTITY_THEFT",
                                                    "label": "Identity theft",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "HACKING",
                                                    "label": "Hacking/computer invasion",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "POCKET_PICKING",
                                                    "label": "Pocket-picking",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "PURSE_SNATCHING",
                                                    "label": "Purse-snatching",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "SHOPLIFTING",
                                                    "label": "Shoplifting",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "THEFT_FROM_BULIDING",
                                                    "label": "Theft from building",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "THEFT_FROM_MACHINE",
                                                    "label": "Theft from coin-operated machine or device",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "THEFT_FROM_VEHICLE",
                                                    "label": "Theft from motor vehicle",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "THEFT_OF_VEHICLE_PARTS",
                                                    "label": "Theft of motor vehicle parts or accessories",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "LARCENY",
                                                    "label": "All other larceny",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "THEFT_OF_VEHICLE",
                                                    "label": "Motor vehicle theft",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "STOLEN_PROPERTY",
                                                    "label": "Stolen property offenses",
                                                },
                                                {
                                                    "default": "No",
                                                    "included": None,
                                                    "key": "ROBBERY",
                                                    "label": "Robbery",
                                                },
                                            ],
                                        }
                                    ],
                                    "is_dimension_includes_excludes_configured": None,
                                    "key": "Property Offenses",
                                    "label": "Property Offenses",
                                },
                                {
                                    "contexts": [
                                        {
                                            "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                            "display_name": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                                            "value": None,
                                        }
                                    ],
                                    "datapoints": None,
                                    "description": "The number of arrests, citations, or summonses made by the agency in which the most serious offense was a public order offense.",
                                    "enabled": True,
                                    "includes_excludes": [
                                        {
                                            "description": None,
                                            "multiselect": True,
                                            "settings": [
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "ANIMAL_CRUELTY",
                                                    "label": "Animal cruelty",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "IMPORT_VIOLATIONS",
                                                    "label": "Import violations",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "EXPORT_VIOLATIONS",
                                                    "label": "Export violations",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "LIQUOR",
                                                    "label": "Federal liquor offenses",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "TOBACCO",
                                                    "label": "Federal tobacco offenses",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "WILDLIFE",
                                                    "label": "Wildlife trafficking",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "ESPIONAGE",
                                                    "label": "Espionage",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "MONEY_LAUNDERING",
                                                    "label": "Money laundering",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "HARBORING",
                                                    "label": "Harboring escapee/concealing from arrest",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "FLIGHT_PROSECUTION",
                                                    "label": "Flight to avoid prosecution",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "FLIGHT_DEPORTATION",
                                                    "label": "Flight to avoid deportation",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "BETTING",
                                                    "label": "Betting/wagering",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "GAMBLING",
                                                    "label": "Operating/promoting/assisting gambling",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "GAMBLING_EQUIPMENT",
                                                    "label": "Gambling equipment violations",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "SPORTS_TAMPERING",
                                                    "label": "Sports tampering",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "ILLEGAL_ENTRY",
                                                    "label": "Illegal entry into the United States",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "FALSE_CITIZENSHIP",
                                                    "label": "False citizenship",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "SMUGGLING",
                                                    "label": "Smuggling aliens",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "RENTRY",
                                                    "label": "Re-entry after deportation",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "PORNOGRAPHY",
                                                    "label": "Pornography/obscene material",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "PROSTITUTION",
                                                    "label": "Prostitution",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "ASSISTING_PROSTITUTION",
                                                    "label": "Assisting or promoting prostitution",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "PURCHASING_PROSTITUTION",
                                                    "label": "Purchasing prostitution",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "TREASON",
                                                    "label": "Treason",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "WEAPON_LAW_VIOLATIONS",
                                                    "label": "Weapon law violations",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "FIREARM_VIOLATIONS",
                                                    "label": "Violation of National Firearm Act of 1934",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "WEAPONS_OF_MASS_DESTRUCTION",
                                                    "label": "Weapons of mass destruction",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "EXPLOSIVES",
                                                    "label": "Explosives",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "FAILURE_TO_APPEAR",
                                                    "label": "Failure to appear",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "CURFEW",
                                                    "label": "Curfew/loitering/vagrancy violations",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "DISORDERLY_CONDUCT",
                                                    "label": "Disorderly conduct",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "DUI",
                                                    "label": "Driving under the influence",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "FAMILY_OFFENSES",
                                                    "label": "Family offenses, nonviolent",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "FEDERAL_RESOURCE_VIOLATIONS",
                                                    "label": "Federal resource violations",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "LIQUOR_LAW_VIOLATIONS",
                                                    "label": "Liquor law violations",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "PERJURY",
                                                    "label": "Perjury",
                                                },
                                                {
                                                    "default": "Yes",
                                                    "included": None,
                                                    "key": "TRESPASS",
                                                    "label": "Trespass of real property",
                                                },
                                                {
                                                    "default": "No",
                                                    "included": None,
                                                    "key": "DRUG_VIOLATIONS",
                                                    "label": "Drug/narcotic violations",
                                                },
                                                {
                                                    "default": "No",
                                                    "included": None,
                                                    "key": "DRUG_EQUIPMENT_VIOLATIONS",
                                                    "label": "Drug equipment violations",
                                                },
                                                {
                                                    "default": "No",
                                                    "included": None,
                                                    "key": "DRUG_SALES",
                                                    "label": "Drug sales",
                                                },
                                                {
                                                    "default": "No",
                                                    "included": None,
                                                    "key": "DRUG_DISTRIBUTION",
                                                    "label": "Drug distribution",
                                                },
                                                {
                                                    "default": "No",
                                                    "included": None,
                                                    "key": "DRUG_MANUFACTURING",
                                                    "label": "Drug manufacturing",
                                                },
                                                {
                                                    "default": "No",
                                                    "included": None,
                                                    "key": "DRUG_SMUGGLING",
                                                    "label": "Drug smuggling",
                                                },
                                                {
                                                    "default": "No",
                                                    "included": None,
                                                    "key": "DRUG_PRODUCTION",
                                                    "label": "Drug production",
                                                },
                                                {
                                                    "default": "No",
                                                    "included": None,
                                                    "key": "DRUG_POSSESSION",
                                                    "label": "Drug possession",
                                                },
                                            ],
                                        }
                                    ],
                                    "is_dimension_includes_excludes_configured": None,
                                    "key": "Public Order Offenses",
                                    "label": "Public Order Offenses",
                                },
                                {
                                    "contexts": [
                                        {
                                            "key": "ADDITIONAL_CONTEXT",
                                            "display_name": "Please describe what data is being included in this breakdown.",
                                            "value": None,
                                        }
                                    ],
                                    "datapoints": None,
                                    "description": "The number of arrests, citations, or summonses made by the agency in which the most serious offense is not known.",
                                    "enabled": True,
                                    "includes_excludes": [],
                                    "is_dimension_includes_excludes_configured": None,
                                    "key": "Unknown Offenses",
                                    "label": "Unknown Offenses",
                                },
                                {
                                    "contexts": [
                                        {
                                            "key": "ADDITIONAL_CONTEXT",
                                            "display_name": "Please describe what data is being included in this breakdown.",
                                            "value": None,
                                        }
                                    ],
                                    "datapoints": None,
                                    "description": "The number of arrests, citations, or summonses made by the agency in which the most serious offense was another type of crime that was not a person, property, drug, or public order offense.",
                                    "enabled": True,
                                    "includes_excludes": [],
                                    "is_dimension_includes_excludes_configured": None,
                                    "key": "Other Offenses",
                                    "label": "Other Offenses",
                                },
                            ],
                            "is_breakdown_configured": None,
                            "display_name": "Offense Types",
                            "enabled": True,
                            "key": "metric/offense/type",
                        }
                    ],
                    "display_name": "Arrests",
                    "enabled": True,
                    "frequency": "MONTHLY",
                    "includes_excludes": [
                        {
                            "description": None,
                            "multiselect": True,
                            "settings": [
                                {
                                    "default": "Yes",
                                    "included": None,
                                    "key": "ON_VIEW",
                                    "label": "On-view arrest (i.e., apprehension without a warrant or previous incident report)",
                                },
                                {
                                    "default": "Yes",
                                    "included": None,
                                    "key": "WARRANT",
                                    "label": "Arrests for warrants or previous incident reports",
                                },
                                {
                                    "default": "Yes",
                                    "included": None,
                                    "key": "CITATION",
                                    "label": "Summonses or citations",
                                },
                                {
                                    "default": "Yes",
                                    "included": None,
                                    "key": "IN_JURISDICTION",
                                    "label": "Arrests made for offenses committed within the agency’s jurisdiction",
                                },
                                {
                                    "default": "No",
                                    "included": None,
                                    "key": "OUTSIDE_JURISDICTION",
                                    "label": "Arrests made for offenses committed outside the agency’s jurisdiction",
                                },
                            ],
                        }
                    ],
                    "is_includes_excludes_configured": None,
                    "key": "LAW_ENFORCEMENT_ARRESTS",
                    "reporting_agency_id": self.prison_super_agency_id,
                    "reporting_agency_name": "Super Agency Prison",
                    "reporting_agency_url": "test.org",
                    "reporting_agency_category": "SUPER_AGENCY",
                    "is_self_reported": None,
                    "sector": {
                        "display_name": "Law Enforcement",
                        "key": "LAW_ENFORCEMENT",
                    },
                    "starting_month": None,
                    "unit": "Arrest Events",
                },
            )
