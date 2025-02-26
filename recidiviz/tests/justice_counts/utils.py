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
"""Implements tests for the Justice Counts Control Panel backend API."""

import datetime
from typing import Any, Dict, Optional
from unittest import TestCase

import pytest
from sqlalchemy.engine import Engine

from recidiviz.auth.auth0_client import Auth0User, JusticeCountsAuth0AppMetadata
from recidiviz.common.constants.justice_counts import ContextKey
from recidiviz.justice_counts.dimensions.law_enforcement import (
    CallType,
    ForceType,
    OffenseType,
    SheriffBudgetType,
)
from recidiviz.justice_counts.dimensions.person import RaceAndEthnicity
from recidiviz.justice_counts.metrics import law_enforcement
from recidiviz.justice_counts.metrics.metric_definition import CallsRespondedOptions
from recidiviz.justice_counts.metrics.report_metric import (
    ReportedAggregatedDimension,
    ReportedContext,
    ReportMetric,
)
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.justice_counts.control_panel.load_fixtures import (
    reset_justice_counts_fixtures,
)
from recidiviz.tools.postgres import local_postgres_helpers


@pytest.mark.uses_db
class JusticeCountsDatabaseTestCase(TestCase):
    """Base class for unit tests that act on the Justice Counts database."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS)
        self.env_vars = (
            local_postgres_helpers.update_local_sqlalchemy_postgres_env_vars()
        )

        # Auto-generate all tables that exist in our schema in this database
        self.engine = self.get_engine()
        self.database_key.declarative_meta.metadata.create_all(self.engine)
        self.maxDiff = None

    def get_engine(self) -> Engine:
        """Return the Engine that this test class should use to connect to
        the database. By default, initialize a new engine. Subclasses can
        override this method to point to an engine that already exists."""
        return SQLAlchemyEngineManager.init_engine_for_postgres_instance(
            database_key=self.database_key,
            db_url=local_postgres_helpers.on_disk_postgres_db_url(),
        )

    def load_fixtures(self) -> None:
        reset_justice_counts_fixtures(self.engine)

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)
        local_postgres_helpers.restore_local_env_vars(self.env_vars)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )


class JusticeCountsSchemaTestObjects:
    """Class for test schema objects"""

    def __init__(self) -> None:
        # Agencies
        self.test_agency_A = schema.Agency(
            name="Agency Alpha",
            state_code="US_XX",
            fips_county_code="us_ak_anchorage",
            system=schema.System.LAW_ENFORCEMENT,
        )
        self.test_agency_B = schema.Agency(
            name="Agency Beta",
            state_code="US_XX",
            fips_county_code="us_ca_san_francisco",
            system=schema.System.LAW_ENFORCEMENT,
        )
        self.test_agency_C = schema.Agency(
            name="Agency Gamma",
            state_code="US_XX",
            fips_county_code="us_ca_san_francisco",
            system=schema.System.SUPERVISION,
        )
        self.test_agency_D = schema.Agency(
            name="Agency Delta",
            state_code="US_XX",
            fips_county_code="us_ca_san_francisco",
            system=schema.System.PAROLE,
        )
        self.test_agency_E = schema.Agency(
            name="Agency Epsilon",
            state_code="US_XX",
            fips_county_code="us_ca_san_francisco",
            systems=[schema.System.PAROLE.value, schema.System.PROBATION.value],
        )
        self.test_agency_F = schema.Agency(
            name="Agency Gamma",
            state_code="US_XX",
            fips_county_code="us_ca_san_francisco",
            systems=[schema.System.PROSECUTION.value],
        )

        # Auth0 Users
        self.test_auth0_user: Auth0User = {
            "email": "userA@fake.com",
            "user_id": "auth0_id_A",
            "name": "Jane Doe",
            "app_metadata": JusticeCountsAuth0AppMetadata(
                agency_ids=[], has_seen_onboarding={}
            ),
        }

        # Users
        self.test_user_A = schema.UserAccount(
            name="Jane Doe",
            auth0_user_id="auth0_id_A",
        )
        self.test_user_B = schema.UserAccount(
            name="John Doe",
            auth0_user_id="auth0_id_B",
        )
        self.test_user_C = schema.UserAccount(
            name="John Smith",
            auth0_user_id="auth0_id_C",
        )

        # Reports
        self.test_report_monthly = schema.Report(
            source=self.test_agency_A,
            type="MONTHLY",
            instance="generated_instance_id",
            status=schema.ReportStatus.NOT_STARTED,
            date_range_start=datetime.date.fromisoformat("2022-06-01"),
            date_range_end=datetime.date.fromisoformat("2022-07-01"),
            project=schema.Project.JUSTICE_COUNTS_CONTROL_PANEL,
            acquisition_method=schema.AcquisitionMethod.CONTROL_PANEL,
            created_at=datetime.date.fromisoformat("2022-05-30"),
        )
        self.test_report_annual = schema.Report(
            source=self.test_agency_B,
            type="ANNUAL",
            instance="generated_instance_id",
            status=schema.ReportStatus.DRAFT,
            date_range_start=datetime.date.fromisoformat("2022-01-01"),
            date_range_end=datetime.date.fromisoformat("2023-01-01"),
            modified_by=[self.test_user_B.id],
            project=schema.Project.JUSTICE_COUNTS_CONTROL_PANEL,
            acquisition_method=schema.AcquisitionMethod.CONTROL_PANEL,
            last_modified_at=datetime.datetime.fromisoformat("2022-07-05T08:00:00"),
            created_at=datetime.date.fromisoformat("2021-12-30"),
        )
        self.test_report_supervision = self.get_report_for_agency(
            agency=self.test_agency_C
        )
        self.test_report_parole = self.get_report_for_agency(agency=self.test_agency_D)
        self.test_report_parole_probation = self.get_report_for_agency(
            agency=self.test_agency_E
        )

        # Metrics
        self.reported_budget_metric = (
            JusticeCountsSchemaTestObjects.get_reported_budget_metric()
        )
        self.reported_calls_for_service_metric = (
            JusticeCountsSchemaTestObjects.get_reported_calls_for_service_metric()
        )
        self.reported_residents_metric = ReportMetric(
            key=law_enforcement.residents.key,
            value=5000,
            aggregated_dimensions=[
                ReportedAggregatedDimension(
                    dimension_to_value={
                        RaceAndEthnicity.AMERICAN_INDIAN_ALASKAN_NATIVE: 100,
                        RaceAndEthnicity.ASIAN: 100,
                        RaceAndEthnicity.BLACK: 1000,
                        RaceAndEthnicity.EXTERNAL_UNKNOWN: 0,
                        RaceAndEthnicity.HISPANIC: 50,
                        RaceAndEthnicity.NATIVE_HAWAIIAN_PACIFIC_ISLANDER: 0,
                        RaceAndEthnicity.OTHER: 100,
                        RaceAndEthnicity.WHITE: 1000,
                    }
                )
            ],
        )

    @staticmethod
    def get_report_for_agency(
        agency: schema.Agency, frequency: str = "MONTHLY"
    ) -> schema.Report:
        return schema.Report(
            source=agency,
            type=frequency,
            instance="generated_instance_id",
            status=schema.ReportStatus.NOT_STARTED,
            date_range_start=datetime.date.fromisoformat("2022-06-01"),
            date_range_end=datetime.date.fromisoformat("2022-07-01"),
            project=schema.Project.JUSTICE_COUNTS_CONTROL_PANEL,
            acquisition_method=schema.AcquisitionMethod.CONTROL_PANEL,
            created_at=datetime.date.fromisoformat("2022-05-30"),
        )

    @staticmethod
    def get_reported_budget_metric(
        value: Optional[int] = 100000,
        detention_value: Optional[int] = 66666,
        patrol_value: Optional[int] = 33334,
        include_contexts: bool = True,
        include_disaggregations: bool = True,
        nullify_contexts_and_disaggregations: bool = False,
    ) -> ReportMetric:
        return ReportMetric(
            key=law_enforcement.annual_budget.key,
            value=value,
            contexts=[
                ReportedContext(
                    key=ContextKey.PRIMARY_FUNDING_SOURCE,
                    value="government"
                    if not nullify_contexts_and_disaggregations
                    else None,
                ),
                ReportedContext(
                    key=ContextKey.ADDITIONAL_CONTEXT,
                    value=None,
                ),
            ]
            if include_contexts
            else [],
            aggregated_dimensions=[
                ReportedAggregatedDimension(
                    dimension_to_value={
                        SheriffBudgetType.DETENTION: detention_value
                        if not nullify_contexts_and_disaggregations
                        else None,
                        SheriffBudgetType.PATROL: patrol_value
                        if not nullify_contexts_and_disaggregations
                        else None,
                    }
                )
            ]
            if include_disaggregations
            else [],
        )

    @staticmethod
    def get_agency_datapoints(
        use_reenabled_breakdown: bool = False,
        include_contexts: bool = False,
        use_disabled_disaggregation: bool = False,
        use_partially_disabled_disaggregation: bool = False,
        is_metric_enabled: bool = True,
    ) -> Dict[str, Any]:
        """Returns metric dictionaries that are formatted in /metrics/update [POST] style"""
        metric_json = {
            "key": law_enforcement.annual_budget.key,
            "enabled": is_metric_enabled,
        }
        if use_partially_disabled_disaggregation:
            metric_json = {
                "key": law_enforcement.calls_for_service.key,
                "enabled": True,
                "disaggregations": [
                    {
                        "enabled": True,
                        "key": CallType.dimension_identifier(),
                        "dimensions": {
                            CallType.UNKNOWN.name: False,
                            CallType.EMERGENCY.name: False,
                        },
                    }
                ],
            }
        if use_disabled_disaggregation:
            metric_json = {
                "key": law_enforcement.officer_use_of_force_incidents.key,
                "enabled": True,
                "disaggregations": [
                    {
                        "enabled": False,
                        "key": ForceType.dimension_identifier(),
                    }
                ],
            }
        if use_reenabled_breakdown:
            metric_json = {
                "key": law_enforcement.calls_for_service.key,
                "enabled": True,
                "disaggregations": [
                    {
                        "enabled": True,
                        "key": CallType.dimension_identifier(),
                        "dimensions": {CallType.UNKNOWN.name: True},
                    }
                ],
            }

        if include_contexts:
            metric_json["contexts"] = [
                {
                    "key": ContextKey.ADDITIONAL_CONTEXT.name,
                    "value": "this additional context provides contexts",
                }
            ]

        return metric_json

    @staticmethod
    def get_agency_datapoints_request(
        agency_id: int,
    ) -> Dict[str, Any]:
        return {
            "agency_id": agency_id,
            "metrics": [
                JusticeCountsSchemaTestObjects.get_agency_datapoints(
                    include_contexts=True, use_disabled_disaggregation=True
                )
            ],
        }

    @staticmethod
    def get_reported_calls_for_service_metric(
        agencies_available_for_response: Optional[str] = None,
    ) -> ReportMetric:
        return ReportMetric(
            key=law_enforcement.calls_for_service.key,
            value=100,
            contexts=[
                ReportedContext(
                    key=ContextKey.ALL_CALLS_OR_CALLS_RESPONDED,
                    value=CallsRespondedOptions.ALL_CALLS.value,
                ),
                ReportedContext(
                    key=ContextKey.AGENCIES_AVAILABLE_FOR_RESPONSE,
                    value=agencies_available_for_response,
                ),
                ReportedContext(
                    key=ContextKey.ADDITIONAL_CONTEXT,
                    value=None,
                ),
            ],
            aggregated_dimensions=[
                ReportedAggregatedDimension(
                    dimension_to_value={
                        CallType.EMERGENCY: 20,
                        CallType.NON_EMERGENCY: 60,
                        CallType.UNKNOWN: 20,
                    }
                )
            ],
        )

    @staticmethod
    def get_total_arrests_metric() -> ReportMetric:
        return ReportMetric(
            key=law_enforcement.total_arrests.key,
            value=120,
            contexts=[
                ReportedContext(
                    key=ContextKey.JURISDICTION_DEFINITION_OF_ARREST,
                    value="it is an arrest",
                ),
                ReportedContext(
                    key=ContextKey.ADDITIONAL_CONTEXT,
                    value="this is a test for additional context",
                ),
            ],
            aggregated_dimensions=[
                ReportedAggregatedDimension(
                    dimension_to_value={
                        OffenseType.DRUG: 60,
                        OffenseType.PERSON: 10,
                        OffenseType.PROPERTY: 40,
                        OffenseType.UNKNOWN: 10,
                    }
                )
            ],
        )

    @staticmethod
    def get_civilian_complaints_sustained_metric() -> ReportMetric:
        return ReportMetric(
            key=law_enforcement.civilian_complaints_sustained.key,
            value=30,
            contexts=[
                ReportedContext(
                    key=ContextKey.ADDITIONAL_CONTEXT,
                    value=None,
                ),
            ],
        )
