# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""This class implements tests for Outliers api routes."""
import os
from datetime import datetime
from http import HTTPStatus
from typing import Callable, Optional
from unittest import TestCase, mock
from unittest.mock import MagicMock, call, patch

import pytest
from flask import Flask
from flask.testing import FlaskClient

from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.case_triage.outliers.outliers_authorization import (
    on_successful_authorization,
)
from recidiviz.case_triage.outliers.outliers_routes import create_outliers_api_blueprint
from recidiviz.outliers.constants import (
    INCARCERATION_STARTS_AND_INFERRED,
    VIOLATION_RESPONSES,
)
from recidiviz.outliers.types import (
    OutliersBackendConfig,
    OutliersClientEventConfig,
    OutliersMetricConfig,
    OutliersProductConfiguration,
    PersonName,
    SupervisionOfficerEntity,
    SupervisionOfficerSupervisorEntity,
)
from recidiviz.persistence.database.schema.insights.schema import (
    SupervisionClientEvent,
    SupervisionClients,
    SupervisionOfficerSupervisor,
)
from recidiviz.persistence.database.schema.outliers.schema import Configuration
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.outliers.querier_test import (
    TEST_CLIENT_EVENT_1,
    TEST_METRIC_3,
    load_model_from_csv_fixture,
    load_model_from_json_fixture,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.utils.metadata import local_project_id_override

TEST_METRIC_1 = OutliersMetricConfig.build_from_metric(
    metric=INCARCERATION_STARTS_AND_INFERRED,
    title_display_name="Incarceration Rate (CPVs & TPVs)",
    body_display_name="incarceration rate",
    event_name="incarcerations",
    event_name_singular="incarceration",
    event_name_past_tense="were incarcerated",
    description_markdown="""Incarceration rate description

<br />
Incarceration rate denominator description""",
)

TEST_CLIENT_EVENT = OutliersClientEventConfig.build(
    event=VIOLATION_RESPONSES, display_name="Sanctions"
)


@pytest.mark.usefixtures("snapshottest_snapshot")
class OutliersBlueprintTestCase(TestCase):
    """Base class for Outliers flask tests"""

    mock_authorization_handler: MagicMock
    test_app: Flask
    test_client: FlaskClient

    def setUp(self) -> None:
        self.mock_authorization_handler = MagicMock()

        self.auth_patcher = mock.patch(
            f"{create_outliers_api_blueprint.__module__}.build_authorization_handler",
            return_value=self.mock_authorization_handler,
        )

        self.auth_patcher.start()

        self.test_app = Flask(__name__)
        register_error_handlers(self.test_app)
        self.test_app.register_blueprint(
            create_outliers_api_blueprint(), url_prefix="/outliers"
        )
        self.test_client = self.test_app.test_client()

    def tearDown(self) -> None:
        self.auth_patcher.stop()

    @staticmethod
    def auth_side_effect(
        state_code: str,
        external_id: Optional[str] = "A1B2",
        allowed_states: Optional[list[str]] = None,
        outliers_routes_enabled: Optional[bool] = True,
        can_access_all_supervisors: Optional[bool] = False,
        pseudonymized_id: Optional[str] = None,
    ) -> Callable:
        if allowed_states is None:
            allowed_states = []

        return lambda: on_successful_authorization(
            {
                f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata": {
                    "stateCode": f"{state_code.lower()}",
                    "allowedStates": allowed_states,
                    "externalId": external_id,
                    "routes": {
                        "insights": outliers_routes_enabled,
                        "insights_supervision_supervisors-list": can_access_all_supervisors,
                    },
                    "pseudonymizedId": pseudonymized_id,
                },
            }
        )


@pytest.mark.uses_db
class TestOutliersRoutes(OutliersBlueprintTestCase):
    """Implements tests for the outliers routes."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        super().setUp()

        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="recidiviz", allowed_states=["US_ID", "US_PA"]
        )

        self.old_auth_claim_namespace = os.environ.get("AUTH0_CLAIM_NAMESPACE", None)
        os.environ["AUTH0_CLAIM_NAMESPACE"] = "https://recidiviz-test"

        self.outliers_database_key = SQLAlchemyDatabaseKey(
            SchemaType.OUTLIERS, db_name="us_pa"
        )
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.outliers_database_key
        )

        self.insights_database_key = SQLAlchemyDatabaseKey(
            SchemaType.INSIGHTS, db_name="us_pa"
        )
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.insights_database_key
        )

        with SessionFactory.using_database(self.outliers_database_key) as session:
            for config in load_model_from_csv_fixture(Configuration):
                session.add(Configuration(**config))

        with SessionFactory.using_database(self.insights_database_key) as session:
            for supervisor in load_model_from_json_fixture(
                SupervisionOfficerSupervisor
            ):
                session.add(SupervisionOfficerSupervisor(**supervisor))
            for event in load_model_from_json_fixture(SupervisionClientEvent):
                session.add(SupervisionClientEvent(**event))
            for client in load_model_from_json_fixture(SupervisionClients):
                session.add(SupervisionClients(**client))

    def tearDown(self) -> None:
        super().tearDown()

        if self.old_auth_claim_namespace:
            os.environ["AUTH0_CLAIM_NAMESPACE"] = self.old_auth_claim_namespace
        else:
            os.unsetenv("AUTH0_CLAIM_NAMESPACE")

        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.outliers_database_key
        )

        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.insights_database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_get_state_configuration_failure(self) -> None:
        # State is not enabled
        response = self.test_client.get(
            "/outliers/US_WY/configuration", headers={"Origin": "http://localhost:3000"}
        )
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            (response.get_json() or {}).get("description"),
            "This product is not enabled for this state",
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_product_configuration",
    )
    def test_get_state_configuration_success(self, mock_config: MagicMock) -> None:
        mock_config.return_value = OutliersProductConfiguration(
            updated_at=datetime(2024, 1, 1),
            updated_by="alexa@recidiviz.org",
            feature_variant=None,
            supervision_district_label="district",
            supervision_district_manager_label="district manager",
            supervision_jii_label="client",
            supervision_unit_label="unit",
            supervision_supervisor_label="supervisor",
            metrics=[TEST_METRIC_1, TEST_METRIC_3],
            client_events=[TEST_CLIENT_EVENT],
            supervision_officer_label="officer",
            learn_more_url="https://recidiviz.org",
            none_are_outliers_label="label1",
            worse_than_rate_label="label2",
            exclusion_reason_description="description",
            at_or_above_rate_label="label3",
        )

        response = self.test_client.get(
            "/outliers/us_id/configuration",
            headers={"Origin": "http://localhost:3000"},
        )

        self.snapshot.assert_match(response.json, name="test_get_state_configuration_success")  # type: ignore[attr-defined]
        self.assertEqual(HTTPStatus.OK, response.status_code)

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_supervisor_entities",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_supervisors(
        self,
        mock_enabled_states: MagicMock,
        mock_get_supervision_officer_entities: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="1", can_access_all_supervisors=True
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_get_supervision_officer_entities.return_value = [
            SupervisionOfficerSupervisorEntity(
                full_name=PersonName(
                    given_names="Supervisor",
                    surname="2",
                    middle_names=None,
                    name_suffix=None,
                ),
                external_id="102",
                pseudonymized_id="hash2",
                supervision_district="2",
                email="supervisor2@recidiviz.org",
                has_outliers=True,
            ),
        ]

        response = self.test_client.get(
            "/outliers/US_PA/supervisors",
            headers={"Origin": "http://localhost:3000"},
        )

        self.snapshot.assert_match(response.json, name="test_get_supervisors")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_supervisors_unauthorized(
        self,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="1", pseudonymized_id="hash-1"
        )

        mock_enabled_states.return_value = ["US_PA"]

        response = self.test_client.get(
            "/outliers/US_PA/supervisors",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(
            response.json,
            {
                "message": "User hash-1 is not authorized to access the /supervisors endpoint"
            },
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_officers_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_officers_for_supervisor(
        self,
        mock_enabled_states: MagicMock,
        mock_get_outliers: MagicMock,
        mock_get_supervisor: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_pa")
        mock_enabled_states.return_value = ["US_PA"]

        with SessionFactory.using_database(self.insights_database_key) as session:
            mock_get_supervisor.return_value = (
                session.query(SupervisionOfficerSupervisor)
                .filter(SupervisionOfficerSupervisor.external_id == "102")
                .first()
            )

            mock_get_outliers.return_value = [
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "HARRY", "surname": "POTTER"}
                    ),
                    external_id="123",
                    pseudonymized_id="hashhash",
                    supervisor_external_id="102",
                    district="Hogwarts",
                    caseload_type=None,
                    outlier_metrics=[
                        {
                            "metric_id": "metric_one",
                            "statuses_over_time": [
                                {
                                    "end_date": "2023-05-01",
                                    "metric_rate": 0.1,
                                    "status": "FAR",
                                },
                                {
                                    "end_date": "2023-04-01",
                                    "metric_rate": 0.1,
                                    "status": "FAR",
                                },
                            ],
                        }
                    ],
                    top_x_pct_metrics=[
                        {
                            "metric_id": "incarceration_starts_and_inferred",
                            "top_x_pct": 10,
                        }
                    ],
                ),
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "RON", "surname": "WEASLEY"}
                    ),
                    external_id="456",
                    pseudonymized_id="hashhashhash",
                    supervisor_external_id="102",
                    district="Hogwarts",
                    caseload_type=None,
                    outlier_metrics=[],
                    top_x_pct_metrics=[],
                ),
            ]

            response = self.test_client.get(
                "/outliers/US_PA/supervisor/hash1/officers?num_lookback_periods=1&period_end_date=2023-05-01",
                headers={"Origin": "http://localhost:3000"},
            )

            self.snapshot.assert_match(response.json, name="test_get_officers_for_supervisor")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_officers_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_officers_mismatched_supervisor_can_access_all(
        self,
        mock_enabled_states: MagicMock,
        mock_get_outliers: MagicMock,
        mock_get_supervisor: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", external_id="101", can_access_all_supervisors=True
        )
        mock_enabled_states.return_value = ["US_PA"]

        with SessionFactory.using_database(self.insights_database_key) as session:
            mock_get_supervisor.return_value = (
                session.query(SupervisionOfficerSupervisor)
                .filter(SupervisionOfficerSupervisor.external_id == "101")
                .first()
            )

            mock_get_outliers.return_value = [
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "HARRY", "surname": "POTTER"}
                    ),
                    external_id="123",
                    pseudonymized_id="hashhash",
                    supervisor_external_id="102",
                    district="Hogwarts",
                    caseload_type=None,
                    outlier_metrics=[
                        {
                            "metric_id": "metric_one",
                            "statuses_over_time": [
                                {
                                    "end_date": "2023-05-01",
                                    "metric_rate": 0.1,
                                    "status": "FAR",
                                },
                                {
                                    "end_date": "2023-04-01",
                                    "metric_rate": 0.1,
                                    "status": "FAR",
                                },
                            ],
                        }
                    ],
                    top_x_pct_metrics=[],
                ),
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "RON", "surname": "WEASLEY"}
                    ),
                    external_id="456",
                    pseudonymized_id="hashhashhash",
                    supervisor_external_id="102",
                    district="Hogwarts",
                    caseload_type=None,
                    outlier_metrics=[],
                    top_x_pct_metrics=[],
                ),
            ]

            response = self.test_client.get(
                "/outliers/US_PA/supervisor/hash1/officers?num_lookback_periods=1&period_end_date=2023-05-01",
                headers={"Origin": "http://localhost:3000"},
            )

            self.snapshot.assert_match(response.json, name="test_get_officers_mismatched_supervisor_can_access_all")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_officers_for_supervisor_failure(
        self,
        mock_enabled_states: MagicMock,
        mock_get_supervisor: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_pa")
        mock_enabled_states.return_value = ["US_PA"]

        mock_get_supervisor.return_value = None

        response = self.test_client.get(
            "/outliers/US_PA/supervisor/hash1/officers?num_lookback_periods=1&period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            response.json,
            {
                "message": "Supervisor with pseudonymized_id doesn't exist in DB. Pseudonymized id: hash1"
            },
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_benchmarks",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_benchmarks(
        self,
        mock_enabled_states: MagicMock,
        mock_get_outliers: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_pa")
        mock_enabled_states.return_value = ["US_PA"]

        mock_get_outliers.return_value = [
            {
                "metric_id": "absconsions_bench_warrants",
                "caseload_type": "ALL",
                "benchmarks": [
                    {"target": 0.14, "end_date": "2023-05-01", "threshold": 0.21},
                    {"target": 0.14, "end_date": "2023-04-01", "threshold": 0.21},
                    {"target": 0.14, "end_date": "2023-03-01", "threshold": 0.21},
                    {"target": 0.14, "end_date": "2023-02-01", "threshold": 0.21},
                    {"target": 0.14, "end_date": "2023-01-01", "threshold": 0.21},
                    {"target": 0.14, "end_date": "2022-12-01", "threshold": 0.21},
                ],
                "latest_period_values": {"far": [0.8], "near": [0.32], "met": [0.1]},
            }
        ]

        response = self.test_client.get(
            "/outliers/US_PA/benchmarks?num_periods=5&period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.snapshot.assert_match(response.json, name="test_get_benchmarks")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_events_by_officer_invalid_date(
        self,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_pa")
        mock_enabled_states.return_value = ["US_PA"]

        response = self.test_client.get(
            "/outliers/US_PA/officer/officerhash3/events?metric_id=absconsions_bench_warrants&period_end_date=23-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.json,
            {
                "message": "Invalid parameters provided. Error: time data '23-05-01' does not match format '%Y-%m-%d'"
            },
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_events_by_officer_only_invalid_metric_ids(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_pa")
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
        )

        response = self.test_client.get(
            "/outliers/US_PA/officer/officerhash3/events?metric_id=fake_metric&period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.json,
            {"message": "Must provide valid metric_ids for US_PA in the request"},
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_events_by_officer_one_valid_one_invalid_metric_id(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_pa")
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
        )

        response = self.test_client.get(
            "/outliers/US_PA/officer/officerhash3/events?metric_id=absconsions_bench_warrants&metric_id=fake_metric&period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.json,
            {"message": "Must provide valid metric_ids for US_PA in the request"},
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_entity",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_events_by_officer_officer_not_found(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer_entity: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_pa")
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
        )

        mock_get_officer_entity.return_value = None

        response = self.test_client.get(
            "/outliers/US_PA/officer/invalidhash/events?metric_id=absconsions_bench_warrants&period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            response.json,
            {"message": "Officer with psuedonymized id not found: invalidhash"},
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_from_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_entity",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_events_by_officer_mismatched_supervisor(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer_entity: MagicMock,
        mock_get_supervisor: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101"
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            district="Guts",
            caseload_type=None,
            outlier_metrics=[
                {
                    "metric_id": "absconsions_bench_warrants",
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                }
            ],
            top_x_pct_metrics=[],
        )

        with SessionFactory.using_database(self.insights_database_key) as session:
            # The supervisor object returned doesn't match the supervisor of the officer
            mock_get_supervisor.return_value = (
                session.query(SupervisionOfficerSupervisor)
                .filter(SupervisionOfficerSupervisor.external_id == "101")
                .first()
            )

            response = self.test_client.get(
                "/outliers/US_PA/officer/hashhash/events?metric_id=absconsions_bench_warrants&period_end_date=2023-05-01",
                headers={"Origin": "http://localhost:3000"},
            )

            self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)
            self.assertEqual(
                response.json,
                {
                    "message": "User cannot access all supervisors and does not supervise the requested officer.",
                },
            )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_events_by_officer",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_from_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_entity",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_events_by_officer_mismatched_supervisor_can_access_all(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer_entity: MagicMock,
        mock_get_supervisor: MagicMock,
        mock_get_events: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101", can_access_all_supervisors=True
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            district="Guts",
            caseload_type=None,
            outlier_metrics=[
                {
                    "metric_id": "absconsions_bench_warrants",
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                }
            ],
            top_x_pct_metrics=[],
        )

        with SessionFactory.using_database(self.insights_database_key) as session:
            # The supervisor object returned doesn't match the supervisor of the officer
            mock_get_supervisor.return_value = (
                session.query(SupervisionOfficerSupervisor)
                .filter(SupervisionOfficerSupervisor.external_id == "101")
                .first()
            )

            mock_get_events.return_value = (
                session.query(SupervisionClientEvent)
                .filter(
                    SupervisionClientEvent.metric_id == TEST_METRIC_3.name,
                    SupervisionClientEvent.client_id == "222",
                )
                .all()
            )

            response = self.test_client.get(
                "/outliers/US_PA/officer/hashhash/events?metric_id=absconsions_bench_warrants&period_end_date=2023-05-01",
                headers={"Origin": "http://localhost:3000"},
            )

            self.snapshot.assert_match(response.json, name="test_get_events_by_officer_mismatched_supervisor_can_access_all")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_from_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_entity",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_events_by_officer_no_outlier_metrics(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer_entity: MagicMock,
        mock_get_supervisor: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101", can_access_all_supervisors=True
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            district="Guts",
            caseload_type=None,
            outlier_metrics=[],
            top_x_pct_metrics=[],
        )
        mock_get_supervisor.return_value = None

        response = self.test_client.get(
            "/outliers/US_PA/officer/hashhash/events?metric_id=absconsions_bench_warrants&period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(
            response.json,
            {"message": "Officer is not an outlier on any of the requested metrics."},
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_from_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_entity",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_events_by_officer_correct_supervisor_but_not_outlier_on_requested(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer_entity: MagicMock,
        mock_get_supervisor: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_pa")
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            district="Guts",
            caseload_type=None,
            outlier_metrics=[
                {
                    "metric_id": "other_metric",
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                }
            ],
            top_x_pct_metrics=[],
        )

        with SessionFactory.using_database(self.insights_database_key) as session:
            mock_get_supervisor.return_value = (
                session.query(SupervisionOfficerSupervisor)
                .filter(SupervisionOfficerSupervisor.external_id == "102")
                .first()
            )

            response = self.test_client.get(
                "/outliers/US_PA/officer/hashhash/events?metric_id=absconsions_bench_warrants&period_end_date=2023-05-01",
                headers={"Origin": "http://localhost:3000"},
            )

            self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)
            self.assertEqual(
                response.json,
                {
                    "message": "Officer is not an outlier on any of the requested metrics."
                },
            )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_from_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_entity",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_events_by_officer_correct_supervisor_requested_nonoutlier_metric(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer_entity: MagicMock,
        mock_get_supervisor: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_pa")
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3, TEST_METRIC_1],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            district="Guts",
            caseload_type=None,
            outlier_metrics=[
                {
                    "metric_id": TEST_METRIC_1.name,
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                }
            ],
            top_x_pct_metrics=[],
        )

        with local_project_id_override("test-project"):
            with SessionFactory.using_database(self.insights_database_key) as session:
                with patch("logging.Logger.error") as mock_logger:
                    mock_get_supervisor.return_value = (
                        session.query(SupervisionOfficerSupervisor)
                        .filter(SupervisionOfficerSupervisor.external_id == "102")
                        .first()
                    )

                    response = self.test_client.get(
                        "/outliers/US_PA/officer/hashhash/events?metric_id=absconsions_bench_warrants&metric_id=incarceration_starts_and_inferred&period_end_date=2023-05-01",
                        headers={"Origin": "http://localhost:3000"},
                    )

                    mock_logger.assert_has_calls(
                        [
                            call(
                                "Officer %s is not an outlier on the following requested metrics: %s",
                                "hashhash",
                                ["absconsions_bench_warrants"],
                            )
                        ]
                    )
                    self.assertEqual(response.status_code, HTTPStatus.OK)

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_events_by_officer",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_from_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_entity",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_events_by_officer_called_with_one_requested_metric(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer_entity: MagicMock,
        mock_get_supervisor: MagicMock,
        mock_get_events: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101", can_access_all_supervisors=True
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3, TEST_METRIC_1],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            district="Guts",
            caseload_type=None,
            outlier_metrics=[
                {
                    "metric_id": TEST_METRIC_1.name,
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                },
                {
                    "metric_id": TEST_METRIC_3.name,
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                },
            ],
            top_x_pct_metrics=[],
        )

        mock_get_supervisor.return_value = None
        mock_get_events.return_value = []
        response = self.test_client.get(
            "/outliers/US_PA/officer/hashhash/events?metric_id=absconsions_bench_warrants&period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        mock_get_events.assert_called_with(
            "hashhash",
            [TEST_METRIC_3.name],
            datetime.strptime("2023-05-01", "%Y-%m-%d"),
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_events_by_officer",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_from_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_entity",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_events_by_officer_success(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer_entity: MagicMock,
        mock_get_supervisor: MagicMock,
        mock_get_events: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101", can_access_all_supervisors=True
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3, TEST_METRIC_1],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            district="Guts",
            caseload_type=None,
            outlier_metrics=[
                {
                    "metric_id": "absconsions_bench_warrants",
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                }
            ],
            top_x_pct_metrics=[],
        )

        with SessionFactory.using_database(self.insights_database_key) as session:
            mock_get_supervisor.return_value = None
            mock_get_events.return_value = (
                session.query(SupervisionClientEvent)
                .filter(
                    SupervisionClientEvent.metric_id == TEST_METRIC_3.name,
                    SupervisionClientEvent.client_id == "222",
                )
                .all()
            )

            response = self.test_client.get(
                "/outliers/US_PA/officer/hashhash/events?period_end_date=2023-05-01",
                headers={"Origin": "http://localhost:3000"},
            )

            mock_get_events.assert_called_with(
                "hashhash",
                ["absconsions_bench_warrants"],
                datetime.strptime("2023-05-01", "%Y-%m-%d"),
            )

            self.snapshot.assert_match(response.json, name="test_get_events_by_officer_success")  # type: ignore[attr-defined]
            mock_get_events.assert_called_with(
                "hashhash",
                [TEST_METRIC_3.name],
                datetime.strptime("2023-05-01", "%Y-%m-%d"),
            )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_events_by_officer",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_from_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_entity",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_events_by_officer_success_with_null_dates(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer_entity: MagicMock,
        mock_get_supervisor: MagicMock,
        mock_get_events: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101", can_access_all_supervisors=True
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3, TEST_METRIC_1],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            district="Guts",
            caseload_type=None,
            outlier_metrics=[
                {
                    "metric_id": "incarceration_starts_and_inferred",
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                }
            ],
            top_x_pct_metrics=[],
        )

        with SessionFactory.using_database(self.insights_database_key) as session:
            mock_get_supervisor.return_value = None
            mock_get_events.return_value = (
                session.query(SupervisionClientEvent)
                .filter(
                    SupervisionClientEvent.metric_id == TEST_METRIC_1.name,
                    SupervisionClientEvent.client_id
                    == "444",  # this client fixture null supervision/assignment dates
                )
                .all()
            )

            response = self.test_client.get(
                "/outliers/US_PA/officer/hashhash/events?period_end_date=2023-05-01",
                headers={"Origin": "http://localhost:3000"},
            )

            mock_get_events.assert_called_with(
                "hashhash",
                ["incarceration_starts_and_inferred"],
                datetime.strptime("2023-05-01", "%Y-%m-%d"),
            )

            self.snapshot.assert_match(response.json, name="test_get_events_by_officer_success_with_null_dates")  # type: ignore[attr-defined]
            mock_get_events.assert_called_with(
                "hashhash",
                [TEST_METRIC_1.name],
                datetime.strptime("2023-05-01", "%Y-%m-%d"),
            )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_entity",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_officer_not_found(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer_entity: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_pa")
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
        )

        mock_get_officer_entity.return_value = None

        response = self.test_client.get(
            "/outliers/US_PA/officer/invalidhash?period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            response.json,
            {"message": "Officer with psuedonymized id not found: invalidhash"},
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_from_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_entity",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_officer_mismatched_supervisor(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer_entity: MagicMock,
        mock_get_supervisor: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101"
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            district="Guts",
            caseload_type=None,
            outlier_metrics=[
                {
                    "metric_id": "absconsions_bench_warrants",
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                }
            ],
            top_x_pct_metrics=[],
        )

        with SessionFactory.using_database(self.insights_database_key) as session:
            # The supervisor object returned doesn't match the supervisor of the officer
            mock_get_supervisor.return_value = (
                session.query(SupervisionOfficerSupervisor)
                .filter(SupervisionOfficerSupervisor.external_id == "101")
                .first()
            )

            response = self.test_client.get(
                "/outliers/US_PA/officer/hashhash?period_end_date=2023-05-01",
                headers={"Origin": "http://localhost:3000"},
            )

            self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)
            self.assertEqual(
                response.json,
                {
                    "message": "User cannot access all supervisors and does not supervise the requested officer.",
                },
            )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_from_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_entity",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_officer_not_outlier(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer_entity: MagicMock,
        mock_get_supervisor: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101", can_access_all_supervisors=True
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            district="Guts",
            caseload_type=None,
            outlier_metrics=[],
            top_x_pct_metrics=[],
        )

        mock_get_supervisor.return_value = None
        response = self.test_client.get(
            "/outliers/US_PA/officer/hashhash?period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.snapshot.assert_match(response.json, name="test_get_officer_not_outlier")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_from_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_entity",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_officer_success(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer_entity: MagicMock,
        mock_get_supervisor: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101", can_access_all_supervisors=True
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            district="Guts",
            caseload_type=None,
            outlier_metrics=[
                {
                    "metric_id": "absconsions_bench_warrants",
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                }
            ],
            top_x_pct_metrics=[],
        )

        mock_get_supervisor.return_value = None

        response = self.test_client.get(
            "/outliers/US_PA/officer/hashhash?period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.snapshot.assert_match(response.json, name="test_get_officer_success")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_user_info_for_supervisor_match(
        self,
        mock_enabled_states: MagicMock,
        mock_get_supervisor_entity: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="102", pseudonymized_id="hashhash"
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_get_supervisor_entity.return_value = SupervisionOfficerSupervisorEntity(
            full_name=PersonName(
                given_names="Supervisor",
                surname="2",
                middle_names=None,
                name_suffix=None,
            ),
            external_id="102",
            pseudonymized_id="hashhash",
            supervision_district="2",
            email="supervisor2@recidiviz.org",
            has_outliers=True,
        )

        response = self.test_client.get(
            "/outliers/US_PA/user-info/hashhash",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.snapshot.assert_match(response.json, name="test_user_info_for_supervisor_match")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_user_info_mismatch(
        self,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="101", pseudonymized_id="hash-101"
        )
        mock_enabled_states.return_value = ["US_PA"]

        response = self.test_client.get(
            "/outliers/US_PA/user-info/hashhash",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(
            response.json,
            {
                "message": "Non-recidiviz user with pseudonymized_id hash-101 is requesting user-info about a user that isn't themselves: hashhash"
            },
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_user_info_for_recidiviz_user(
        self,
        mock_enabled_states: MagicMock,
        mock_get_supervisor_entity: MagicMock,
    ) -> None:
        # Recidiviz user is requesting information
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="RECIDIVIZ", external_id="RECIDIVIZ", allowed_states=["US_PA"]
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_get_supervisor_entity.return_value = SupervisionOfficerSupervisorEntity(
            full_name=PersonName(
                given_names="Supervisor",
                surname="2",
                middle_names=None,
                name_suffix=None,
            ),
            external_id="102",
            pseudonymized_id="hashhash",
            supervision_district="2",
            email="supervisor2@recidiviz.org",
            has_outliers=True,
        )

        response = self.test_client.get(
            "/outliers/US_PA/user-info/hashhash",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.snapshot.assert_match(response.json, name="test_user_info_for_recidiviz_user")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_update_user_info_for_supervisor(
        self,
        mock_enabled_states: MagicMock,
        mock_get_supervisor_entity: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="102", pseudonymized_id="hashhash"
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_get_supervisor_entity.return_value = SupervisionOfficerSupervisorEntity(
            full_name=PersonName(
                given_names="Supervisor",
                surname="2",
                middle_names=None,
                name_suffix=None,
            ),
            external_id="102",
            pseudonymized_id="hashhash",
            supervision_district="2",
            email="supervisor2@recidiviz.org",
            has_outliers=True,
        )

        # Our PATCH endpoint returns the updated entity, so we can compare its response directly
        # without having to make a separate GET
        response = self.test_client.patch(
            "/outliers/US_PA/user-info/hashhash",
            headers={"Origin": "http://localhost:3000"},
            json={"hasSeenOnboarding": True},
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.snapshot.assert_match(response.json, name="test_update_user_info_for_supervisor")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_update_user_info_mismatch(
        self,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="101", pseudonymized_id="hash-101"
        )
        mock_enabled_states.return_value = ["US_PA"]

        response = self.test_client.patch(
            "/outliers/US_PA/user-info/hashhash",
            headers={"Origin": "http://localhost:3000"},
            json={"hasSeenOnboarding": True},
        )

        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(
            response.json,
            {
                "message": "User with pseudonymized_id hash-101 is attempting to update user-info about a user that isn't themselves: hashhash"
            },
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_update_user_info_for_recidiviz_user(
        self,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="RECIDIVIZ", external_id="RECIDIVIZ", allowed_states=["US_PA"]
        )
        mock_enabled_states.return_value = ["US_PA"]

        response = self.test_client.patch(
            "/outliers/US_PA/user-info/hashhash",
            headers={"Origin": "http://localhost:3000"},
            json={"hasSeenOnboarding": True},
        )

        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(
            response.json,
            {
                "message": "User with pseudonymized_id RECIDIVIZ is attempting to update user-info about a user that isn't themselves: hashhash"
            },
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_update_user_info_missing_request_body(
        self,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="101", pseudonymized_id="hashhash"
        )
        mock_enabled_states.return_value = ["US_PA"]

        response = self.test_client.patch(
            "/outliers/US_PA/user-info/hashhash",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.json,
            {
                "message": "Missing request body",
            },
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_update_user_info_empty_request(
        self,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="101", pseudonymized_id="hashhash"
        )
        mock_enabled_states.return_value = ["US_PA"]

        response = self.test_client.patch(
            "/outliers/US_PA/user-info/hashhash",
            headers={"Origin": "http://localhost:3000"},
            json={},
        )

        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.json,
            {
                "message": "Invalid request body. Expected keys: ['hasSeenOnboarding']. Found keys: []",
            },
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_update_user_info_unknown_key(
        self,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="101", pseudonymized_id="hashhash"
        )
        mock_enabled_states.return_value = ["US_PA"]

        response = self.test_client.patch(
            "/outliers/US_PA/user-info/hashhash",
            headers={"Origin": "http://localhost:3000"},
            json={"notARealKey": "value"},
        )

        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.json,
            {
                "message": "Invalid request body. Expected keys: ['hasSeenOnboarding']. Found keys: ['notARealKey']",
            },
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_events_by_client_no_date(
        self,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_pa")
        mock_enabled_states.return_value = ["US_PA"]

        response = self.test_client.get(
            "/outliers/US_PA/client/clienthash1/events?metric_id=violations",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.json,
            {"message": "Must provide an end date"},
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_events_by_client_one_valid_one_invalid_metric_id(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_pa")
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
            client_events=[TEST_CLIENT_EVENT_1],
        )

        response = self.test_client.get(
            "/outliers/US_PA/client/clienthash1/events?metric_id=violation&metric_id=violations&period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.json,
            {"message": "Must provide valid event metric_ids for US_PA in the request"},
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_events_by_client_supervisor_not_outlier(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_supervisor_entity: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa",
            external_id="102",
            pseudonymized_id="officerhash",
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
            client_events=[TEST_CLIENT_EVENT_1],
        )

        mock_get_supervisor_entity.return_value = SupervisionOfficerSupervisorEntity(
            full_name=PersonName(
                given_names="Supervisor",
                surname="2",
                middle_names=None,
                name_suffix=None,
            ),
            external_id="102",
            pseudonymized_id="hashhash",
            supervision_district="2",
            email="supervisor2@recidiviz.org",
            has_outliers=False,
        )

        response = self.test_client.get(
            "/outliers/US_PA/client/clienthash1/events?metric_id=violations&period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(
            response.json,
            {
                "message": "Supervisors must have outliers in the latest period to access this endpoint. User officerhash does not have outliers."
            },
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_events_by_client_success(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_supervisor_entity: MagicMock,
    ) -> None:
        client_hash = "clienthash1"
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="102", pseudonymized_id="hash"
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
            client_events=[TEST_CLIENT_EVENT_1],
        )

        mock_get_supervisor_entity.return_value = SupervisionOfficerSupervisorEntity(
            full_name=PersonName(
                given_names="Supervisor",
                surname="2",
                middle_names=None,
                name_suffix=None,
            ),
            external_id="102",
            pseudonymized_id="hashhash",
            supervision_district="2",
            email="supervisor2@recidiviz.org",
            has_outliers=True,
        )

        response = self.test_client.get(
            f"/outliers/US_PA/client/{client_hash}/events?metric_id=violations&period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.snapshot.assert_match(response.json, name="test_get_events_by_client_success")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_events_by_client_no_name(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_supervisor_entity: MagicMock,
    ) -> None:
        client_hash = "clienthash5"
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="103", pseudonymized_id="hash"
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
            client_events=[TEST_CLIENT_EVENT_1],
        )

        mock_get_supervisor_entity.return_value = SupervisionOfficerSupervisorEntity(
            full_name=PersonName(
                given_names="Supervisor",
                surname="3",
                middle_names=None,
                name_suffix=None,
            ),
            external_id="102",
            pseudonymized_id="hashhash",
            supervision_district="2",
            email="supervisor2@recidiviz.org",
            has_outliers=True,
        )

        response = self.test_client.get(
            f"/outliers/US_PA/client/{client_hash}/events?metric_id=violations&period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.snapshot.assert_match(response.json, name="test_get_events_by_client_no_name")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_events_by_client_success_default_metrics(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_supervisor_entity: MagicMock,
    ) -> None:
        client_hash = "clienthash1"
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="102", pseudonymized_id="hash"
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
            client_events=[TEST_CLIENT_EVENT_1],
        )

        mock_get_supervisor_entity.return_value = SupervisionOfficerSupervisorEntity(
            full_name=PersonName(
                given_names="Supervisor",
                surname="2",
                middle_names=None,
                name_suffix=None,
            ),
            external_id="102",
            pseudonymized_id="hashhash",
            supervision_district="2",
            email="supervisor2@recidiviz.org",
            has_outliers=True,
        )

        response = self.test_client.get(
            f"/outliers/US_PA/client/{client_hash}/events?period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.snapshot.assert_match(response.json, name="test_get_events_by_client_success_default_metrics")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_client_not_outlier(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_supervisor_entity: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa",
            external_id="102",
            pseudonymized_id="officerhash",
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
            client_events=[TEST_CLIENT_EVENT_1],
        )

        mock_get_supervisor_entity.return_value = SupervisionOfficerSupervisorEntity(
            full_name=PersonName(
                given_names="Supervisor",
                surname="2",
                middle_names=None,
                name_suffix=None,
            ),
            external_id="102",
            pseudonymized_id="officerhash",
            supervision_district="2",
            email="supervisor2@recidiviz.org",
            has_outliers=False,
        )

        response = self.test_client.get(
            "/outliers/US_PA/client/clienthash1",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(
            response.json,
            {
                "message": "Supervisors must have outliers in the latest period to access this endpoint. User officerhash does not have outliers."
            },
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_client_not_found(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_supervisor_entity: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="102", pseudonymized_id="hash"
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
            client_events=[TEST_CLIENT_EVENT_1],
        )

        mock_get_supervisor_entity.return_value = SupervisionOfficerSupervisorEntity(
            full_name=PersonName(
                given_names="Supervisor",
                surname="2",
                middle_names=None,
                name_suffix=None,
            ),
            external_id="102",
            pseudonymized_id="hashhash",
            supervision_district="2",
            email="supervisor2@recidiviz.org",
            has_outliers=True,
        )

        response = self.test_client.get(
            "/outliers/US_PA/client/randomrandom",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            response.json,
            {"message": "Client randomrandom not found in the DB"},
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_client_success(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_supervisor_entity: MagicMock,
    ) -> None:
        client_hash = "clienthash1"
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="102", pseudonymized_id="hash"
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
            client_events=[TEST_CLIENT_EVENT_1],
        )

        mock_get_supervisor_entity.return_value = SupervisionOfficerSupervisorEntity(
            full_name=PersonName(
                given_names="Supervisor",
                surname="2",
                middle_names=None,
                name_suffix=None,
            ),
            external_id="102",
            pseudonymized_id="hashhash",
            supervision_district="2",
            email="supervisor2@recidiviz.org",
            has_outliers=True,
        )

        response = self.test_client.get(
            f"/outliers/US_PA/client/{client_hash}",
            headers={"Origin": "http://localhost:3000"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.snapshot.assert_match(response.json, name="test_get_client_success")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.CSG_ALLOWED_OUTLIERS_STATES",
        ["US_PA"],
    )
    def test_get_client_success_for_csg_user(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_supervisor_entity: MagicMock,
    ) -> None:
        client_hash = "clienthash1"
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="CSG", external_id=None
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[TEST_METRIC_3],
            client_events=[TEST_CLIENT_EVENT_1],
        )

        mock_get_supervisor_entity.return_value = SupervisionOfficerSupervisorEntity(
            full_name=PersonName(
                given_names="Supervisor",
                surname="2",
                middle_names=None,
                name_suffix=None,
            ),
            external_id="102",
            pseudonymized_id="hashhash",
            supervision_district="2",
            email="supervisor2@recidiviz.org",
            has_outliers=True,
        )

        response = self.test_client.get(
            f"/outliers/US_PA/client/{client_hash}",
            headers={"Origin": "http://localhost:3000"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.snapshot.assert_match(response.json, name="test_get_client_success_for_csg_user")  # type: ignore[attr-defined]
