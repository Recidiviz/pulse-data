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
from http import HTTPStatus
from typing import Callable, Optional
from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

import pytest
from flask import Flask
from flask.testing import FlaskClient

from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.case_triage.outliers.outliers_authorization import (
    on_successful_authorization,
)
from recidiviz.case_triage.outliers.outliers_routes import create_outliers_api_blueprint
from recidiviz.outliers.constants import INCARCERATION_STARTS_AND_INFERRED
from recidiviz.outliers.types import (
    OutliersConfig,
    OutliersMetricConfig,
    PersonName,
    SupervisionOfficerEntity,
)
from recidiviz.persistence.database.schema.outliers.schema import (
    SupervisionOfficerSupervisor,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.outliers.querier_test import load_model_fixture
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers

TEST_METRIC_1 = OutliersMetricConfig.build_from_metric(
    metric=INCARCERATION_STARTS_AND_INFERRED,
    title_display_name="Incarceration Rate (CPVs & TPVs)",
    body_display_name="incarceration rate",
    event_name="incarcerations",
)


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
        external_id: str = "A1B2",
        allowed_states: Optional[list[str]] = None,
        outliers_routes_enabled: Optional[bool] = True,
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
                        "outliers": outliers_routes_enabled,
                    },
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
            state_code="recidiviz", allowed_states=["US_IX", "US_XX"]
        )

        self.old_auth_claim_namespace = os.environ.get("AUTH0_CLAIM_NAMESPACE", None)
        os.environ["AUTH0_CLAIM_NAMESPACE"] = "https://recidiviz-test"

        self.database_key = SQLAlchemyDatabaseKey(SchemaType.OUTLIERS, db_name="us_xx")
        local_persistence_helpers.use_on_disk_postgresql_database(self.database_key)

        with SessionFactory.using_database(self.database_key) as session:
            for supervisor in load_model_fixture(SupervisionOfficerSupervisor):
                session.add(SupervisionOfficerSupervisor(**supervisor))

    def tearDown(self) -> None:
        super().tearDown()

        if self.old_auth_claim_namespace:
            os.environ["AUTH0_CLAIM_NAMESPACE"] = self.old_auth_claim_namespace
        else:
            os.unsetenv("AUTH0_CLAIM_NAMESPACE")

        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
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
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_config",
    )
    def test_get_state_configuration_success(self, mock_config: MagicMock) -> None:
        mock_config.return_value = OutliersConfig(
            metrics=[TEST_METRIC_1],
            supervision_officer_label="officer",
            learn_more_url="https://recidiviz.org",
        )

        response = self.test_client.get(
            "/outliers/us_ix/configuration",
            headers={"Origin": "http://localhost:3000"},
        )

        expected_json = {
            "config": {
                "learnMoreUrl": "https://recidiviz.org",
                "metrics": [
                    {
                        "bodyDisplayName": "incarceration rate",
                        "eventName": "incarcerations",
                        "name": "incarceration_starts_and_inferred",
                        "outcomeType": "ADVERSE",
                        "titleDisplayName": "Incarceration Rate (CPVs & TPVs)",
                    }
                ],
                "supervisionOfficerLabel": "officer",
            }
        }

        self.assertEqual(HTTPStatus.OK, response.status_code)
        self.assertEqual(expected_json, response.json)

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisors_with_outliers",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_supervisors_with_outliers(
        self, mock_enabled_states: MagicMock, mock_get_supervisors: MagicMock
    ) -> None:
        mock_enabled_states.return_value = ["US_XX", "US_IX"]

        with SessionFactory.using_database(self.database_key) as session:
            mock_get_supervisors.return_value = [
                (
                    session.query(SupervisionOfficerSupervisor)
                    .filter(SupervisionOfficerSupervisor.external_id == "102")
                    .first()
                )
            ]

            response = self.test_client.get(
                "/outliers/US_XX/supervisors",
                headers={"Origin": "http://localhost:3000"},
            )

            expected_json = {
                "supervisors": [
                    {
                        "externalId": "102",
                        "fullName": {
                            "givenNames": "Supervisor",
                            "middleNames": None,
                            "surname": "2",
                        },
                        "supervisionDistrict": "2",
                        "email": "supervisor2@recidiviz.org",
                    }
                ]
            }

            self.assertEqual(response.json, expected_json)

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_from_pseudonymized_id",
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
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_xx")
        mock_enabled_states.return_value = ["US_XX", "US_IX"]

        with SessionFactory.using_database(self.database_key) as session:
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
                ),
            ]

            response = self.test_client.get(
                "/outliers/US_XX/supervisor/hash1/officers?num_lookback_periods=1&period_end_date=2023-05-01",
                headers={"Origin": "http://localhost:3000"},
            )

            expected_json = {
                "officers": [
                    {
                        "fullName": {
                            "givenNames": "Harry",
                            "middleNames": None,
                            "surname": "Potter",
                            "nameSuffix": None,
                        },
                        "externalId": "123",
                        "pseudonymizedId": "hashhash",
                        "supervisorExternalId": "102",
                        "district": "Hogwarts",
                        "caseloadType": None,
                        "outlierMetrics": [
                            {
                                "metricId": "metric_one",
                                "statusesOverTime": [
                                    {
                                        "endDate": "2023-05-01",
                                        "metricRate": 0.1,
                                        "status": "FAR",
                                    },
                                    {
                                        "endDate": "2023-04-01",
                                        "metricRate": 0.1,
                                        "status": "FAR",
                                    },
                                ],
                            }
                        ],
                    },
                    {
                        "fullName": {
                            "givenNames": "Ron",
                            "middleNames": None,
                            "surname": "Weasley",
                            "nameSuffix": None,
                        },
                        "externalId": "456",
                        "pseudonymizedId": "hashhashhash",
                        "supervisorExternalId": "102",
                        "district": "Hogwarts",
                        "caseloadType": None,
                        "outlierMetrics": [],
                    },
                ]
            }

            self.assertEqual(response.json, expected_json)

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_officers_for_supervisor_failure(
        self,
        mock_enabled_states: MagicMock,
        mock_get_supervisor: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_xx")
        mock_enabled_states.return_value = ["US_XX", "US_IX"]

        mock_get_supervisor.return_value = None

        response = self.test_client.get(
            "/outliers/US_XX/supervisor/hash1/officers?num_lookback_periods=1&period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            response.json,
            "Supervisor with pseudonymized_id doesn't exist in DB. Pseudonymized id: hash1",
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
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_xx")
        mock_enabled_states.return_value = ["US_XX", "US_IX"]

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
            "/outliers/US_XX/benchmarks?num_periods=5&period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        expected_json = {
            "metrics": [
                {
                    "metricId": "absconsions_bench_warrants",
                    "caseloadType": "ALL",
                    "benchmarks": [
                        {"target": 0.14, "endDate": "2023-05-01", "threshold": 0.21},
                        {"target": 0.14, "endDate": "2023-04-01", "threshold": 0.21},
                        {"target": 0.14, "endDate": "2023-03-01", "threshold": 0.21},
                        {"target": 0.14, "endDate": "2023-02-01", "threshold": 0.21},
                        {"target": 0.14, "endDate": "2023-01-01", "threshold": 0.21},
                        {"target": 0.14, "endDate": "2022-12-01", "threshold": 0.21},
                    ],
                    "latestPeriodValues": {"far": [0.8], "near": [0.32], "met": [0.1]},
                }
            ]
        }

        self.assertEqual(response.json, expected_json)
