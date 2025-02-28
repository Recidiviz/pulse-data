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
from datetime import date, datetime
from http import HTTPStatus
from typing import Callable, Optional
from unittest import mock
from unittest.mock import MagicMock, call, patch

import freezegun
import pytest
from flask import Flask
from flask.testing import FlaskClient

from recidiviz.calculator.query.state.views.analyst_data.insights_caseload_category_sessions import (
    InsightsCaseloadCategoryType,
)
from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.case_triage.outliers.outliers_authorization import (
    on_successful_authorization,
)
from recidiviz.case_triage.outliers.outliers_routes import create_outliers_api_blueprint
from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.constants import (
    TIMELY_CONTACT,
    TIMELY_RISK_ASSESSMENT,
    VIOLATION_RESPONSES,
)
from recidiviz.outliers.types import (
    ActionStrategySurfacedEvent,
    ActionStrategyType,
    CaseloadCategory,
    ExcludedSupervisionOfficerEntity,
    OutliersBackendConfig,
    OutliersClientEventConfig,
    OutliersProductConfiguration,
    OutliersVitalsMetricConfig,
    PersonName,
    SupervisionOfficerEntity,
    SupervisionOfficerOutcomes,
    SupervisionOfficerSupervisorEntity,
    SupervisionOfficerVitalsEntity,
    VitalsMetric,
)
from recidiviz.persistence.database.schema.insights.schema import (
    ACTION_STRATEGIES_DEFAULT_COPY,
    ActionStrategySurfacedEvents,
    Configuration,
    SupervisionClientEvent,
    SupervisionClients,
    SupervisionOfficer,
    SupervisionOfficerSupervisor,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.insights.insights_db_test_case import InsightsDbTestCase
from recidiviz.tests.insights.utils import load_model_from_json_fixture
from recidiviz.tests.outliers.querier_test import (
    TEST_CLIENT_EVENT_1,
    build_test_metric_1,
    build_test_metric_3,
)
from recidiviz.utils.metadata import local_project_id_override

TEST_CLIENT_EVENT = OutliersClientEventConfig.build(
    event=VIOLATION_RESPONSES, display_name="Sanctions"
)


@pytest.mark.uses_db
@pytest.mark.usefixtures("snapshottest_snapshot")
class OutliersBlueprintTestCase(InsightsDbTestCase):
    """Base class for Outliers flask tests"""

    mock_authorization_handler: MagicMock
    test_app: Flask
    test_client: FlaskClient

    def setUp(self) -> None:
        super().setUp()
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
        super().tearDown()
        self.auth_patcher.stop()

    @staticmethod
    def auth_side_effect(
        state_code: str,
        external_id: Optional[str] = "A1B2",
        allowed_states: Optional[list[str]] = None,
        outliers_routes_enabled: Optional[bool] = True,
        can_access_all_supervisors: Optional[bool] = False,
        can_access_supervision_workflows: Optional[bool] = False,
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
                        "workflowsSupervision": can_access_supervision_workflows,
                    },
                    "pseudonymizedId": pseudonymized_id,
                },
            }
        )


@pytest.mark.uses_db
class TestOutliersRoutes(OutliersBlueprintTestCase):
    """Implements tests for the outliers routes."""

    def setUp(self) -> None:
        super().setUp()

        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="recidiviz", allowed_states=["US_ID", "US_PA"]
        )

        self.old_auth_claim_namespace = os.environ.get("AUTH0_CLAIM_NAMESPACE", None)
        os.environ["AUTH0_CLAIM_NAMESPACE"] = "https://recidiviz-test"

        with SessionFactory.using_database(self.insights_database_key) as session:
            for config in load_model_from_json_fixture(Configuration):
                session.add(Configuration(**config))
            for supervisor in load_model_from_json_fixture(
                SupervisionOfficerSupervisor
            ):
                session.add(SupervisionOfficerSupervisor(**supervisor))
            for event in load_model_from_json_fixture(SupervisionClientEvent):
                session.add(SupervisionClientEvent(**event))
            for client in load_model_from_json_fixture(SupervisionClients):
                session.add(SupervisionClients(**client))
            for as_event in load_model_from_json_fixture(ActionStrategySurfacedEvents):
                session.add(ActionStrategySurfacedEvents(**as_event))

    def tearDown(self) -> None:
        super().tearDown()

        if self.old_auth_claim_namespace:
            os.environ["AUTH0_CLAIM_NAMESPACE"] = self.old_auth_claim_namespace
        else:
            os.unsetenv("AUTH0_CLAIM_NAMESPACE")

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
    def test_get_state_configuration_internal_error(
        self, mock_config: MagicMock
    ) -> None:
        mock_config.side_effect = ValueError("oops")

        response = self.test_client.get(
            "/outliers/us_id/configuration",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(
            (response.get_json() or {}).get("name"),
            "Internal Server Error",
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
            supervisor_has_no_outlier_officers_label="Nice! No officers are outliers on any metrics this month.",
            officer_has_no_outlier_metrics_label="Nice! No outlying metrics this month.",
            supervisor_has_no_officers_with_eligible_clients_label="Nice! No outstanding opportunities for now.",
            officer_has_no_eligible_clients_label="Nice! No outstanding opportunities for now.",
            supervision_unit_label="unit",
            supervision_supervisor_label="supervisor",
            metrics=[
                build_test_metric_1(StateCode.US_IX),
                build_test_metric_3(StateCode.US_IX),
            ],
            client_events=[TEST_CLIENT_EVENT],
            supervision_officer_label="officer",
            learn_more_url="https://recidiviz.org",
            none_are_outliers_label="label1",
            worse_than_rate_label="label2",
            exclusion_reason_description="description",
            at_or_above_rate_label="label3",
            primary_category_type=InsightsCaseloadCategoryType.SEX_OFFENSE_BINARY,
            caseload_categories=[
                CaseloadCategory(
                    id="SEX_OFFENSE",
                    display_name="Sex Offense Caseload",
                ),
                CaseloadCategory(
                    id="NOT_SEX_OFFENSE",
                    display_name="General + Other Caseloads",
                ),
            ],
            vitals_metrics_methodology_url="https://recidiviz.org",
            vitals_metrics=[
                OutliersVitalsMetricConfig(
                    metric_id="timely_risk_assessment",
                    title_display_name="Timely Risk Assessment",
                ),
                OutliersVitalsMetricConfig(
                    metric_id="timely_contact",
                    title_display_name="Timely Contact",
                ),
            ],
            action_strategy_copy=ACTION_STRATEGIES_DEFAULT_COPY,
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
                supervision_unit="central",
                supervision_location_for_list_page="region 2",
                supervision_location_for_supervisor_page="unit 2",
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
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_officers_unauthorized(
        self,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", pseudonymized_id="invalidhash"
        )

        mock_enabled_states.return_value = ["US_PA"]

        response = self.test_client.get(
            "/outliers/US_PA/supervisor/hash1/officers",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(
            response.json,
            {
                "message": "User with pseudo id [invalidhash] cannot access requested supervisor with pseudo id [hash1]."
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
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", pseudonymized_id="hash1"
        )
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
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    earliest_person_assignment_date=date(2024, 1, 1),
                    zero_grant_opportunities=["usPaAdminSupervision"],
                    include_in_outcomes=True,
                ),
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "RON", "surname": "WEASLEY"}
                    ),
                    external_id="456",
                    pseudonymized_id="hashhashhash",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    earliest_person_assignment_date=date(2024, 1, 1),
                    zero_grant_opportunities=[],
                    include_in_outcomes=True,
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
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_excluded_officers_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_excluded_officers_for_supervisor(
        self,
        mock_enabled_states: MagicMock,
        mock_get_excluded_officers: MagicMock,
        mock_get_supervisor: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", pseudonymized_id="hash1"
        )
        mock_enabled_states.return_value = ["US_PA"]

        with SessionFactory.using_database(self.insights_database_key) as session:
            mock_get_supervisor.return_value = (
                session.query(SupervisionOfficerSupervisor)
                .filter(SupervisionOfficerSupervisor.external_id == "102")
                .first()
            )

            mock_get_excluded_officers.return_value = [
                ExcludedSupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "HARRY", "surname": "POTTER"}
                    ),
                    external_id="123",
                    pseudonymized_id="hashhash",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                ),
                ExcludedSupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "RON", "surname": "WEASLEY"}
                    ),
                    external_id="456",
                    pseudonymized_id="hashhashhash",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                ),
            ]

            response = self.test_client.get(
                "/outliers/US_PA/supervisor/hash1/excluded_officers",
                headers={"Origin": "http://localhost:3000"},
            )

            self.snapshot.assert_match(response.json, name="test_get_excluded_officers_for_supervisor")  # type: ignore[attr-defined]

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
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "RON", "surname": "WEASLEY"}
                    ),
                    external_id="456",
                    pseudonymized_id="hashhashhash",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
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
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", pseudonymized_id="hash1"
        )
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
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_outcomes_unauthorized(
        self,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", pseudonymized_id="invalidhash"
        )

        mock_enabled_states.return_value = ["US_PA"]

        response = self.test_client.get(
            "/outliers/US_PA/supervisor/hash1/outcomes",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(
            response.json,
            {
                "message": "User with pseudo id [invalidhash] cannot access requested supervisor with pseudo id [hash1]."
            },
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_officer_outcomes_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_outcomes_for_supervisor(
        self,
        mock_enabled_states: MagicMock,
        mock_get_outcomes: MagicMock,
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

            mock_get_outcomes.return_value = [
                SupervisionOfficerOutcomes(
                    external_id="123",
                    pseudonymized_id="hashhash",
                    caseload_category="ALL",
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
                SupervisionOfficerOutcomes(
                    external_id="456",
                    pseudonymized_id="hashhashhash",
                    caseload_category="ALL",
                    outlier_metrics=[],
                    top_x_pct_metrics=[],
                ),
            ]

            response = self.test_client.get(
                "/outliers/US_PA/supervisor/hash1/outcomes?num_lookback_periods=1&period_end_date=2023-05-01",
                headers={"Origin": "http://localhost:3000"},
            )

            self.snapshot.assert_match(response.json, name="test_get_outcomes_for_supervisor")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_officer_outcomes_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_outcomes_for_supervisor_mismatched_supervisor_can_access_all(
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
                SupervisionOfficerOutcomes(
                    external_id="123",
                    pseudonymized_id="hashhash",
                    caseload_category="ALL",
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
                SupervisionOfficerOutcomes(
                    external_id="456",
                    pseudonymized_id="hashhashhash",
                    caseload_category="ALL",
                    outlier_metrics=[],
                    top_x_pct_metrics=[],
                ),
            ]

            response = self.test_client.get(
                "/outliers/US_PA/supervisor/hash1/outcomes?num_lookback_periods=1&period_end_date=2023-05-01",
                headers={"Origin": "http://localhost:3000"},
            )

            self.snapshot.assert_match(response.json, name="test_get_outcomes_for_supervisor_mismatched_supervisor_can_access_all")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_outcomes_for_supervisor_does_not_exist_failure(
        self,
        mock_enabled_states: MagicMock,
        mock_get_supervisor: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", pseudonymized_id="hash1"
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_get_supervisor.return_value = None

        response = self.test_client.get(
            "/outliers/US_PA/supervisor/hash1/outcomes?num_lookback_periods=1&period_end_date=2023-05-01",
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
        mock_get_benchmarks: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_pa")
        mock_enabled_states.return_value = ["US_PA"]

        mock_get_benchmarks.return_value = [
            {
                "metric_id": "absconsions_bench_warrants",
                "caseload_category": "ALL",
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
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_action_strategy_surfaced_events_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_id_to_supervision_officer_outcomes_entities",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_officers_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_action_strategies_as_outlier_eligible(
        self,
        mock_enabled_states: MagicMock,
        mock_get_officers: MagicMock,
        mock_get_outcomes_by_id: MagicMock,
        mock_get_supervisor: MagicMock,
        mock_get_events: MagicMock,
    ) -> None:
        pseudo_id = "hash2"
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa",
            external_id="102",
            pseudonymized_id=pseudo_id,
        )
        mock_enabled_states.return_value = ["US_PA"]

        with SessionFactory.using_database(self.insights_database_key) as session:
            mock_get_supervisor.return_value = (
                session.query(SupervisionOfficerSupervisor)
                .filter(SupervisionOfficerSupervisor.external_id == "102")
                .first()
            )

            mock_get_officers.return_value = [
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "HARRY", "surname": "POTTER"}
                    ),
                    external_id="123",
                    pseudonymized_id="hashhash",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "RON", "surname": "WEASLEY"}
                    ),
                    external_id="456",
                    pseudonymized_id="hashhashhash",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
            ]

            mock_get_outcomes_by_id.return_value = {
                "123": SupervisionOfficerOutcomes(
                    external_id="123",
                    pseudonymized_id="hashhash",
                    caseload_category="ALL",
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
                "456": SupervisionOfficerOutcomes(
                    external_id="456",
                    pseudonymized_id="hashhashhash",
                    caseload_category="ALL",
                    outlier_metrics=[],
                    top_x_pct_metrics=[],
                ),
            }

            mock_get_events.return_value = []

            response = self.test_client.get(
                "/outliers/us_pa/action_strategies/hash2",
                headers={"Origin": "http://localhost:3000"},
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.snapshot.assert_match(response.json, name="test_get_action_strategies_as_outlier_eligible")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_action_strategy_surfaced_events_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_id_to_supervision_officer_outcomes_entities",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_officers_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_action_strategies_leadership_user_also_supervisor(
        self,
        mock_enabled_states: MagicMock,
        mock_get_outliers: MagicMock,
        mock_get_outcomes_by_id: MagicMock,
        mock_get_supervisor: MagicMock,
        mock_get_events: MagicMock,
    ) -> None:
        pseudo_id = "hash2"
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa",
            external_id="102",
            pseudonymized_id=pseudo_id,
            can_access_all_supervisors=True,
        )
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
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "RON", "surname": "WEASLEY"}
                    ),
                    external_id="456",
                    pseudonymized_id="hashhashhash",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
            ]

            mock_get_outcomes_by_id.return_value = {
                "123": SupervisionOfficerOutcomes(
                    external_id="123",
                    pseudonymized_id="hashhash",
                    caseload_category="ALL",
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
                "456": SupervisionOfficerOutcomes(
                    external_id="456",
                    pseudonymized_id="hashhashhash",
                    caseload_category="ALL",
                    outlier_metrics=[],
                    top_x_pct_metrics=[],
                ),
            }

            mock_get_events.return_value = []

            response = self.test_client.get(
                "/outliers/us_pa/action_strategies/hash2",
                headers={"Origin": "http://localhost:3000"},
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.snapshot.assert_match(response.json, name="test_get_action_strategies_leadership_user_also_supervisor")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_action_strategies_leadership_user(
        self,
        mock_enabled_states: MagicMock,
        mock_get_supervisor: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa",
            external_id="102",
            pseudonymized_id="leadershipHash",
            can_access_all_supervisors=True,
        )
        mock_enabled_states.return_value = ["US_PA"]

        with SessionFactory.using_database(self.insights_database_key):
            mock_get_supervisor.return_value = None

            response = self.test_client.get(
                "/outliers/us_pa/action_strategies/leadershipHash",
                headers={"Origin": "http://localhost:3000"},
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.snapshot.assert_match(response.json, name="test_get_action_strategies_leadership_user")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_action_strategy_surfaced_events_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_id_to_supervision_officer_outcomes_entities",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_officers_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_action_strategies_as_outlier_already_surfaced(
        self,
        mock_enabled_states: MagicMock,
        mock_get_officers: MagicMock,
        mock_get_outcomes_by_id: MagicMock,
        mock_get_supervisor: MagicMock,
        mock_get_events: MagicMock,
    ) -> None:
        pseudo_id = "hash2"
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa",
            external_id="102",
            pseudonymized_id=pseudo_id,
        )
        mock_enabled_states.return_value = ["US_PA"]

        with SessionFactory.using_database(self.insights_database_key) as session:
            mock_get_supervisor.return_value = (
                session.query(SupervisionOfficerSupervisor)
                .filter(SupervisionOfficerSupervisor.external_id == "102")
                .first()
            )

            mock_get_officers.return_value = [
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "HARRY", "surname": "POTTER"}
                    ),
                    external_id="123",
                    pseudonymized_id="hashhash",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "RON", "surname": "WEASLEY"}
                    ),
                    external_id="456",
                    pseudonymized_id="hashhashhash",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
            ]

            mock_get_outcomes_by_id.return_value = {
                "123": SupervisionOfficerOutcomes(
                    external_id="123",
                    pseudonymized_id="hashhash",
                    caseload_category="ALL",
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
                "456": SupervisionOfficerOutcomes(
                    external_id="456",
                    pseudonymized_id="hashhashhash",
                    caseload_category="ALL",
                    # This officer is ineligible for ACTION_STRATEGY_OUTLIER because they are not an outlier
                    outlier_metrics=[],
                    top_x_pct_metrics=[],
                ),
            }

            mock_get_events.return_value = [
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hashhash",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER.value,
                    timestamp=date(2023, 4, 1),
                ),
            ]

            response = self.test_client.get(
                "/outliers/us_pa/action_strategies/hash2",
                headers={"Origin": "http://localhost:3000"},
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.snapshot.assert_match(response.json, name="test_get_action_strategies_as_outlier_already_surfaced")  # type: ignore[attr-defined]

    @freezegun.freeze_time(date(2023, 6, 20))
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_action_strategy_surfaced_events_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_id_to_supervision_officer_outcomes_entities",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_officers_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_action_strategies_3_months_as_outlier_eligible(
        self,
        mock_enabled_states: MagicMock,
        mock_get_officers: MagicMock,
        mock_get_outcomes_by_id: MagicMock,
        mock_get_supervisor: MagicMock,
        mock_get_events: MagicMock,
    ) -> None:
        pseudo_id = "hash2"
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa",
            external_id="102",
            pseudonymized_id=pseudo_id,
        )
        mock_enabled_states.return_value = ["US_PA"]

        with SessionFactory.using_database(self.insights_database_key) as session:
            mock_get_supervisor.return_value = (
                session.query(SupervisionOfficerSupervisor)
                .filter(SupervisionOfficerSupervisor.external_id == "102")
                .first()
            )

            mock_get_officers.return_value = [
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "HERMIONE", "surname": "GRANGER"}
                    ),
                    external_id="456",
                    pseudonymized_id="hash4",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "LORD", "surname": "VOLDEMORT"}
                    ),
                    external_id="678",
                    pseudonymized_id="hash6",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
            ]

            mock_get_outcomes_by_id.return_value = {
                "678": SupervisionOfficerOutcomes(
                    external_id="678",
                    pseudonymized_id="hash6",
                    caseload_category="ALL",
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
                                    "end_date": "2023-03-01",
                                    "metric_rate": 0.1,
                                    "status": "FAR",
                                },
                                {
                                    "end_date": "2023-02-01",
                                    "metric_rate": 0.1,
                                    "status": "FAR",
                                },
                                {
                                    "end_date": "2023-01-01",
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
                "456": SupervisionOfficerOutcomes(
                    external_id="456",
                    pseudonymized_id="hash4",
                    caseload_category="ALL",
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
                                    "end_date": "2023-03-01",
                                    "metric_rate": 0.1,
                                    "status": "FAR",
                                },
                                {
                                    "end_date": "2023-02-01",
                                    "metric_rate": 0.1,
                                    "status": "FAR",
                                },
                                {
                                    "end_date": "2023-01-01",
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
            }

            mock_get_events.return_value = [
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash4",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER.value,
                    timestamp=date(2023, 4, 1),
                ),
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash6",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER.value,
                    timestamp=date(2023, 4, 1),
                ),
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash4",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_60_PERC_OUTLIERS.value,
                    timestamp=date(2023, 4, 1),
                ),
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash6",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_60_PERC_OUTLIERS.value,
                    timestamp=date(2023, 4, 1),
                ),
            ]

            response = self.test_client.get(
                "/outliers/us_pa/action_strategies/hash2",
                headers={"Origin": "http://localhost:3000"},
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.snapshot.assert_match(response.json, name="test_get_action_strategies_3_months_as_outlier_eligible")  # type: ignore[attr-defined]

    @freezegun.freeze_time(datetime(2023, 6, 20, 0, 0, 0, 0))
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_action_strategy_surfaced_events_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_id_to_supervision_officer_outcomes_entities",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_officers_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_action_strategies_3_months_as_outlier_ineligible(
        self,
        mock_enabled_states: MagicMock,
        mock_get_officers: MagicMock,
        mock_get_outcomes_by_id: MagicMock,
        mock_get_supervisor: MagicMock,
        mock_get_events: MagicMock,
    ) -> None:
        pseudo_id = "hash2"
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa",
            external_id="102",
            pseudonymized_id=pseudo_id,
        )
        mock_enabled_states.return_value = ["US_PA"]

        with SessionFactory.using_database(self.insights_database_key) as session:
            mock_get_supervisor.return_value = (
                session.query(SupervisionOfficerSupervisor)
                .filter(SupervisionOfficerSupervisor.external_id == "102")
                .first()
            )

            mock_get_officers.return_value = [
                # This officer is ineligible for ACTION_STRATEGY_OUTLIER_3_MONTHS because outlier status is not consecutive for 3+ months
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "DRACO", "surname": "MALFOY"}
                    ),
                    external_id="567",
                    pseudonymized_id="hash5",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
                # This officer is ineligible for ACTION_STRATEGY_OUTLIER_3_MONTHS because neither ACTION_STRATEGY_OUTLIER or ACTION_STRATEGY_60_PERC_OUTLIERS surfaced
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "HARRY", "surname": "POTTER"}
                    ),
                    external_id="789",
                    pseudonymized_id="hash7",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
                # # This officer is ineligible for ACTION_STRATEGY_OUTLIER_3_MONTHS because outlier status is not consecutive for 3+ months
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "HERMIONE", "surname": "GRANGER"}
                    ),
                    external_id="890",
                    pseudonymized_id="hash8",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
            ]

            mock_get_outcomes_by_id.return_value = {
                "567": SupervisionOfficerOutcomes(
                    external_id="567",
                    pseudonymized_id="hash5",
                    caseload_category="ALL",
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
                                    "end_date": "2023-03-01",
                                    "metric_rate": 0.1,
                                    "status": "FAR",
                                },
                                {
                                    "end_date": "2023-02-01",
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
                "789": SupervisionOfficerOutcomes(
                    external_id="789",
                    pseudonymized_id="hash7",
                    caseload_category="ALL",
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
                                {
                                    "end_date": "2023-03-01",
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
                "890": SupervisionOfficerOutcomes(
                    external_id="890",
                    pseudonymized_id="hash8",
                    caseload_category="ALL",
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
                                    "status": "NEAR",
                                },
                                {
                                    "end_date": "2023-03-01",
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
            }

            mock_get_events.return_value = [
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash5",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER.value,
                    timestamp=date(2023, 4, 1),
                ),
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash5",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_60_PERC_OUTLIERS.value,
                    timestamp=date(2023, 4, 1),
                ),
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash7",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_60_PERC_OUTLIERS.value,
                    timestamp=date(2023, 5, 1),
                ),
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash8",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER.value,
                    timestamp=date(2023, 3, 1),
                ),
            ]

            response = self.test_client.get(
                "/outliers/us_pa/action_strategies/hash2",
                headers={"Origin": "http://localhost:3000"},
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.snapshot.assert_match(response.json, name="test_get_action_strategies_3_months_as_outlier_ineligible")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_action_strategy_surfaced_events_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_id_to_supervision_officer_outcomes_entities",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_officers_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_action_strategies_3_months_as_outlier_already_surfaced(
        self,
        mock_enabled_states: MagicMock,
        mock_get_officers: MagicMock,
        mock_get_outcomes_by_id: MagicMock,
        mock_get_supervisor: MagicMock,
        mock_get_events: MagicMock,
    ) -> None:
        pseudo_id = "hash2"
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa",
            external_id="102",
            pseudonymized_id=pseudo_id,
        )
        mock_enabled_states.return_value = ["US_PA"]

        with SessionFactory.using_database(self.insights_database_key) as session:
            mock_get_supervisor.return_value = (
                session.query(SupervisionOfficerSupervisor)
                .filter(SupervisionOfficerSupervisor.external_id == "102")
                .first()
            )

            mock_get_officers.return_value = [
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "HERMIONE", "surname": "GRANGER"}
                    ),
                    external_id="456",
                    pseudonymized_id="hash4",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "LORD", "surname": "VOLDEMORT"}
                    ),
                    external_id="678",
                    pseudonymized_id="hash6",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
            ]

            mock_get_outcomes_by_id.return_value = {
                "456": SupervisionOfficerOutcomes(
                    external_id="456",
                    pseudonymized_id="hash4",
                    caseload_category="ALL",
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
                                {
                                    "end_date": "2023-03-01",
                                    "metric_rate": 0.1,
                                    "status": "FAR",
                                },
                                {
                                    "end_date": "2023-01-01",
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
                "678": SupervisionOfficerOutcomes(
                    external_id="678",
                    pseudonymized_id="hash6",
                    caseload_category="ALL",
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
                                    "end_date": "2023-03-01",
                                    "metric_rate": 0.1,
                                    "status": "FAR",
                                },
                                {
                                    "end_date": "2023-02-01",
                                    "metric_rate": 0.1,
                                    "status": "FAR",
                                },
                                {
                                    "end_date": "2023-01-01",
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
            }

            mock_get_events.return_value = [
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash4",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER.value,
                    timestamp=date(2024, 8, 20),
                ),
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash6",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER.value,
                    timestamp=date(2024, 8, 20),
                ),
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash4",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER_3_MONTHS.value,
                    timestamp=date(2024, 8, 20),
                ),
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash6",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER_3_MONTHS.value,
                    timestamp=date(2024, 8, 20),
                ),
            ]

            response = self.test_client.get(
                "/outliers/us_pa/action_strategies/hash2",
                headers={"Origin": "http://localhost:3000"},
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.snapshot.assert_match(response.json, name="test_get_action_strategies_3_months_as_outlier_already_surfaced")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_action_strategy_surfaced_events_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_id_to_supervision_officer_outcomes_entities",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_officers_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_action_strategy_60_perc_outliers_eligible_eligible(
        self,
        mock_enabled_states: MagicMock,
        mock_get_officers: MagicMock,
        mock_get_outcomes_by_id: MagicMock,
        mock_get_supervisor: MagicMock,
        mock_get_events: MagicMock,
    ) -> None:
        pseudo_id = "supervisorHash"
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa",
            external_id="102",
            pseudonymized_id=pseudo_id,
        )
        mock_enabled_states.return_value = ["US_PA"]

        with SessionFactory.using_database(self.insights_database_key) as session:
            supervisor = (
                session.query(SupervisionOfficerSupervisor)
                .filter(SupervisionOfficerSupervisor.external_id == "102")
                .first()
            )
            supervisor.pseudonymized_id = pseudo_id
            mock_get_supervisor.return_value = supervisor

            mock_get_officers.return_value = [
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "DRACO", "surname": "MALFOY"}
                    ),
                    external_id="1",
                    pseudonymized_id="hash1",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "HARRY", "surname": "POTTER"}
                    ),
                    external_id="2",
                    pseudonymized_id="hash2",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "HERMOINE", "surname": "GRANGER"}
                    ),
                    external_id="3",
                    pseudonymized_id="hash3",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "RON", "surname": "WHATSHISNAME"}
                    ),
                    external_id="4",
                    pseudonymized_id="hash4",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
            ]

            mock_get_outcomes_by_id.return_value = {
                "1": SupervisionOfficerOutcomes(
                    external_id="1",
                    pseudonymized_id="hash1",
                    caseload_category="ALL",
                    outlier_metrics=[
                        {
                            "metric_id": "metric_one",
                            "statuses_over_time": [
                                {
                                    "end_date": "2023-05-01",
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
                "2": SupervisionOfficerOutcomes(
                    external_id="2",
                    pseudonymized_id="hash2",
                    caseload_category="ALL",
                    outlier_metrics=[
                        {
                            "metric_id": "metric_one",
                            "statuses_over_time": [
                                {
                                    "end_date": "2023-05-01",
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
                "3": SupervisionOfficerOutcomes(
                    external_id="3",
                    pseudonymized_id="hash3",
                    caseload_category="ALL",
                    outlier_metrics=[
                        {
                            "metric_id": "metric_one",
                            "statuses_over_time": [
                                {
                                    "end_date": "2023-05-01",
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
                "4": SupervisionOfficerOutcomes(
                    external_id="4",
                    pseudonymized_id="hash4",
                    caseload_category="ALL",
                    outlier_metrics=[],
                    top_x_pct_metrics=[
                        {
                            "metric_id": "incarceration_starts_and_inferred",
                            "top_x_pct": 10,
                        }
                    ],
                ),
            }

            mock_get_events.return_value = []

            response = self.test_client.get(
                "/outliers/us_pa/action_strategies/supervisorHash",
                headers={"Origin": "http://localhost:3000"},
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.snapshot.assert_match(response.json, name="test_get_action_strategy_60_perc_outliers_eligible_eligible")  # type: ignore[attr-defined]

    @freezegun.freeze_time(datetime(2023, 8, 24, 0, 0, 0, 0))
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_action_strategy_surfaced_events_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervisor_entity_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_id_to_supervision_officer_outcomes_entities",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_officers_for_supervisor",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_action_strategies_as_outlier_comprehensive(
        self,
        mock_enabled_states: MagicMock,
        mock_get_officers: MagicMock,
        mock_get_outcomes_by_id: MagicMock,
        mock_get_supervisor: MagicMock,
        mock_get_events: MagicMock,
    ) -> None:
        pseudo_id = "supervisorHash"
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa",
            external_id="102",
            pseudonymized_id=pseudo_id,
        )
        mock_enabled_states.return_value = ["US_PA"]

        with SessionFactory.using_database(self.insights_database_key) as session:
            supervisor = (
                session.query(SupervisionOfficerSupervisor)
                .filter(SupervisionOfficerSupervisor.external_id == "102")
                .first()
            )
            supervisor.pseudonymized_id = pseudo_id
            mock_get_supervisor.return_value = supervisor

            mock_get_officers.return_value = [
                # Has no events so should return ACTION_STRATEGY_OUTLIER
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "DRACO", "surname": "MALFOY"}
                    ),
                    external_id="1",
                    pseudonymized_id="hash1",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
                # Has an ACTION_STRATEGY_OUTLIER event + outlier 3 months
                # Should return ACTION_STRATEGY_OUTLIER_3_MONTHS
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "HARRY", "surname": "POTTER"}
                    ),
                    external_id="2",
                    pseudonymized_id="hash2",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
                # Has an ACTION_STRATEGY_OUTLIER event + is absconsion metric
                # Should return ACTION_STRATEGY_OUTLIER_ABSCONSION
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "HERMOINE", "surname": "GRANGER"}
                    ),
                    external_id="3",
                    pseudonymized_id="hash3",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
                # Not an outlier, should return None
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "RON", "surname": "WHATSHISNAME"}
                    ),
                    external_id="4",
                    pseudonymized_id="hash4",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    include_in_outcomes=True,
                ),
                # Has an ACTION_STRATEGY_OUTLIER event, ACTION_STRATEGY_OUTLIER_ABSCONSION, and ACTION_STRATEGY_OUTLIER_3_MONTHS events
                # Should return ACTION_STRATEGY_OUTLIER_NEW_OFFICER
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "RON", "surname": "WHATSHISNAME"}
                    ),
                    external_id="5",
                    pseudonymized_id="hash5",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    earliest_person_assignment_date=date(2022, 7, 30),
                    include_in_outcomes=True,
                ),
                # Has seen all of the action strategies, should return None
                SupervisionOfficerEntity(
                    full_name=PersonName(
                        **{"given_names": "SEVERUS", "surname": "SNAPE"}
                    ),
                    external_id="6",
                    pseudonymized_id="hash6",
                    supervisor_external_id="102",
                    supervisor_external_ids=["102"],
                    district="Hogwarts",
                    avg_daily_population=10.0,
                    earliest_person_assignment_date=date(2022, 7, 30),
                    include_in_outcomes=True,
                ),
            ]

            mock_get_outcomes_by_id.return_value = {
                "1": SupervisionOfficerOutcomes(
                    external_id="1",
                    pseudonymized_id="hash1",
                    caseload_category="ALL",
                    outlier_metrics=[
                        {
                            "metric_id": "metric_one",
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
                ),
                "2": SupervisionOfficerOutcomes(
                    external_id="2",
                    pseudonymized_id="hash2",
                    caseload_category="ALL",
                    outlier_metrics=[
                        {
                            "metric_id": "metric_one",
                            "statuses_over_time": [
                                {
                                    "end_date": "2023-08-01",
                                    "metric_rate": 0.1,
                                    "status": "FAR",
                                },
                                {
                                    "end_date": "2023-07-01",
                                    "metric_rate": 0.1,
                                    "status": "FAR",
                                },
                                {
                                    "end_date": "2023-06-01",
                                    "metric_rate": 0.1,
                                    "status": "FAR",
                                },
                            ],
                        }
                    ],
                    top_x_pct_metrics=[],
                ),
                "3": SupervisionOfficerOutcomes(
                    external_id="3",
                    pseudonymized_id="hash3",
                    caseload_category="ALL",
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
                ),
                "4": SupervisionOfficerOutcomes(
                    external_id="4",
                    pseudonymized_id="hash4",
                    caseload_category="ALL",
                    outlier_metrics=[],
                    top_x_pct_metrics=[
                        {
                            "metric_id": "incarceration_starts_and_inferred",
                            "top_x_pct": 10,
                        }
                    ],
                ),
                "5": SupervisionOfficerOutcomes(
                    external_id="5",
                    pseudonymized_id="hash5",
                    caseload_category="ALL",
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
                ),
                "6": SupervisionOfficerOutcomes(
                    external_id="6",
                    pseudonymized_id="hash6",
                    caseload_category="ALL",
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
                ),
            }

            mock_get_events.return_value = [
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash2",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER.value,
                    timestamp=date(2023, 7, 20),
                ),
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash3",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER.value,
                    timestamp=date(2023, 8, 20),
                ),
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash5",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER.value,
                    timestamp=date(2023, 8, 20),
                ),
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash5",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER_3_MONTHS.value,
                    timestamp=date(2023, 8, 20),
                ),
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash5",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER_ABSCONSION.value,
                    timestamp=date(2023, 8, 20),
                ),
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash6",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER.value,
                    timestamp=date(2023, 8, 20),
                ),
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash6",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER_3_MONTHS.value,
                    timestamp=date(2023, 8, 20),
                ),
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash6",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER_ABSCONSION.value,
                    timestamp=date(2023, 8, 20),
                ),
                ActionStrategySurfacedEvent(
                    state_code="us_pa",
                    user_pseudonymized_id=pseudo_id,
                    officer_pseudonymized_id="hash6",
                    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER_NEW_OFFICER.value,
                    timestamp=date(2023, 8, 21),
                ),
            ]

            response = self.test_client.get(
                "/outliers/us_pa/action_strategies/supervisorHash",
                headers={"Origin": "http://localhost:3000"},
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.snapshot.assert_match(response.json, name="test_get_action_strategies_as_outlier_comprehensive")  # type: ignore[attr-defined]

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
            metrics=[build_test_metric_3(StateCode.US_PA)],
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
            metrics=[build_test_metric_3(StateCode.US_PA)],
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
            metrics=[build_test_metric_3(StateCode.US_PA)],
        )

        mock_get_officer_entity.return_value = None

        response = self.test_client.get(
            "/outliers/US_PA/officer/invalidhash/events?metric_id=absconsions_bench_warrants&period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            response.json,
            {"message": "Officer with pseudonymized id not found: invalidhash"},
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.supervisor_exists_with_external_id",
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
        mock_supervisor_exists: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101"
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_3(StateCode.US_PA)],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Guts",
            avg_daily_population=10.0,
            include_in_outcomes=True,
        )

        # The supervisor exists, but doesn't match the supervisor of the officer
        mock_supervisor_exists.return_value = True

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
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.supervisor_exists_with_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_outcomes",
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
        mock_get_officer_outcomes: MagicMock,
        mock_supervisor_exists: MagicMock,
        mock_get_events: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101", can_access_all_supervisors=True
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_1(StateCode.US_PA)],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Guts",
            avg_daily_population=10.0,
            include_in_outcomes=True,
        )

        mock_get_officer_outcomes.return_value = SupervisionOfficerOutcomes(
            external_id="123",
            pseudonymized_id="hashhash",
            caseload_category="ALL",
            outlier_metrics=[
                {
                    "metric_id": build_test_metric_1(StateCode.US_PA).name,
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
            # The supervisor exists, but doesn't match the supervisor of the officer
            mock_supervisor_exists.return_value = True

            mock_get_events.return_value = (
                session.query(SupervisionClientEvent)
                .filter(
                    SupervisionClientEvent.metric_id
                    == build_test_metric_1(StateCode.US_PA).name,
                    SupervisionClientEvent.client_id == "222",
                )
                .all()
            )

            response = self.test_client.get(
                "/outliers/US_PA/officer/hashhash/events?metric_id=incarceration_starts_and_inferred&period_end_date=2023-05-01",
                headers={"Origin": "http://localhost:3000"},
            )

            self.snapshot.assert_match(response.json, name="test_get_events_by_officer_mismatched_supervisor_can_access_all")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.supervisor_exists_with_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_outcomes",
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
    def test_get_events_by_officer_outcomes_not_found(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer_entity: MagicMock,
        mock_get_officer_outcomes: MagicMock,
        mock_supervisor_exists: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101", can_access_all_supervisors=True
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[
                build_test_metric_3(StateCode.US_PA),
                build_test_metric_1(StateCode.US_PA),
            ],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Guts",
            avg_daily_population=10.0,
            include_in_outcomes=True,
        )
        # No outcomes found for officer
        mock_get_officer_outcomes.return_value = None
        mock_supervisor_exists.return_value = True

        response = self.test_client.get(
            "/outliers/US_PA/officer/hashhash/events?metric_id=absconsions_bench_warrants&period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            response.json,
            {
                "message": "Officer outcomes data for pseudonymized id not found: hashhash",
            },
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.supervisor_exists_with_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_outcomes",
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
        mock_get_officer_outcomes: MagicMock,
        mock_supervisor_exists: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101", can_access_all_supervisors=True
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_3(StateCode.US_PA)],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Guts",
            avg_daily_population=10.0,
            include_in_outcomes=True,
        )

        mock_get_officer_outcomes.return_value = SupervisionOfficerOutcomes(
            external_id="123",
            pseudonymized_id="hashhash",
            caseload_category="ALL",
            outlier_metrics=[],
            top_x_pct_metrics=[],
        )
        mock_supervisor_exists.return_value = False

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
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.supervisor_exists_with_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_outcomes",
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
        mock_get_officer_outcomes: MagicMock,
        mock_supervisor_exists: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", external_id="102"
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_3(StateCode.US_PA)],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Guts",
            avg_daily_population=10.0,
            include_in_outcomes=True,
        )

        mock_get_officer_outcomes.return_value = SupervisionOfficerOutcomes(
            external_id="123",
            pseudonymized_id="hashhash",
            caseload_category="ALL",
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

        mock_supervisor_exists.return_value = True

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
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.supervisor_exists_with_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_outcomes",
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
        mock_get_officer_outcomes: MagicMock,
        mock_supervisor_exists: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", external_id="102"
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[
                build_test_metric_3(StateCode.US_PA),
                build_test_metric_1(StateCode.US_PA),
            ],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Guts",
            avg_daily_population=10.0,
            include_in_outcomes=True,
        )

        mock_get_officer_outcomes.return_value = SupervisionOfficerOutcomes(
            external_id="123",
            pseudonymized_id="hashhash",
            caseload_category="ALL",
            outlier_metrics=[
                {
                    "metric_id": build_test_metric_3(StateCode.US_PA).name,
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
            with patch("logging.Logger.error") as mock_logger:
                mock_supervisor_exists.return_value = True

                response = self.test_client.get(
                    "/outliers/US_PA/officer/hashhash/events?metric_id=absconsions_bench_warrants&metric_id=incarceration_starts_and_inferred&period_end_date=2023-05-01",
                    headers={"Origin": "http://localhost:3000"},
                )

                mock_logger.assert_has_calls(
                    [
                        call(
                            "Officer %s is not an outlier on the following requested metrics: %s",
                            "hashhash",
                            ["incarceration_starts_and_inferred"],
                        )
                    ]
                )
                self.assertEqual(response.status_code, HTTPStatus.OK)

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_events_by_officer",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.supervisor_exists_with_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_outcomes",
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
        mock_get_officer_outcomes: MagicMock,
        mock_supervisor_exists: MagicMock,
        mock_get_events: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101", can_access_all_supervisors=True
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[
                build_test_metric_3(StateCode.US_PA),
                build_test_metric_1(StateCode.US_PA),
            ],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Guts",
            avg_daily_population=10.0,
            include_in_outcomes=True,
        )

        mock_get_officer_outcomes.return_value = SupervisionOfficerOutcomes(
            external_id="123",
            pseudonymized_id="hashhash",
            caseload_category="ALL",
            outlier_metrics=[
                {
                    "metric_id": build_test_metric_1(StateCode.US_PA).name,
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                },
                {
                    "metric_id": build_test_metric_3(StateCode.US_PA).name,
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

        mock_supervisor_exists.return_value = False
        mock_get_events.return_value = []
        response = self.test_client.get(
            "/outliers/US_PA/officer/hashhash/events?metric_id=absconsions_bench_warrants&period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        mock_get_events.assert_called_with(
            "hashhash",
            [build_test_metric_3(StateCode.US_PA).name],
            datetime.strptime("2023-05-01", "%Y-%m-%d"),
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_events_by_officer",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.supervisor_exists_with_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_outcomes",
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
        mock_get_officer_outcomes: MagicMock,
        mock_supervisor_exists: MagicMock,
        mock_get_events: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101", can_access_all_supervisors=True
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[
                build_test_metric_3(StateCode.US_PA),
                build_test_metric_1(StateCode.US_PA),
            ],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Guts",
            avg_daily_population=10.0,
            include_in_outcomes=True,
        )

        mock_get_officer_outcomes.return_value = SupervisionOfficerOutcomes(
            external_id="123",
            pseudonymized_id="hashhash",
            caseload_category="ALL",
            outlier_metrics=[
                {
                    "metric_id": build_test_metric_1(StateCode.US_PA).name,
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
            mock_supervisor_exists.return_value = False
            mock_get_events.return_value = (
                session.query(SupervisionClientEvent)
                .filter(
                    SupervisionClientEvent.metric_id
                    == build_test_metric_1(StateCode.US_PA).name,
                    SupervisionClientEvent.client_id == "222",
                )
                .all()
            )

            response = self.test_client.get(
                "/outliers/US_PA/officer/hashhash/events?period_end_date=2023-05-01",
                headers={"Origin": "http://localhost:3000"},
            )

            self.snapshot.assert_match(response.json, name="test_get_events_by_officer_success")  # type: ignore[attr-defined]
            mock_get_events.assert_called_with(
                "hashhash",
                [build_test_metric_1(StateCode.US_PA).name],
                datetime.strptime("2023-05-01", "%Y-%m-%d"),
            )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_events_by_officer",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.supervisor_exists_with_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_outcomes",
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
        mock_get_officer_outcomes: MagicMock,
        mock_supervisor_exists: MagicMock,
        mock_get_events: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101", can_access_all_supervisors=True
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[
                build_test_metric_3(StateCode.US_PA),
                build_test_metric_1(StateCode.US_PA),
            ],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Guts",
            avg_daily_population=10.0,
            include_in_outcomes=True,
        )

        mock_get_officer_outcomes.return_value = SupervisionOfficerOutcomes(
            external_id="123",
            pseudonymized_id="hashhash",
            caseload_category="ALL",
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
            mock_supervisor_exists.return_value = False
            mock_get_events.return_value = (
                session.query(SupervisionClientEvent)
                .filter(
                    SupervisionClientEvent.metric_id
                    == build_test_metric_3(StateCode.US_PA).name,
                    SupervisionClientEvent.client_id
                    == "444",  # this client fixture null supervision/assignment dates
                )
                .all()
            )

            response = self.test_client.get(
                "/outliers/US_PA/officer/hashhash/events?period_end_date=2023-05-01",
                headers={"Origin": "http://localhost:3000"},
            )

            self.snapshot.assert_match(response.json, name="test_get_events_by_officer_success_with_null_dates")  # type: ignore[attr-defined]
            mock_get_events.assert_called_with(
                "hashhash",
                [build_test_metric_3(StateCode.US_PA).name],
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
            metrics=[build_test_metric_3(StateCode.US_PA)],
        )

        mock_get_officer_entity.return_value = None

        response = self.test_client.get(
            "/outliers/US_PA/officer/invalidhash?period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            response.json,
            {"message": "Officer with pseudonymized id not found: invalidhash"},
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.supervisor_exists_with_external_id",
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
        mock_supervisor_exists: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101"
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_3(StateCode.US_PA)],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Guts",
            avg_daily_population=10.0,
            include_in_outcomes=True,
        )

        # The supervisor exists but doesn't match the supervisor of the officer
        mock_supervisor_exists.return_value = True

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
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.supervisor_exists_with_external_id",
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
        mock_supervisor_exists: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa",
            "101",
            can_access_all_supervisors=True,
            # Set workflows permission to True,
            can_access_supervision_workflows=True,
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_3(StateCode.US_PA)],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Guts",
            avg_daily_population=10.0,
            include_in_outcomes=True,
        )

        mock_supervisor_exists.return_value = False

        response = self.test_client.get(
            "/outliers/US_PA/officer/hashhash?period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_get_officer_entity.assert_called_with(
            "hashhash",
            InsightsCaseloadCategoryType.ALL,
            # include_workflows_info is True when relevant permission is set
            True,
            None,
            datetime.strptime("2023-05-01", "%Y-%m-%d"),
        )
        self.snapshot.assert_match(response.json, name="test_get_officer_success")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.supervisor_exists_with_external_id",
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
    def test_get_officer_without_workflows_permissions(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer_entity: MagicMock,
        mock_supervisor_exists: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa",
            "101",
            can_access_all_supervisors=True,
            # Set workflows permission to false
            can_access_supervision_workflows=False,
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_3(StateCode.US_PA)],
        )

        mock_get_officer_entity.return_value = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Guts",
            avg_daily_population=10.0,
            include_in_outcomes=True,
        )

        mock_supervisor_exists.return_value = False

        response = self.test_client.get(
            "/outliers/US_PA/officer/hashhash?period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_get_officer_entity.assert_called_with(
            "hashhash",
            InsightsCaseloadCategoryType.ALL,
            # include_workflows_info is False when relevant permission is not set
            False,
            None,
            datetime.strptime("2023-05-01", "%Y-%m-%d"),
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_outcomes_officer_not_found(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_pa")
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_3(StateCode.US_PA)],
        )

        mock_get_officer.return_value = None

        response = self.test_client.get(
            "/outliers/US_PA/officer/invalidhash/outcomes?period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            response.json,
            {"message": "Officer with pseudonymized id not found: invalidhash"},
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.supervisor_exists_with_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_outcomes_mismatched_supervisor(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer: MagicMock,
        mock_supervisor_exists: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101"
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_3(StateCode.US_PA)],
        )

        mock_get_officer.return_value = SupervisionOfficer(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
        )

        # The supervisor exists but doesn't match the supervisor of the officer (102)
        mock_supervisor_exists.return_value = True

        response = self.test_client.get(
            "/outliers/US_PA/officer/hashhash/outcomes?period_end_date=2023-05-01",
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
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_outcomes",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.supervisor_exists_with_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_outcomes_not_found(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer: MagicMock,
        mock_supervisor_exists: MagicMock,
        mock_get_outcomes: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101"
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_3(StateCode.US_PA)],
        )

        mock_get_officer.return_value = SupervisionOfficer(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="101",
            supervisor_external_ids=["101"],
        )

        mock_get_outcomes.return_value = None
        mock_supervisor_exists.return_value = True

        response = self.test_client.get(
            "/outliers/US_PA/officer/hashhash/outcomes?period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            response.json,
            {
                "message": "Outcomes info not found for officer with pseudonymized id: hashhash",
            },
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_outcomes",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.supervisor_exists_with_external_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_outliers_backend_config",
    )
    def test_get_outcomes_success(
        self,
        mock_config: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer: MagicMock,
        mock_supervisor_exists: MagicMock,
        mock_get_outcomes: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_pa", "101", can_access_all_supervisors=True
        )
        mock_enabled_states.return_value = ["US_PA"]

        mock_config.return_value = OutliersBackendConfig(
            metrics=[build_test_metric_3(StateCode.US_PA)],
        )

        mock_get_officer.return_value = SupervisionOfficer(
            full_name=PersonName(**{"given_names": "OLIVIA", "surname": "RODRIGO"}),
            external_id="123",
            pseudonymized_id="hashhash",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
        )

        mock_get_outcomes.return_value = SupervisionOfficerOutcomes(
            external_id="123",
            pseudonymized_id="hashhash",
            caseload_category="ALL",
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
            top_x_pct_metrics=[
                {
                    "metric_id": "incarceration_starts_and_inferred",
                    "top_x_pct": 10,
                }
            ],
        )

        mock_supervisor_exists.return_value = False
        response = self.test_client.get(
            "/outliers/US_PA/officer/hashhash/outcomes?period_end_date=2023-05-01",
            headers={"Origin": "http://localhost:3000"},
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.snapshot.assert_match(response.json, name="test_get_outcomes_success")  # type: ignore[attr-defined]

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
            supervision_unit="central",
            supervision_location_for_list_page="region 2",
            supervision_location_for_supervisor_page="unit 2",
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
            supervision_unit="central",
            supervision_location_for_list_page="region 2",
            supervision_location_for_supervisor_page="unit 2",
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
            supervision_unit="central",
            supervision_location_for_list_page="region 2",
            supervision_location_for_supervisor_page="unit 2",
            email="supervisor2@recidiviz.org",
            has_outliers=True,
        )

        # Our PATCH endpoint returns the updated entity, so we can compare its response directly
        # without having to make a separate GET
        response = self.test_client.patch(
            "/outliers/US_PA/user-info/hashhash",
            headers={"Origin": "http://localhost:3000"},
            json={
                "hasSeenOnboarding": True,
            },
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
            headers={
                "Origin": "http://localhost:3000",
                "Content-Type": "application/json",
            },
            json={},
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
        self.maxDiff = None
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="101", pseudonymized_id="hashhash"
        )
        mock_enabled_states.return_value = ["US_PA"]

        response = self.test_client.patch(
            "/outliers/US_PA/user-info/hashhash",
            headers={"Origin": "http://localhost:3000"},
            json={"fakeKey": True},
        )

        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.json,
            {
                "message": "Invalid request body. Expected keys: ['hasDismissedDataUnavailableNote', 'hasDismissedRateOver100PercentNote', 'hasSeenOnboarding']. Found keys: ['fakeKey']",
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
                "message": "Invalid request body. Expected keys: ['hasDismissedDataUnavailableNote', 'hasDismissedRateOver100PercentNote', 'hasSeenOnboarding']. Found keys: ['notARealKey']",
            },
        )

    @freezegun.freeze_time(datetime(2024, 8, 20, 0, 0, 0, 0))
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_patch_action_strategy_success(
        self,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="102", pseudonymized_id="hashhash"
        )
        mock_enabled_states.return_value = ["US_PA"]

        # The PATCH endpoint returns the new event
        response = self.test_client.patch(
            "/outliers/US_PA/action_strategies/hashhash",
            headers={"Origin": "http://localhost:3000"},
            json={
                "userPseudonymizedId": "hashhash",
                "officerPseudonymizedId": "officerHash",
                "actionStrategy": ActionStrategyType.ACTION_STRATEGY_OUTLIER_ABSCONSION.value,
            },
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.snapshot.assert_match(response.json, name="test_patch_action_strategy_success")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_patch_action_strategy_mismatch(
        self,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="101", pseudonymized_id="hash-101"
        )
        mock_enabled_states.return_value = ["US_PA"]

        response = self.test_client.patch(
            "/outliers/US_PA/action_strategies/hashhash",
            headers={"Origin": "http://localhost:3000"},
            json={
                "userPseudonymizedId": "hashhash",
                "officerPseudonymizedId": "officerHash",
                "actionStrategy": ActionStrategyType.ACTION_STRATEGY_OUTLIER_ABSCONSION.value,
            },
        )

        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(
            response.json,
            {
                "message": "Non-recidiviz user with pseudonymized_id hash-101 is attempting to update action strategies for a user that isn't themselves: hashhash"
            },
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_patch_action_strategy_missing_request_body(
        self,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="101", pseudonymized_id="hashhash"
        )
        mock_enabled_states.return_value = ["US_PA"]

        response = self.test_client.patch(
            "/outliers/US_PA/action_strategies/hashhash",
            headers={
                "Origin": "http://localhost:3000",
                "Content-Type": "application/json",
            },
            json={},
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
    def test_patch_action_strategy_invalid_request(
        self,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.maxDiff = None
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="101", pseudonymized_id="hashhash"
        )
        mock_enabled_states.return_value = ["US_PA"]

        response = self.test_client.patch(
            "/outliers/US_PA/action_strategies/hashhash",
            headers={"Origin": "http://localhost:3000"},
            json={"fakeKey": True},
        )

        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.json,
            {
                "message": "Invalid request body. Expected keys: ['actionStrategy', 'officerPseudonymizedId', 'userPseudonymizedId']. Found keys: ['fakeKey']",
            },
        )

    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_patch_action_strategy_without_officer_pseudonymized_id(
        self,
        mock_enabled_states: MagicMock,
    ) -> None:
        self.maxDiff = None
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa", external_id="101", pseudonymized_id="hashhash"
        )
        mock_enabled_states.return_value = ["US_PA"]

        response = self.test_client.patch(
            "/outliers/US_PA/action_strategies/hashhash",
            headers={"Origin": "http://localhost:3000"},
            json={
                "userPseudonymizedId": "hashhash",
                "actionStrategy": ActionStrategyType.ACTION_STRATEGY_60_PERC_OUTLIERS.value,
            },
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)

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
            metrics=[build_test_metric_3(StateCode.US_PA)],
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
            metrics=[build_test_metric_3(StateCode.US_PA)],
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
            supervision_unit="central",
            supervision_location_for_list_page="region 2",
            supervision_location_for_supervisor_page="unit 2",
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
            metrics=[build_test_metric_3(StateCode.US_PA)],
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
            supervision_unit="central",
            supervision_location_for_list_page="region 2",
            supervision_location_for_supervisor_page="unit 2",
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
            metrics=[build_test_metric_3(StateCode.US_PA)],
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
            supervision_unit="central",
            supervision_location_for_list_page="region 2",
            supervision_location_for_supervisor_page="unit 2",
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
            metrics=[build_test_metric_3(StateCode.US_PA)],
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
            supervision_unit="central",
            supervision_location_for_list_page="region 2",
            supervision_location_for_supervisor_page="unit 2",
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
            metrics=[build_test_metric_3(StateCode.US_PA)],
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
            supervision_unit="central",
            supervision_location_for_list_page="region 2",
            supervision_location_for_supervisor_page="unit 2",
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
            metrics=[build_test_metric_3(StateCode.US_PA)],
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
            supervision_unit="central",
            supervision_location_for_list_page="region 2",
            supervision_location_for_supervisor_page="unit 2",
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
            metrics=[build_test_metric_3(StateCode.US_PA)],
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
            supervision_unit="central",
            supervision_location_for_list_page="region 2",
            supervision_location_for_supervisor_page="unit 2",
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
            metrics=[build_test_metric_3(StateCode.US_PA)],
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
            supervision_unit="central",
            supervision_location_for_list_page="region 2",
            supervision_location_for_supervisor_page="unit 2",
            email="supervisor2@recidiviz.org",
            has_outliers=True,
        )

        response = self.test_client.get(
            f"/outliers/US_PA/client/{client_hash}",
            headers={"Origin": "http://localhost:3000"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.snapshot.assert_match(response.json, name="test_get_client_success_for_csg_user")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_vitals_metrics_for_supervision_officer_supervisor",
    )
    def test_get_vitals_metrics_for_supervisor(
        self, mock_get_vitals: MagicMock, mock_enabled_states: MagicMock
    ) -> None:
        # Mock the officer retrieval
        pseudo_id = "officerhash1"
        external_id = "1"

        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa",
            external_id=external_id,
            pseudonymized_id=pseudo_id,
            allowed_states=["US_PA"],
        )

        mock_enabled_states.return_value = ["US_PA"]

        mock_vitals = [
            VitalsMetric(
                metric_id=TIMELY_CONTACT.metric_id,
                vitals_metrics=[
                    SupervisionOfficerVitalsEntity(
                        officer_pseudonymized_id="officerhash1",
                        metric_value=95.0,
                        metric_30d_delta=-3.0,
                    ),
                    SupervisionOfficerVitalsEntity(
                        officer_pseudonymized_id="officerhash2",
                        metric_value=80.0,
                        metric_30d_delta=-17.0,
                    ),
                ],
            ),
            VitalsMetric(
                metric_id=TIMELY_RISK_ASSESSMENT.metric_id,
                vitals_metrics=[
                    SupervisionOfficerVitalsEntity(
                        officer_pseudonymized_id="officerhash1",
                        metric_value=100.0,
                        metric_30d_delta=0.0,
                    ),
                    SupervisionOfficerVitalsEntity(
                        officer_pseudonymized_id="officerhash2",
                        metric_value=94.0,
                        metric_30d_delta=0.0,
                    ),
                ],
            ),
        ]

        mock_get_vitals.return_value = mock_vitals

        response = self.test_client.get(
            f"outliers/us_pa/supervisor/{pseudo_id}/vitals",
            headers={"Origin": "http://localhost:3000"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.snapshot.assert_match(response.json, name="test_vitals_metrics_for_supervisor")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_vitals_metrics_for_supervision_officer_supervisor",
    )
    def test_get_vitals_metrics_for_supervisor_when_can_access_all_supervisors(
        self, mock_get_vitals: MagicMock, mock_enabled_states: MagicMock
    ) -> None:
        # Mock the officer retrieval
        user_pseudo_id = "userhash1"
        external_id = "1"

        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa",
            external_id=external_id,
            pseudonymized_id=user_pseudo_id,
            allowed_states=["US_PA"],
            can_access_all_supervisors=True,
        )

        mock_enabled_states.return_value = ["US_PA"]

        mock_vitals = [
            VitalsMetric(
                metric_id=TIMELY_CONTACT.metric_id,
                vitals_metrics=[
                    SupervisionOfficerVitalsEntity(
                        officer_pseudonymized_id="officerhash1",
                        metric_value=95.0,
                        metric_30d_delta=-3.0,
                    ),
                    SupervisionOfficerVitalsEntity(
                        officer_pseudonymized_id="officerhash2",
                        metric_value=80.0,
                        metric_30d_delta=-17.0,
                    ),
                ],
            ),
            VitalsMetric(
                metric_id=TIMELY_RISK_ASSESSMENT.metric_id,
                vitals_metrics=[
                    SupervisionOfficerVitalsEntity(
                        officer_pseudonymized_id="officerhash1",
                        metric_value=100.0,
                        metric_30d_delta=0.0,
                    ),
                    SupervisionOfficerVitalsEntity(
                        officer_pseudonymized_id="officerhash2",
                        metric_value=94.0,
                        metric_30d_delta=0.0,
                    ),
                ],
            ),
        ]

        mock_get_vitals.return_value = mock_vitals

        response = self.test_client.get(
            "outliers/us_pa/supervisor/some_pseudo_id/vitals",
            headers={"Origin": "http://localhost:3000"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.snapshot.assert_match(response.json, name="get_vitals_metrics_for_supervisor_when_can_access_all_supervisors")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_vitals_metrics_for_supervisor_unauthorized(
        self, mock_enabled_states: MagicMock
    ) -> None:
        # Mock the officer retrieval
        pseudo_id = "officerhash1"
        external_id = "1"

        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa",
            external_id=external_id,
            pseudonymized_id=pseudo_id,
            allowed_states=["US_PA"],
        )

        mock_enabled_states.return_value = ["US_PA"]

        response = self.test_client.get(
            "outliers/us_pa/supervisor/unauthorized_pseudo_id/vitals",
            headers={"Origin": "http://localhost:3000"},
        )
        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_vitals_metrics_for_supervision_officer",
    )
    def test_get_vitals_metrics_for_officer(
        self,
        mock_get_vitals: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer: MagicMock,
    ) -> None:
        # Mock the officer retrieval
        supervisor_pseudo_id = "supervisorhash1"
        supervisor_external_id = "1"
        officer_pseudo_id = "officerhash2"
        officer_external_id = "2"
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa",
            external_id=supervisor_external_id,
            pseudonymized_id=supervisor_pseudo_id,
            allowed_states=["US_PA"],
        )

        mock_enabled_states.return_value = ["US_PA"]

        mock_get_officer.return_value = SupervisionOfficerEntity(
            full_name=PersonName(
                given_names="John", surname="Doe"
            ),  # Provide a valid PersonName
            pseudonymized_id=officer_pseudo_id,
            external_id=officer_external_id,
            supervisor_external_id=supervisor_external_id,
            supervisor_external_ids=[supervisor_external_id],
            district="Some District",
            avg_daily_population=5,
            include_in_outcomes=True,
        )

        mock_vitals = [
            VitalsMetric(
                metric_id=TIMELY_CONTACT.metric_id,
                vitals_metrics=[
                    SupervisionOfficerVitalsEntity(
                        officer_pseudonymized_id="officerhash2",
                        metric_value=80.0,
                        metric_30d_delta=-17.0,
                    ),
                ],
            ),
            VitalsMetric(
                metric_id=TIMELY_RISK_ASSESSMENT.metric_id,
                vitals_metrics=[
                    SupervisionOfficerVitalsEntity(
                        officer_pseudonymized_id="officerhash2",
                        metric_value=94.0,
                        metric_30d_delta=0.0,
                    ),
                ],
            ),
        ]

        mock_get_vitals.return_value = mock_vitals

        response = self.test_client.get(
            f"outliers/us_pa/officer/{officer_pseudo_id}/vitals",
            headers={"Origin": "http://localhost:3000"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.snapshot.assert_match(response.json, name="test_get_vitals_metrics_for_officer")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_vitals_metrics_for_supervision_officer",
    )
    def test_get_vitals_metrics_for_officer_when_can_access_all_supervisors(
        self,
        mock_get_vitals: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_officer: MagicMock,
    ) -> None:
        # Mock the officer retrieval
        supervisor_pseudo_id = "supervisorhash3"
        supervisor_external_id = "3"
        officer_pseudo_id = "officerhash2"
        officer_external_id = "2"
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa",
            external_id="some_id",
            pseudonymized_id=supervisor_pseudo_id + "some_pseudo",
            allowed_states=["US_PA"],
            can_access_all_supervisors=True,
        )

        mock_enabled_states.return_value = ["US_PA"]

        mock_get_officer.return_value = SupervisionOfficerEntity(
            full_name=PersonName(
                given_names="John", surname="Doe"
            ),  # Provide a valid PersonName
            pseudonymized_id=officer_pseudo_id,
            external_id=officer_external_id,
            supervisor_external_id=supervisor_external_id,
            supervisor_external_ids=[supervisor_external_id],
            district="Some District",
            avg_daily_population=5,
            include_in_outcomes=True,
        )

        mock_vitals = [
            VitalsMetric(
                metric_id=TIMELY_CONTACT.metric_id,
                vitals_metrics=[
                    SupervisionOfficerVitalsEntity(
                        officer_pseudonymized_id="officerhash2",
                        metric_value=80.0,
                        metric_30d_delta=-17.0,
                    ),
                ],
            ),
            VitalsMetric(
                metric_id=TIMELY_RISK_ASSESSMENT.metric_id,
                vitals_metrics=[
                    SupervisionOfficerVitalsEntity(
                        officer_pseudonymized_id="officerhash2",
                        metric_value=94.0,
                        metric_30d_delta=0.0,
                    ),
                ],
            ),
        ]

        mock_get_vitals.return_value = mock_vitals

        response = self.test_client.get(
            f"outliers/us_pa/officer/{officer_pseudo_id}/vitals",
            headers={"Origin": "http://localhost:3000"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.snapshot.assert_match(response.json, name="get_vitals_metrics_for_officer_when_can_access_all_supervisors")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_vitals_metrics_for_officer_unauthorized(
        self,
        mock_enabled_states: MagicMock,
        mock_get_officer: MagicMock,
    ) -> None:
        # Mock the officer retrieval
        supervisor_pseudo_id = "supervisorhash1"
        supervisor_external_id = "1"
        officer_pseudo_id = "officerhash2"
        officer_external_id = "2"
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa",
            external_id=supervisor_external_id,
            pseudonymized_id=supervisor_pseudo_id,
            allowed_states=["US_PA"],
        )

        mock_enabled_states.return_value = ["US_PA"]

        mock_get_officer.return_value = SupervisionOfficerEntity(
            full_name=PersonName(
                given_names="John", surname="Doe"
            ),  # Provide a valid PersonName
            pseudonymized_id=officer_pseudo_id,
            external_id=officer_external_id,
            supervisor_external_id=supervisor_external_id,
            supervisor_external_ids=["authorized_id"],
            district="Some District",
            avg_daily_population=5,
            include_in_outcomes=True,
        )

        response = self.test_client.get(
            f"outliers/us_pa/officer/{officer_pseudo_id}/vitals",
            headers={"Origin": "http://localhost:3000"},
        )
        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)

    @patch(
        "recidiviz.case_triage.outliers.outliers_routes.OutliersQuerier.get_supervision_officer_from_pseudonymized_id",
    )
    @patch(
        "recidiviz.case_triage.outliers.outliers_authorization.get_outliers_enabled_states",
    )
    def test_get_vitals_metrics_for_officer_not_found(
        self,
        mock_enabled_states: MagicMock,
        mock_get_officer: MagicMock,
    ) -> None:
        # Mock the officer retrieval
        supervisor_pseudo_id = "supervisorhash1"
        supervisor_external_id = "1"
        officer_pseudo_id = "officerhash2"
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="us_pa",
            external_id=supervisor_external_id,
            pseudonymized_id=supervisor_pseudo_id,
            allowed_states=["US_PA"],
        )

        mock_enabled_states.return_value = ["US_PA"]

        mock_get_officer.return_value = None

        response = self.test_client.get(
            f"outliers/us_pa/officer/{officer_pseudo_id}/vitals",
            headers={"Origin": "http://localhost:3000"},
        )
        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
