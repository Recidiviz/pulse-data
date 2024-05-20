# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for insights-specific routes in admin_panel/routes/line_staff_tools.py"""
import json
from datetime import datetime
from http import HTTPStatus
from typing import Any, Dict, Optional
from unittest import TestCase
from unittest.mock import MagicMock, patch

import flask
import freezegun
import pytest
import responses
from flask import Flask
from flask_smorest import Api

from recidiviz.admin_panel.all_routes import admin_panel_blueprint
from recidiviz.admin_panel.routes.outliers import outliers_blueprint
from recidiviz.persistence.database.schema.outliers.schema import Configuration
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.outliers.querier_test import load_model_from_csv_fixture
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers


@patch(
    "recidiviz.admin_panel.routes.outliers.get_authenticated_user_email",
    MagicMock(return_value=("test-user@recidiviz.org", None)),
)
@pytest.mark.uses_db
@pytest.mark.usefixtures("snapshottest_snapshot")
class OutliersAdminPanelEndpointTests(TestCase):
    """Tests of our Flask endpoints"""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    def setUp(self) -> None:
        # Set up app
        self.app = Flask(__name__)
        self.app.register_blueprint(admin_panel_blueprint)
        api = Api(
            self.app,
            spec_kwargs={
                "title": "default",
                "version": "1.0.0",
                "openapi_version": "3.1.0",
            },
        )
        api.register_blueprint(outliers_blueprint, url_prefix="/admin/outliers")
        self.client = self.app.test_client()

        self.headers: Dict[str, Dict[Any, Any]] = {"x-goog-iap-jwt-assertion": {}}

        with self.app.test_request_context():
            self.enabled_states = flask.url_for(
                "outliers.EnabledStatesAPI",
            )
            self.configurations = flask.url_for(
                "outliers.ConfigurationsAPI", state_code_str="US_PA"
            )
            self.deactivate = lambda config_id=None: flask.url_for(
                "outliers.DeactivateConfigurationByIdAPI",
                state_code_str="US_PA",
                config_id=config_id,
            )
            self.promote_prod = lambda config_id=None: flask.url_for(
                "outliers.PromoteToProdConfigurationsAPI",
                state_code_str="US_PA",
                config_id=config_id,
            )
            self.promoteDefault = lambda config_id=None: flask.url_for(
                "outliers.PromoteToDefaultConfigurationsAPI",
                state_code_str="US_PA",
                config_id=config_id,
            )
            self.reactivate = lambda config_id=None: flask.url_for(
                "outliers.ReactivateConfigurationsAPI",
                state_code_str="US_PA",
                config_id=config_id,
            )

        # Set up database
        self.database_key = SQLAlchemyDatabaseKey(SchemaType.OUTLIERS, db_name="us_pa")
        local_persistence_helpers.use_on_disk_postgresql_database(self.database_key)

        with SessionFactory.using_database(self.database_key) as session:
            # Restart the sequence in tests as per https://stackoverflow.com/questions/46841912/sqlalchemy-revert-auto-increment-during-testing-pytest
            session.execute("""ALTER SEQUENCE configurations_id_seq RESTART WITH 1;""")

            for config in load_model_from_csv_fixture(Configuration):
                session.add(Configuration(**config))

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    ########
    # GET /outliers/enabled_state_codes
    ########

    @patch(
        "recidiviz.admin_panel.routes.outliers.get_outliers_enabled_states",
    )
    def test_enabled_states(self, mock_enabled_states: MagicMock) -> None:
        mock_enabled_states.return_value = ["US_MI"]

        response = self.client.get(
            self.enabled_states,
            headers=self.headers,
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertEqual(response.json, [{"code": "US_MI", "name": "Michigan"}])

    ########
    # GET /outliers/<state_code_str>/configurations
    ########

    @patch(
        "recidiviz.admin_panel.routes.outliers.get_outliers_enabled_states",
    )
    def test_get_configurations(self, mock_enabled_states: MagicMock) -> None:
        mock_enabled_states.return_value = ["US_PA"]
        response = self.client.get(self.configurations, headers=self.headers)

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.snapshot.assert_match(json.loads(response.data), name="test_get_configurations")  # type: ignore[attr-defined]

    ########
    # POST /outliers/<state_code_str>/configurations
    ########

    @patch(
        "recidiviz.admin_panel.routes.outliers.get_outliers_enabled_states",
    )
    @freezegun.freeze_time(datetime(2024, 2, 1, 0, 0, 0, 0))
    def test_add_configuration(self, mock_enabled_states: MagicMock) -> None:
        mock_enabled_states.return_value = ["US_PA"]

        self.client.post(
            self.configurations,
            headers=self.headers,
            json={
                "featureVariant": "fv1",
                "supervisionDistrictLabel": "district",
                "supervisionSupervisorLabel": "supervisor",
                "supervisionJiiLabel": "client",
                "supervisionOfficerLabel": "officer",
                "supervisionUnitLabel": "unit",
                "supervisionDistrictManagerLabel": "district manager",
                "learnMoreUrl": "fake.com",
                "noneAreOutliersLabel": "are outliers",
                "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
                "worseThanRateLabel": "Far worse than statewide rate",
                "atOrBelowRateLabel": "At or below statewide rate",
                "exclusionReasonDescription": None,
                "abscondersLabel": "absconders",
                "atOrAboveRateLabel": "At or above statewide rate",
            },
        )

        response = self.client.get(self.configurations, headers=self.headers)
        self.snapshot.assert_match(json.loads(response.data), name="test_add_configuration")  # type: ignore[attr-defined]

    @patch(
        "recidiviz.admin_panel.routes.outliers.get_outliers_enabled_states",
    )
    def test_add_configuration_bad_request(
        self, mock_enabled_states: MagicMock
    ) -> None:
        mock_enabled_states.return_value = ["US_PA"]
        result = self.client.post(
            self.configurations,
            headers=self.headers,
            json={
                # incorrect type
                "featureVariant": 1,
                "supervisionDistrictLabel": "district",
                "supervisionSupervisorLabel": "supervisor",
                "supervisionJiiLabel": "client",
                "supervisionOfficerLabel": "officer",
                "supervisionUnitLabel": "unit",
                "supervisionDistrictManagerLabel": "district manager",
                "learnMoreUrl": "fake.com",
                "noneAreOutliersLabel": "are outliers",
                "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
                "worseThanRateLabel": "Far worse than statewide rate",
                "atOrBelowRateLabel": "At or below statewide rate",
                "exclusionReasonDescription": None,
                "abscondersLabel": "absconders",
                "atOrAboveRateLabel": "At or above statewide rate",
            },
        )
        self.assertEqual(HTTPStatus.BAD_REQUEST, result.status_code)

    @patch(
        "recidiviz.admin_panel.routes.outliers.get_outliers_enabled_states",
    )
    @freezegun.freeze_time(datetime(2024, 2, 1, 0, 0, 0, 0))
    def test_add_configuration_with_updated_by(
        self, mock_enabled_states: MagicMock
    ) -> None:
        mock_enabled_states.return_value = ["US_PA"]

        self.client.post(
            self.configurations,
            headers=self.headers,
            json={
                "updatedBy": "email@gmail.com",
                "featureVariant": "fv1",
                "supervisionDistrictLabel": "district",
                "supervisionSupervisorLabel": "supervisor",
                "supervisionJiiLabel": "client",
                "supervisionOfficerLabel": "officer",
                "supervisionUnitLabel": "unit",
                "supervisionDistrictManagerLabel": "district manager",
                "learnMoreUrl": "fake.com",
                "noneAreOutliersLabel": "are outliers",
                "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
                "worseThanRateLabel": "Far worse than statewide rate",
                "atOrBelowRateLabel": "At or below statewide rate",
                "exclusionReasonDescription": None,
                "abscondersLabel": "absconders",
                "atOrAboveRateLabel": "At or above statewide rate",
            },
        )

        response = self.client.get(self.configurations, headers=self.headers)
        self.snapshot.assert_match(json.loads(response.data), name="test_add_configuration_with_updated_by")  # type: ignore[attr-defined]

    ########
    # PUT /outliers/<state_code_str>/configurations/<config_id_str>/deactivate
    ########

    @patch(
        "recidiviz.admin_panel.routes.outliers.get_outliers_enabled_states",
    )
    def test_deactivate_config_not_found(self, mock_enabled_states: MagicMock) -> None:
        config_id = 6
        mock_enabled_states.return_value = ["US_PA"]
        with self.app.test_request_context():
            result = self.client.put(
                self.deactivate(config_id),
                headers=self.headers,
            )

            self.assertEqual(HTTPStatus.BAD_REQUEST, result.status_code)
            error_message = f"Configuration with id {config_id} does not exist"
            self.assertEqual(error_message, json.loads(result.data)["message"])

    @patch(
        "recidiviz.admin_panel.routes.outliers.get_outliers_enabled_states",
    )
    def test_deactivate_config_default(self, mock_enabled_states: MagicMock) -> None:
        config_id = 1
        mock_enabled_states.return_value = ["US_PA"]
        with self.app.test_request_context():
            result = self.client.put(
                self.deactivate(config_id),
                headers=self.headers,
            )

            self.assertEqual(HTTPStatus.BAD_REQUEST, result.status_code)
            error_message = (
                "Cannot deactivate the only active default configuration for US_PA"
            )
            self.assertEqual(error_message, json.loads(result.data)["message"])

    @patch(
        "recidiviz.admin_panel.routes.outliers.get_outliers_enabled_states",
    )
    def test_deactivate_config_success(self, mock_enabled_states: MagicMock) -> None:
        config_id = 2
        mock_enabled_states.return_value = ["US_PA"]
        with self.app.test_request_context():
            result = self.client.put(
                self.deactivate(config_id),
                headers=self.headers,
            )

            self.assertEqual(HTTPStatus.OK, result.status_code)
            self.assertEqual(
                f"Configuration {config_id} has been deactivated",
                json.loads(result.data),
            )

    ########
    # POST /outliers/<state_code_str>/configurations/<config_id>/promote/prod
    ########

    @patch("recidiviz.admin_panel.routes.outliers.get_gcp_environment")
    @patch("recidiviz.admin_panel.routes.outliers.fetch_id_token")
    @patch("recidiviz.admin_panel.routes.outliers.in_gcp")
    def test_promote_configuration_success(
        self, in_gcp_mock: MagicMock, fetch_id_token_mock: MagicMock, get_env: MagicMock
    ) -> None:
        get_env.return_value = "staging"
        test_token = "test-token-value"
        in_gcp_mock.return_value = True
        fetch_id_token_mock.return_value = test_token
        config_id = 1
        with self.app.test_request_context(), responses.RequestsMock() as rsps:
            rsps.post(
                "https://admin-panel-prod.recidiviz.org/admin/outliers/US_PA/configurations",
                status=200,
                match=[
                    responses.matchers.header_matcher(
                        {"Authorization": f"Bearer {test_token}"}
                    ),
                    responses.matchers.json_params_matcher(
                        {
                            "learnMoreUrl": "fake.com",
                            "supervisionDistrictLabel": "district",
                            "featureVariant": None,
                            "supervisionDistrictManagerLabel": "district manager",
                            "supervisionJiiLabel": "client",
                            "supervisionOfficerLabel": "agent",
                            "supervisionSupervisorLabel": "supervisor",
                            "supervisionUnitLabel": "unit",
                            "noneAreOutliersLabel": "are outliers",
                            "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
                            "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
                            "worseThanRateLabel": "Far worse than statewide rate",
                            "atOrBelowRateLabel": "At or below statewide rate",
                            "exclusionReasonDescription": "excluded because x",
                            "updatedBy": "test-user@recidiviz.org",
                            "abscondersLabel": "absconders",
                            "atOrAboveRateLabel": "At or above statewide rate",
                        }
                    ),
                ],
            )

            result = self.client.post(
                self.promote_prod(config_id), headers=self.headers
            )
            self.assertEqual(result.status_code, HTTPStatus.OK)
            self.assertEqual(
                json.loads(result.data),
                "Configuration 1 successfully promoted to production",
            )

    @patch("recidiviz.admin_panel.routes.outliers.get_gcp_environment")
    def test_promote_configuration_wrong_env(self, get_env: MagicMock) -> None:
        get_env.return_value = "production"
        # Assumes second non-header row in configurations.csv has status=INACTIVE
        config_id = 2
        with self.app.test_request_context():
            result = self.client.post(
                self.promote_prod(config_id), headers=self.headers
            )
            self.assertEqual(result.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(
                json.loads(result.data)["message"],
                "This endpoint should not be called from production",
            )

    ########
    # POST /outliers/<state_code_str>/configurations/<config_id>/promote/default
    ########

    @freezegun.freeze_time(datetime(2024, 2, 1, 0, 0, 0, 0))
    def test_promote_default_configuration_success(self) -> None:
        config_id = 3

        with self.app.test_request_context():
            result = self.client.post(
                self.promoteDefault(config_id), headers=self.headers
            )
            self.assertEqual(result.status_code, HTTPStatus.OK)
            self.assertEqual(
                json.loads(result.data),
                "Configuration 3 successfully promoted to the default configuration",
            )

            response = self.client.get(self.configurations, headers=self.headers)
            self.snapshot.assert_match(json.loads(response.data), name="test_promote_default_configuration_success")  # type: ignore[attr-defined]

    def test_promote_default_configuration_bad_request(self) -> None:
        # Assumes first non-header row in configurations.csv has fv=None and status=ACTIVE
        config_id = 1
        with self.app.test_request_context():
            result = self.client.post(
                self.promoteDefault(config_id), headers=self.headers
            )
            self.assertEqual(result.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(
                json.loads(result.data)["message"],
                "Configuration 1 is already a default configuration, status is ACTIVE",
            )

    ########
    # POST /outliers/<state_code_str>/configurations/<config_id>/reactivate
    ########

    @freezegun.freeze_time(datetime(2024, 2, 1, 0, 0, 0, 0))
    def test_reactivate_configuration_success(self) -> None:
        config_id = 5
        with self.app.test_request_context():
            result = self.client.post(self.reactivate(config_id), headers=self.headers)
            self.assertEqual(result.status_code, HTTPStatus.OK)
            self.assertEqual(
                json.loads(result.data),
                "Configuration 5 successfully reactivated",
            )

            response = self.client.get(self.configurations, headers=self.headers)
            self.snapshot.assert_match(json.loads(response.data), name="test_reactivate_configuration_success")  # type: ignore[attr-defined]

    def test_reactivate_configuration_bad_request(self) -> None:
        # Assumes first non-header row in configurations.csv has status=ACTIVE
        config_id = 1
        with self.app.test_request_context():
            result = self.client.post(self.reactivate(config_id), headers=self.headers)
            self.assertEqual(result.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(
                json.loads(result.data)["message"],
                "Configuration 1 is already active",
            )
