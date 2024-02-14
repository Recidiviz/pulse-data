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
from flask import Flask
from flask_smorest import Api

from recidiviz.admin_panel.all_routes import admin_panel_blueprint
from recidiviz.admin_panel.routes.outliers import outliers_blueprint
from recidiviz.persistence.database.schema.outliers.schema import Configuration
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.outliers.querier_test import load_model_fixture
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers


@patch(
    "recidiviz.admin_panel.routes.outliers.get_authenticated_user_email",
    MagicMock(return_value=("test-user@recidiviz.org", None)),
)
@pytest.mark.uses_db
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

        # Set up database
        self.database_key = SQLAlchemyDatabaseKey(SchemaType.OUTLIERS, db_name="us_pa")
        local_persistence_helpers.use_on_disk_postgresql_database(self.database_key)

        with SessionFactory.using_database(self.database_key) as session:
            # Restart the sequence in tests as per https://stackoverflow.com/questions/46841912/sqlalchemy-revert-auto-increment-during-testing-pytest
            session.execute("""ALTER SEQUENCE configurations_id_seq RESTART WITH 1;""")

            for config in load_model_fixture(Configuration):
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

        expected = [
            {
                "atOrBelowRateLabel": "At or below statewide rate",
                "exclusionReasonDescription": None,
                "featureVariant": None,
                "id": 1,
                "learnMoreUrl": "fake.com",
                "noneAreOutliersLabel": "are outliers",
                "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
                "status": "ACTIVE",
                "supervisionDistrictLabel": "district",
                "supervisionDistrictManagerLabel": "district manager",
                "supervisionJiiLabel": "client",
                "supervisionOfficerLabel": "agent",
                "supervisionSupervisorLabel": "supervisor",
                "supervisionUnitLabel": "unit",
                "updatedAt": "2024-01-26T13:30:00",
                "updatedBy": "alexa@recidiviz.org",
                "worseThanRateLabel": "Far worse than statewide rate",
            },
            {
                "atOrBelowRateLabel": "At or below statewide rate",
                "exclusionReasonDescription": None,
                "featureVariant": "fv2",
                "id": 4,
                "learnMoreUrl": "fake.com",
                "noneAreOutliersLabel": "are outliers",
                "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
                "status": "ACTIVE",
                "supervisionDistrictLabel": "district",
                "supervisionDistrictManagerLabel": "district manager",
                "supervisionJiiLabel": "client",
                "supervisionOfficerLabel": "officer",
                "supervisionSupervisorLabel": "supervisor",
                "supervisionUnitLabel": "unit",
                "updatedAt": "2024-01-01T13:30:03",
                "updatedBy": "dana2@recidiviz.org",
                "worseThanRateLabel": "Far worse than statewide rate",
            },
            {
                "atOrBelowRateLabel": "At or below statewide rate",
                "exclusionReasonDescription": None,
                "featureVariant": "fv1",
                "id": 3,
                "learnMoreUrl": "fake.com",
                "noneAreOutliersLabel": "are outliers",
                "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
                "status": "ACTIVE",
                "supervisionDistrictLabel": "district",
                "supervisionDistrictManagerLabel": "district manager",
                "supervisionJiiLabel": "client",
                "supervisionOfficerLabel": "officer",
                "supervisionSupervisorLabel": "supervisor",
                "supervisionUnitLabel": "unit",
                "updatedAt": "2024-01-01T13:30:02",
                "updatedBy": "dana1@recidiviz.org",
                "worseThanRateLabel": "Far worse than statewide rate",
            },
            {
                "atOrBelowRateLabel": "At or below statewide rate",
                "exclusionReasonDescription": None,
                "featureVariant": None,
                "id": 2,
                "learnMoreUrl": "fake.com",
                "noneAreOutliersLabel": "are outliers",
                "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
                "status": "INACTIVE",
                "supervisionDistrictLabel": "district",
                "supervisionDistrictManagerLabel": "district manager",
                "supervisionJiiLabel": "client",
                "supervisionOfficerLabel": "officer",
                "supervisionSupervisorLabel": "supervisor",
                "supervisionUnitLabel": "unit",
                "updatedAt": "2024-01-01T13:30:01",
                "updatedBy": "fake@recidiviz.org",
                "worseThanRateLabel": "Far worse than statewide rate",
            },
        ]

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertEqual(json.loads(response.data), expected)

    ########
    # POST /outliers/<state_code_str>/configurations
    ########

    @patch(
        "recidiviz.admin_panel.routes.outliers.get_outliers_enabled_states",
    )
    @freezegun.freeze_time(datetime(2024, 2, 1, 0, 0, 0, 0))
    def test_add_configuration(self, mock_enabled_states: MagicMock) -> None:
        mock_enabled_states.return_value = ["US_PA"]

        expected = [
            {
                "atOrBelowRateLabel": "At or below statewide rate",
                "exclusionReasonDescription": None,
                "featureVariant": None,
                "id": 1,
                "learnMoreUrl": "fake.com",
                "noneAreOutliersLabel": "are outliers",
                "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
                "status": "ACTIVE",
                "supervisionDistrictLabel": "district",
                "supervisionDistrictManagerLabel": "district manager",
                "supervisionJiiLabel": "client",
                "supervisionOfficerLabel": "agent",
                "supervisionSupervisorLabel": "supervisor",
                "supervisionUnitLabel": "unit",
                "updatedAt": "2024-01-26T13:30:00",
                "updatedBy": "alexa@recidiviz.org",
                "worseThanRateLabel": "Far worse than statewide rate",
            },
            {
                "atOrBelowRateLabel": "At or below statewide rate",
                "exclusionReasonDescription": None,
                "featureVariant": "fv2",
                "id": 4,
                "learnMoreUrl": "fake.com",
                "noneAreOutliersLabel": "are outliers",
                "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
                "status": "ACTIVE",
                "supervisionDistrictLabel": "district",
                "supervisionDistrictManagerLabel": "district manager",
                "supervisionJiiLabel": "client",
                "supervisionOfficerLabel": "officer",
                "supervisionSupervisorLabel": "supervisor",
                "supervisionUnitLabel": "unit",
                "updatedAt": "2024-01-01T13:30:03",
                "updatedBy": "dana2@recidiviz.org",
                "worseThanRateLabel": "Far worse than statewide rate",
            },
            {
                "atOrBelowRateLabel": "At or below statewide rate",
                "exclusionReasonDescription": None,
                "featureVariant": "fv1",
                "id": 3,
                "learnMoreUrl": "fake.com",
                "noneAreOutliersLabel": "are outliers",
                "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
                "status": "ACTIVE",
                "supervisionDistrictLabel": "district",
                "supervisionDistrictManagerLabel": "district manager",
                "supervisionJiiLabel": "client",
                "supervisionOfficerLabel": "officer",
                "supervisionSupervisorLabel": "supervisor",
                "supervisionUnitLabel": "unit",
                "updatedAt": "2024-01-01T13:30:02",
                "updatedBy": "dana1@recidiviz.org",
                "worseThanRateLabel": "Far worse than statewide rate",
            },
            {
                "atOrBelowRateLabel": "At or below statewide rate",
                "exclusionReasonDescription": None,
                "featureVariant": None,
                "id": 2,
                "learnMoreUrl": "fake.com",
                "noneAreOutliersLabel": "are outliers",
                "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
                "status": "INACTIVE",
                "supervisionDistrictLabel": "district",
                "supervisionDistrictManagerLabel": "district manager",
                "supervisionJiiLabel": "client",
                "supervisionOfficerLabel": "officer",
                "supervisionSupervisorLabel": "supervisor",
                "supervisionUnitLabel": "unit",
                "updatedAt": "2024-01-01T13:30:01",
                "updatedBy": "fake@recidiviz.org",
                "worseThanRateLabel": "Far worse than statewide rate",
            },
        ]
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
            },
        )

        response = self.client.get(self.configurations, headers=self.headers)

        self.assertEqual(json.loads(response.data), expected)

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
            },
        )
        self.assertEqual(HTTPStatus.BAD_REQUEST, result.status_code)
