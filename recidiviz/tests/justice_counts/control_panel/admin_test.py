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
"""Implements tests for the Justice Counts Control Panel backend API."""
import datetime
from typing import Any, Dict

import mock
import pytest
from mock import patch
from sqlalchemy.engine import Engine

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.control_panel.config import Config
from recidiviz.justice_counts.control_panel.server import create_app
from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.metrics import law_enforcement
from recidiviz.justice_counts.metrics.custom_reporting_frequency import (
    CustomReportingFrequency,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.justice_counts.utils.constants import VALID_SYSTEMS
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import (
    Agency,
    AgencyUserAccountAssociation,
    Datapoint,
    DatapointHistory,
    MetricSetting,
    Report,
    Source,
    Spreadsheet,
    SpreadsheetStatus,
    System,
    UserAccount,
    UserAccountInvitationStatus,
)
from recidiviz.tests.auth.utils import get_test_auth0_config
from recidiviz.tests.justice_counts.utils.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)
from recidiviz.tools.justice_counts.copy_over_metric_settings_to_child_agencies import (
    copy_metric_settings,
)
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils.auth.auth0 import passthrough_authorization_decorator
from recidiviz.utils.types import assert_type


@pytest.mark.uses_db
class TestJusticePublisherAdminPanelAPI(JusticeCountsDatabaseTestCase):
    """Implements tests for the Justice Counts Publisher Admin Panel backend API."""

    def setUp(self) -> None:
        self.test_schema_objects = JusticeCountsSchemaTestObjects()

        def mock_create_JC_user(name: str, email: str) -> Dict[str, str]:
            return {
                "name": name,
                "email": email,
                "user_id": f"auth0|1234{name}",
            }

        def mock_delete_JC_user(user_id: str) -> Any:
            return {
                "user_id": user_id,
            }

        mock_auth0_client = mock.Mock()
        mock_auth0_client.create_JC_user.side_effect = mock_create_JC_user
        mock_auth0_client.delete_JC_user.side_effect = mock_delete_JC_user

        self.client_patcher = patch("recidiviz.auth.auth0_client.Auth0")
        self.test_auth0_client = (
            self.client_patcher.start().return_value
        ) = mock_auth0_client
        test_config = Config(
            DB_URL=local_postgres_helpers.on_disk_postgres_db_url(),
            WTF_CSRF_ENABLED=False,
            AUTH_DECORATOR=passthrough_authorization_decorator(),
            AUTH_DECORATOR_ADMIN_PANEL=passthrough_authorization_decorator(),
            AUTH0_CONFIGURATION=get_test_auth0_config(),
            AUTH0_CLIENT=self.test_auth0_client,
            SEGMENT_KEY="fake_segment_key",
        )
        self.app = create_app(config=test_config)
        self.client = self.app.test_client()
        self.app.secret_key = "NOT A SECRET"
        # `flask_sqlalchemy_session` sets the `scoped_session` attribute on the app,
        # even though this is not specified in the types for `app`.
        self.session = self.app.scoped_session  # type: ignore[attr-defined]
        super().setUp()

    def get_engine(self) -> Engine:
        return self.session.get_bind()

    def load_users_and_agencies(self) -> None:
        """Loads three agency and three users to the current session."""
        agency_A = self.test_schema_objects.test_agency_A
        user_A = self.test_schema_objects.test_user_A
        agency_B = self.test_schema_objects.test_agency_B
        user_B = self.test_schema_objects.test_user_B
        agency_C = self.test_schema_objects.test_agency_C
        user_C = self.test_schema_objects.test_user_C
        self.session.add_all([agency_A, user_A, agency_B, user_B, agency_C, user_C])
        self.session.commit()
        self.session.refresh(user_A)
        self.session.refresh(user_B)
        self.session.refresh(user_C)
        self.session.refresh(agency_A)
        self.session.refresh(agency_B)
        self.session.refresh(agency_C)

        user_agency_association_A = AgencyUserAccountAssociation(
            user_account_id=user_A.id,
            agency_id=agency_A.id,
            invitation_status=UserAccountInvitationStatus.PENDING,
        )
        user_agency_association_B = AgencyUserAccountAssociation(
            user_account_id=user_B.id,
            agency_id=agency_B.id,
            invitation_status=UserAccountInvitationStatus.PENDING,
        )
        user_agency_association_C = AgencyUserAccountAssociation(
            user_account_id=user_C.id,
            agency_id=agency_C.id,
            invitation_status=UserAccountInvitationStatus.PENDING,
        )
        self.session.add_all(
            [
                user_agency_association_A,
                user_agency_association_B,
                user_agency_association_C,
            ]
        )
        self.session.commit()
        self.agency_A_id = agency_A.id
        self.agency_B_id = agency_B.id
        self.agency_C_id = agency_C.id
        self.user_A_id = user_A.id
        self.user_B_id = user_B.id
        self.user_B_id = user_B.id

    # UserAccount

    def test_get_all_users(self) -> None:
        self.load_users_and_agencies()
        response = self.client.get("/admin/user")
        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        user_json_list = response_json["users"]
        self.assertEqual(len(user_json_list), 3)
        users = self.session.query(UserAccount).all()
        user_id_to_user = {user.id: user for user in users}

        for user_json in user_json_list:
            db_user = user_id_to_user[(user_json["id"])]
            agency = db_user.agency_assocs[0].agency
            self.assertEqual(user_json["auth0_user_id"], db_user.auth0_user_id)
            self.assertEqual(user_json["name"], db_user.name)
            self.assertEqual(len(user_json["agencies"]), 1)
            self.assertEqual(user_json["agencies"][0]["id"], agency.id)
            self.assertEqual(user_json["agencies"][0]["name"], agency.name)

    def test_get_user(self) -> None:
        self.load_users_and_agencies()
        user = (
            self.session.query(schema.UserAccount)
            .filter(schema.UserAccount.auth0_user_id == "auth0_id_A")
            .one()
        )
        response = self.client.get(f"/admin/user/{user.id}")
        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        self.assertEqual(response_json["name"], user.name)
        self.assertEqual(response_json["id"], user.id)
        self.assertEqual(len(response_json["agencies"]), 1)
        self.assertEqual(response_json["agencies"][0]["name"], "Agency Alpha")

    def test_delete_user(self) -> None:
        self.load_users_and_agencies()
        user = (
            self.session.query(UserAccount)
            .filter(UserAccount.auth0_user_id == "auth0_id_A")
            .one()
        )

        self.session.add(
            Spreadsheet(
                original_name="original_name",
                uploaded_by=user.auth0_user_id,
                standardized_name="standardized_name",
                agency_id=self.agency_A_id,
                system=System.LAW_ENFORCEMENT,
                status=SpreadsheetStatus.UPLOADED,
                uploaded_at=datetime.datetime.now(tz=datetime.timezone.utc),
            )
        )

        # Setup datapoint and datapoint history entries logged under the user.
        datapoint = Datapoint(
            id=1,
            metric_definition_key=law_enforcement.funding.key,
        )
        self.session.add(datapoint)
        self.session.add(
            DatapointHistory(
                user_account_id=user.id,
                datapoint_id=datapoint.id,
                timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            )
        )

        self.session.commit()

        response = self.client.delete(f"/admin/user/{user.id}")
        self.assertEqual(response.status_code, 200)

        # There are still two entries in AgencyUserAccountAssociation but neither are
        # for the deleted user.
        self.assertEqual(2, len(self.session.query(AgencyUserAccountAssociation).all()))
        self.assertEqual(
            0,
            len(
                list(
                    self.session.query(AgencyUserAccountAssociation).filter(
                        AgencyUserAccountAssociation.user_account_id == user.id
                    )
                )
            ),
        )

        # There are still two entries in UserAccount but neither are for the deleted user.
        self.assertEqual(2, len(self.session.query(UserAccount).all()))
        self.assertEqual(
            0,
            len(
                list(self.session.query(UserAccount).filter(UserAccount.id == user.id))
            ),
        )

        # There is still one entry in DatapointHistory but it does not correspond to the
        # deleted user.
        self.assertEqual(1, len(self.session.query(DatapointHistory).all()))
        self.assertEqual(
            0,
            len(
                list(
                    self.session.query(DatapointHistory).filter(
                        DatapointHistory.user_account_id == user.id
                    )
                )
            ),
        )

        # There is still one entry in Spreadsheet but it does not correspond to the
        # deleted user.
        self.assertEqual(1, len(self.session.query(Spreadsheet).all()))
        self.assertEqual(
            0,
            len(
                list(
                    self.session.query(Spreadsheet).filter(
                        Spreadsheet.uploaded_by == user.auth0_user_id
                    )
                )
            ),
        )

        response_json = assert_type(response.json, dict)
        self.assertEqual(response_json["name"], user.name)
        self.assertEqual(response_json["id"], user.id)
        self.assertEqual(len(response_json["agencies"]), 1)
        self.assertEqual(response_json["agencies"][0]["name"], "Agency Alpha")

    def test_delete_agency(self) -> None:
        self.load_users_and_agencies()
        agency_A = (
            self.session.query(Agency).filter(Agency.name == "Agency Alpha").one()
        )

        # Make agency A a superagency and create child agency of agency A. For a
        # superagency, we nullify the super_agency_id field from all of its children.
        # This causes the child agency to no longer be a child agency.
        agency_A.is_superagency = True
        child_agency_1 = Agency(
            name="Agency Alpha Child Agency",
            super_agency_id=agency_A.id,
            systems=["LAW_ENFORCEMENT"],
        )
        self.session.add(child_agency_1)

        # This agency setting will be deleted when agency A is deleted.
        self.session.add(
            schema.AgencySetting(
                source_id=agency_A.id,
                setting_type="HOMEPAGE_URL",
            )
        )
        # This agency setting belongs to a different agency and will not be deleted.
        self.session.add(
            schema.AgencySetting(
                source_id=self.agency_B_id,
                setting_type="HOMEPAGE_URL",
            )
        )

        # This agency jurisdiction will be deleted when agency A is deleted.
        self.session.add(
            schema.AgencyJurisdiction(
                source_id=agency_A.id,
                membership="INCLUDE",
                jurisdiction_id="000000000",
            )
        )
        # This agency jurisdiction belongs to a different agency and will not be deleted.
        self.session.add(
            schema.AgencyJurisdiction(
                source_id=self.agency_B_id,
                membership="INCLUDE",
                jurisdiction_id="000000000",
            )
        )

        # This spreadsheet will be deleted when agency A is deleted.
        self.session.add(
            Spreadsheet(
                original_name="original_name",
                standardized_name="standardized_name",
                agency_id=agency_A.id,
                system=System.LAW_ENFORCEMENT,
                status=SpreadsheetStatus.UPLOADED,
                uploaded_at=datetime.datetime.now(tz=datetime.timezone.utc),
            )
        )
        # This spreadsheet belongs to a different agency and will not be deleted.
        self.session.add(
            Spreadsheet(
                original_name="original_name",
                standardized_name="standardized_name",
                agency_id=self.agency_B_id,
                system=System.LAW_ENFORCEMENT,
                status=SpreadsheetStatus.UPLOADED,
                uploaded_at=datetime.datetime.now(tz=datetime.timezone.utc),
            )
        )

        # This report will be deleted when agency A is deleted.
        self.session.add(
            Report(
                source_id=agency_A.id,
                type="ANNUAL",
                instance="2020 Annual Metrics",
                acquisition_method="CONTROL_PANEL",
                project="JUSTICE_COUNTS_CONTROL_PANEL",
                status="NOT_STARTED",
            )
        )
        # This report belongs to a different agency and will not be deleted.
        self.session.add(
            Report(
                source_id=self.agency_B_id,
                type="ANNUAL",
                instance="2020 Annual Metrics",
                acquisition_method="CONTROL_PANEL",
                project="JUSTICE_COUNTS_CONTROL_PANEL",
                status="NOT_STARTED",
            )
        )

        # Datapoint and datapoint history entries for Agency A that will be deleted.
        datapoint = Datapoint(
            source_id=agency_A.id,
            metric_definition_key=law_enforcement.funding.key,
        )
        self.session.add(datapoint)
        self.session.commit()  # Committing to get the autoincremented datapoint id.

        self.session.add(
            DatapointHistory(
                datapoint_id=datapoint.id,
                timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            )
        )
        self.session.commit()

        # Datapoint and datapoint history entries for Agency B that will NOT be deleted.
        datapoint = Datapoint(
            source_id=self.agency_B_id,
            metric_definition_key=law_enforcement.funding.key,
        )
        self.session.add(datapoint)
        self.session.commit()  # Committing here is necessary to get the datapoint id.
        self.session.add(
            DatapointHistory(
                datapoint_id=datapoint.id,
                timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            )
        )
        self.session.commit()

        response = self.client.delete(f"/admin/agency/{agency_A.id}")
        self.assertEqual(response.status_code, 200)

        # There are three entries in Agency (Agency B, Agency C, and child agency A) and
        # none are for the deleted agency.
        self.assertEqual(3, len(self.session.query(Agency).all()))
        self.assertEqual(
            0,
            len(list(self.session.query(Agency).filter(Agency.name == "Agency Alpha"))),
        )

        # Child agency A has a null value for super_agency_id.
        self.assertEqual(
            self.session.query(Agency)
            .filter(Agency.name == "Agency Alpha Child Agency")
            .one()
            .super_agency_id,
            None,
        )

        # There are still two entries in AgencyUserAccountAssociation but neither are
        # for the deleted agency.
        self.assertEqual(2, len(self.session.query(AgencyUserAccountAssociation).all()))
        self.assertEqual(
            0,
            len(
                list(
                    self.session.query(AgencyUserAccountAssociation).filter(
                        AgencyUserAccountAssociation.agency_id == agency_A.id
                    )
                )
            ),
        )

        # There is only one entry in the AgencySetting table and it does not correspond
        # to the deleted agency.
        self.assertEqual(1, len(self.session.query(schema.AgencySetting).all()))
        self.assertEqual(
            0,
            len(
                list(
                    self.session.query(schema.AgencySetting).filter(
                        schema.AgencySetting.source_id == agency_A.id
                    )
                )
            ),
        )

        # There is only one entry in the AgencySetting table and it does not correspond
        # to the deleted agency.
        self.assertEqual(1, len(self.session.query(schema.AgencyJurisdiction).all()))
        self.assertEqual(
            0,
            len(
                list(
                    self.session.query(schema.AgencyJurisdiction).filter(
                        schema.AgencyJurisdiction.source_id == agency_A.id
                    )
                )
            ),
        )

        # There is only one entry in the Datapoint table and it does not correspond to
        # the deleted agency.
        queried_datapoints = self.session.query(Datapoint).all()
        self.assertEqual(1, len(queried_datapoints))
        self.assertEqual(
            0,
            len(
                list(
                    self.session.query(Datapoint).filter(
                        Datapoint.source_id == agency_A.id
                    )
                )
            ),
        )

        # There is only one entry in DatapointHistory and it corresponds to Agency B.
        agency_B_datapoint = queried_datapoints.pop()
        self.assertEqual(1, len(self.session.query(DatapointHistory).all()))
        self.assertEqual(
            1,
            len(
                list(
                    self.session.query(DatapointHistory).filter(
                        DatapointHistory.datapoint_id == agency_B_datapoint.id
                    )
                )
            ),
        )

        # There is only one entry in the Report table and it does not correspond to
        # the deleted agency.
        self.assertEqual(1, len(self.session.query(Report).all()))
        self.assertEqual(
            0,
            len(
                list(self.session.query(Report).filter(Report.source_id == agency_A.id))
            ),
        )

        # There is only one entry in Spreadsheet and it does not correspond to the
        # deleted agency.
        self.assertEqual(1, len(self.session.query(Spreadsheet).all()))
        self.assertEqual(
            0,
            len(
                list(
                    self.session.query(Spreadsheet).filter(
                        Spreadsheet.agency_id == agency_A.id
                    )
                )
            ),
        )
        response_json = assert_type(response.json, dict)
        self.assertEqual(response_json["name"], agency_A.name)
        self.assertEqual(response_json["id"], agency_A.id)
        self.assertEqual(len(response_json["systems"]), 1)

    def test_create_or_update_user(
        self,
    ) -> None:
        self.load_users_and_agencies()

        # Create a new user
        response = self.client.put(
            "/admin/user",
            json={
                "users": [
                    {
                        "name": "Jane Doe",
                        "email": "email1@test.com",
                        "agency_ids": [self.agency_A_id, self.agency_B_id],
                    }
                ]
            },
        )
        self.assertEqual(response.status_code, 200)
        user = assert_type(
            UserAccountInterface.get_user_by_email(
                session=self.session, email="email1@test.com"
            ),
            schema.UserAccount,
        )
        self.assertEqual(len(user.agency_assocs), 2)
        self.assertEqual(user.agency_assocs[0].agency_id, self.agency_A_id)
        self.assertEqual(
            user.agency_assocs[0].role,
            schema.UserAccountRole.AGENCY_ADMIN,
        )
        self.assertEqual(user.agency_assocs[1].agency_id, self.agency_B_id)
        self.assertEqual(
            user.agency_assocs[1].role,
            schema.UserAccountRole.AGENCY_ADMIN,
        )

        # Update Jane Smith, Add John Doe
        response = self.client.put(
            "/admin/user",
            json={
                "users": [
                    {
                        "name": "Jane Smith Doe",
                        "email": "email1@test.com",
                        "agency_ids": [self.agency_B_id],
                        "user_account_id": user.id,
                    },
                    {
                        "name": "John Doe",
                        "email": "email2@test.com",
                        "agency_ids": [self.agency_A_id],
                    },
                ]
            },
        )
        self.assertEqual(response.status_code, 200)
        jane_doe = assert_type(
            UserAccountInterface.get_user_by_email(
                session=self.session, email="email1@test.com"
            ),
            schema.UserAccount,
        )
        self.assertIsNotNone(jane_doe)
        self.assertEqual(jane_doe.name, "Jane Smith Doe")
        self.assertEqual(len(jane_doe.agency_assocs), 1)
        self.assertEqual(jane_doe.agency_assocs[0].agency_id, self.agency_B_id)
        self.assertEqual(
            jane_doe.agency_assocs[0].role,
            schema.UserAccountRole.AGENCY_ADMIN,
        )

        john_doe = assert_type(
            UserAccountInterface.get_user_by_email(
                session=self.session, email="email2@test.com"
            ),
            schema.UserAccount,
        )
        self.assertIsNotNone(john_doe)
        self.assertEqual(john_doe.name, "John Doe")
        self.assertEqual(len(john_doe.agency_assocs), 1)
        self.assertEqual(john_doe.agency_assocs[0].agency_id, self.agency_A_id)
        self.assertEqual(
            john_doe.agency_assocs[0].role,
            schema.UserAccountRole.AGENCY_ADMIN,
        )

    # Agency
    def test_get_all_agencies(self) -> None:
        self.load_users_and_agencies()
        response = self.client.get("/admin/agency")
        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        self.assertEqual(
            response_json["systems"], [enum.value for enum in VALID_SYSTEMS]
        )
        agency_json_list = response_json["agencies"]
        self.assertEqual(len(agency_json_list), 3)
        agencies = self.session.query(Source).all()
        agency_id_to_agency = {a.id: a for a in agencies}

        for agency_json in agency_json_list:
            db_agency = agency_id_to_agency[(agency_json["id"])]
            user = db_agency.user_account_assocs[0].user_account
            self.assertEqual(agency_json["name"], db_agency.name)
            self.assertEqual(agency_json["child_agency_ids"], [])
            self.assertEqual(len(agency_json["team"]), 1)
            self.assertEqual(agency_json["team"][0]["name"], user.name)

    def test_get_agency(self) -> None:
        self.load_users_and_agencies()
        agency = self.session.query(Agency).filter(Agency.name == "Agency Alpha").one()
        agency.is_superagency = True
        self.session.commit()

        response = self.client.get(f"/admin/agency/{agency.id}")
        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        agency_json = response_json["agency"]
        metrics = response_json["metrics"]
        self.assertEqual(agency_json["name"], agency.name)
        self.assertEqual(agency_json["id"], agency.id)
        self.assertTrue(len(metrics) > 0)
        self.assertEqual(metrics[0]["sector"], "LAW ENFORCEMENT")

    def test_create_or_update_agency(self) -> None:
        self.load_users_and_agencies()

        user_A_id = UserAccountInterface.get_user_by_auth0_user_id(
            session=self.session, auth0_user_id="auth0_id_A"
        ).id
        user_B_id = UserAccountInterface.get_user_by_auth0_user_id(
            session=self.session, auth0_user_id="auth0_id_B"
        ).id
        # Create a new agency
        response = self.client.put(
            "/admin/agency",
            json={
                "name": "New Agency",
                "state_code": "us_ca",
                "systems": ["LAW_ENFORCEMENT", "JAILS"],
                "is_superagency": False,
                "super_agency_id": None,
                "agency_id": None,
                "team": [{"user_account_id": user_A_id, "role": "AGENCY_ADMIN"}],
                "child_agency_ids": [],
                "is_dashboard_enabled": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        agency = AgencyInterface.get_agency_by_name_state_and_systems(
            session=self.session,
            name="New Agency",
            state_code="us_ca",
            systems=["LAW_ENFORCEMENT", "JAILS"],
        )
        self.assertIsNotNone(agency)
        self.assertFalse(agency.is_superagency)
        self.assertEqual(
            agency.systems,
            [schema.System.LAW_ENFORCEMENT.value, schema.System.JAILS.value],
        )
        self.assertEqual(agency.state_code, "us_ca")
        self.assertEqual(len(agency.user_account_assocs), 1)
        self.assertEqual(agency.user_account_assocs[0].user_account_id, user_A_id)
        self.assertEqual(
            agency.user_account_assocs[0].role,
            schema.UserAccountRole.AGENCY_ADMIN,
        )

        # Update name, users, and child agency, and turn on dashboard
        response = self.client.put(
            "/admin/agency",
            json={
                "name": "New Agency New Name",
                "state_code": "us_ca",
                "systems": ["LAW_ENFORCEMENT", "JAILS"],
                "is_superagency": True,
                "super_agency_id": None,
                "child_agency_ids": [self.agency_A_id],
                "agency_id": agency.id,
                "team": [
                    {"user_account_id": user_B_id, "role": "AGENCY_ADMIN"},
                    {"user_account_id": user_A_id, "role": "JUSTICE_COUNTS_ADMIN"},
                ],
                "is_dashboard_enabled": True,
            },
        )
        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        self.assertEqual(response_json["child_agency_ids"], [self.agency_A_id])
        agency = AgencyInterface.get_agency_by_name_state_and_systems(
            session=self.session,
            name="New Agency New Name",
            state_code="us_ca",
            systems=["LAW_ENFORCEMENT", "JAILS"],
        )
        self.assertIsNotNone(agency)
        self.assertTrue(agency.is_superagency)
        self.assertTrue(agency.is_dashboard_enabled)
        self.assertEqual(
            agency.systems,
            [schema.System.LAW_ENFORCEMENT.value, schema.System.JAILS.value],
        )
        self.assertEqual(agency.state_code, "us_ca")
        self.assertEqual(len(agency.user_account_assocs), 2)
        self.assertEqual(agency.user_account_assocs[0].user_account_id, user_A_id)
        self.assertEqual(
            agency.user_account_assocs[0].role,
            schema.UserAccountRole.JUSTICE_COUNTS_ADMIN,
        )
        self.assertEqual(agency.user_account_assocs[1].user_account_id, user_B_id)
        self.assertEqual(
            agency.user_account_assocs[1].role,
            schema.UserAccountRole.AGENCY_ADMIN,
        )
        child_agencies = AgencyInterface.get_child_agencies_for_agency(
            session=self.session, agency=agency
        )
        self.assertEqual(len(child_agencies), 1)
        self.assertEqual(child_agencies[0].name, "Agency Alpha")

        # Delete users from Agency and remove child agency
        response = self.client.put(
            "/admin/agency",
            json={
                "name": "New Agency New Name",
                "state_code": "us_ca",
                "systems": ["LAW_ENFORCEMENT", "JAILS"],
                "is_superagency": False,
                "super_agency_id": None,
                "child_agency_ids": [],
                "agency_id": agency.id,
                "is_dashboard_enabled": False,
                "team": [],
            },
        )

        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        self.assertEqual(response_json["child_agency_ids"], [])
        agency = AgencyInterface.get_agency_by_name_state_and_systems(
            session=self.session,
            name="New Agency New Name",
            state_code="us_ca",
            systems=["LAW_ENFORCEMENT", "JAILS"],
        )
        self.assertIsNotNone(agency)
        self.assertFalse(agency.is_superagency)
        self.assertFalse(agency.is_dashboard_enabled)
        self.assertEqual(
            agency.systems,
            [schema.System.LAW_ENFORCEMENT.value, schema.System.JAILS.value],
        )
        self.assertEqual(agency.state_code, "us_ca")
        self.assertEqual(len(agency.user_account_assocs), 0)
        child_agencies = AgencyInterface.get_child_agencies_for_agency(
            session=self.session, agency=agency
        )
        self.assertEqual(len(child_agencies), 0)

    def test_copy_metric_settings_to_child_agencies(self) -> None:
        self.load_users_and_agencies()
        super_agency = (
            self.session.query(Agency).filter(Agency.name == "Agency Alpha").one()
        )

        child_agency_1 = Agency(
            name="Agency Alpha Child Agency",
            super_agency_id=super_agency.id,
            systems=["LAW_ENFORCEMENT"],
        )

        child_agency_2 = Agency(
            name="Agency Beta Child Agency",
            super_agency_id=super_agency.id,
            systems=["LAW_ENFORCEMENT"],
        )
        MetricSettingInterface.add_or_update_agency_metric_setting(
            session=self.session,
            agency=super_agency,
            agency_metric=MetricInterface(
                key=law_enforcement.funding.key,
                is_metric_enabled=False,
            ),
        )
        MetricSettingInterface.add_or_update_agency_metric_setting(
            session=self.session,
            agency=super_agency,
            agency_metric=MetricInterface(
                key=law_enforcement.expenses.key,
                custom_reporting_frequency=CustomReportingFrequency(
                    frequency=schema.ReportingFrequency.ANNUAL, starting_month=2
                ),
            ),
        )

        self.session.add_all(
            [
                child_agency_1,
                child_agency_2,
            ]
        )
        self.session.commit()
        self.session.refresh(child_agency_1)
        self.session.refresh(child_agency_2)
        self.session.refresh(super_agency)
        child_agency_1_id = child_agency_1.id
        child_agency_2_id = child_agency_2.id
        super_agency_id = super_agency.id

        # Copy the expenses metric settings values for child agency 1.
        copy_metric_settings(
            super_agency_id=super_agency_id,
            dry_run=False,
            metric_definition_key_subset=[law_enforcement.expenses.key],
            child_agency_id_subset=[child_agency_1_id],
            current_session=self.session,
            agency_name="Agency Alpha",
        )
        # Check if the expense metric settings have been copied to the child agencies.
        copied_expenses_metrics = (
            self.session.query(MetricSetting)
            .filter(
                MetricSetting.agency_id == child_agency_1_id,
                MetricSetting.metric_definition_key == law_enforcement.expenses.key,
            )
            .one()
        )
        self.assertEqual(
            copied_expenses_metrics.metric_interface["custom_reporting_frequency"][
                "custom_frequency"
            ],
            "ANNUAL",
        )
        self.assertEqual(
            copied_expenses_metrics.metric_interface["custom_reporting_frequency"][
                "starting_month"
            ],
            2,
        )
        # Make sure funding metrics were NOT copied.
        self.assertEqual(
            0,
            len(
                self.session.query(MetricSetting)
                .filter(
                    MetricSetting.agency_id == child_agency_1_id,
                    MetricSetting.metric_definition_key == law_enforcement.funding.key,
                )
                .all()
            ),
        )

        # Make sure NO metrics were copied for child agency 2.
        child_2_metrics = (
            self.session.query(MetricSetting)
            .filter(
                MetricSetting.agency_id == child_agency_2_id,
            )
            .all()
        )
        self.assertEqual(len(child_2_metrics), 0)

        # Test copying all metric_definitions for child agency 1.
        copy_metric_settings(
            super_agency_id=super_agency_id,
            dry_run=False,
            metric_definition_key_subset=["ALL"],
            child_agency_id_subset=[child_agency_1_id],
            current_session=self.session,
            agency_name="Agency Alpha",
        )
        # Check if the expense metric settings have been copied to the child agencies.
        copied_funding_metrics = (
            self.session.query(MetricSetting)
            .filter(
                MetricSetting.agency_id == child_agency_1_id,
                MetricSetting.metric_definition_key == law_enforcement.funding.key,
            )
            .one()
        )
        self.assertEqual(
            copied_funding_metrics.metric_interface["is_metric_enabled"],
            False,
        )
        # Make sure still NO metrics were copied for child agency 2.
        child_2_metrics = (
            self.session.query(MetricSetting)
            .filter(
                MetricSetting.agency_id == child_agency_2_id,
            )
            .all()
        )
        self.assertEqual(len(child_2_metrics), 0)

        # Copy all metric settings for all agencies (so, child agency 2 also).
        copy_metric_settings(
            super_agency_id=super_agency_id,
            dry_run=False,
            metric_definition_key_subset=["ALL"],
            current_session=self.session,
            agency_name="Agency Alpha",
        )
        # Expense metric settings have been copied to child agency 2.
        copied_funding_metrics = (
            self.session.query(MetricSetting)
            .filter(
                MetricSetting.agency_id == child_agency_2_id,
                MetricSetting.metric_definition_key == law_enforcement.funding.key,
            )
            .one()
        )
        self.assertEqual(
            copied_funding_metrics.metric_interface["is_metric_enabled"],
            False,
        )
