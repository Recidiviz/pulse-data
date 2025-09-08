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
from flask import g
from mock import patch
from sqlalchemy.engine import Engine

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.agency_setting import AgencySettingInterface
from recidiviz.justice_counts.control_panel.config import Config
from recidiviz.justice_counts.control_panel.server import create_app
from recidiviz.justice_counts.control_panel.user_context import UserContext
from recidiviz.justice_counts.dimensions.common import ExpenseType
from recidiviz.justice_counts.dimensions.law_enforcement import ForceType, FundingType
from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.metrics import law_enforcement, prisons
from recidiviz.justice_counts.metrics.custom_reporting_frequency import (
    CustomReportingFrequency,
)
from recidiviz.justice_counts.metrics.metric_disaggregation_data import (
    MetricAggregatedDimensionData,
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
            DB_URL=self.postgres_launch_result.url(),
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
        # `flask_scoped_session` sets the `scoped_session` attribute on the app,
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
        for assoc in user.agency_assocs:
            self.assertEqual(
                assoc.role,
                schema.UserAccountRole.AGENCY_ADMIN,
            )
            self.assertTrue(assoc.agency_id in {self.agency_A_id, self.agency_B_id})

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
    def test_get_agency(self) -> None:
        self.load_users_and_agencies()
        agency = self.session.query(Agency).filter(Agency.name == "Agency Alpha").one()
        agency.is_superagency = True
        self.session.commit()
        self.session.refresh(agency)

        response = self.client.get(f"/admin/agency/{agency.id}")
        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        agency_json = response_json["agency"]
        metrics = response_json["metrics"]
        self.assertEqual(agency_json["name"], agency.name)
        self.assertEqual(agency_json["id"], agency.id)
        self.assertIsNone(agency_json["is_stepping_up_agency"])
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
                "is_stepping_up_agency": False,
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
        self.assertFalse(agency.is_stepping_up_agency)

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
                "agency_url": "foobar",
                "team": [
                    {"user_account_id": user_B_id, "role": "AGENCY_ADMIN"},
                    {"user_account_id": user_A_id, "role": "JUSTICE_COUNTS_ADMIN"},
                ],
                "is_dashboard_enabled": True,
                "is_stepping_up_agency": True,
            },
        )
        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        self.assertEqual(response_json["child_agency_ids"], [self.agency_A_id])
        self.assertEqual(response_json["agency_url"], "foobar")
        agency = AgencyInterface.get_agency_by_name_state_and_systems(
            session=self.session,
            name="New Agency New Name",
            state_code="us_ca",
            systems=["LAW_ENFORCEMENT", "JAILS"],
        )
        self.assertIsNotNone(agency)
        self.assertTrue(agency.is_superagency)
        self.assertTrue(agency.is_dashboard_enabled)
        self.assertTrue(agency.is_stepping_up_agency)
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
                "agency_url": "foobar",
                "agency_description": "hello world",
                "is_dashboard_enabled": False,
                "team": [],
            },
        )

        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        self.assertEqual(response_json["child_agency_ids"], [])
        self.assertEqual(response_json["agency_url"], "foobar")
        self.assertEqual(response_json["agency_description"], "hello world")
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
            agency_metric_updates=MetricInterface(
                key=law_enforcement.funding.key,
                is_metric_enabled=False,
            ),
        )
        MetricSettingInterface.add_or_update_agency_metric_setting(
            session=self.session,
            agency=super_agency,
            agency_metric_updates=MetricInterface(
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

    def test_fetch_users_overview(self) -> None:
        self.load_users_and_agencies()

        response = self.client.get("/admin/user/overview")
        self.assertEqual(response.status_code, 200)

        response_json = assert_type(response.json, dict)
        users_data = response_json["users"]

        self.assertEqual(len(users_data), 3)

        expected_users = [
            {"auth0_user_id": "auth0_id_A", "email": None, "name": "Jane Doe"},
            {"auth0_user_id": "auth0_id_B", "email": None, "name": "John Doe"},
            {
                "auth0_user_id": "auth0_id_C",
                "email": None,
                "name": "John Smith",
            },
        ]

        # Remove 'id' field from the actual response before comparison
        cleaned_users_data = [
            {key: user[key] for key in user if key != "id"} for user in users_data
        ]

        # Sort both lists for order-agnostic comparison, safely handling None values
        sorted_users_data = sorted(
            cleaned_users_data, key=lambda user: user["auth0_user_id"] or ""
        )
        sorted_expected_users = sorted(
            expected_users, key=lambda user: user["auth0_user_id"] or ""
        )

        self.assertEqual(sorted_users_data, sorted_expected_users)

    def test_get_user_agencies(self) -> None:
        """Test that the get_user_agencies endpoint returns the correct agencies for a user."""
        self.load_users_and_agencies()
        user_A = self.test_schema_objects.test_user_A

        response = self.client.get(f"/admin/user/{user_A.id}/agencies")
        self.assertEqual(response.status_code, 200)

        response_json = assert_type(response.json, dict)
        agencies_data = response_json["agencies"]

        self.assertEqual(len(agencies_data), 1)

        # User A has access to Agency A.
        agency_A = self.test_schema_objects.test_agency_A
        self.assertEqual(agencies_data[0]["id"], agency_A.id)
        self.assertEqual(agencies_data[0]["name"], agency_A.name)

    def test_fetch_agencies_overview(self) -> None:
        """Test that the fetch_agencies_overview endpoint returns the correct data,
        including superagency and child agencies."""
        super_agency = self.test_schema_objects.test_prison_super_agency
        # Commit and refresh so we can access the super agency's ID which is auto-incremented.
        self.session.add(super_agency)
        self.session.commit()
        self.session.refresh(super_agency)

        AgencySettingInterface.create_or_update_agency_setting(
            session=self.session,
            agency_id=super_agency.id,
            setting_type=schema.AgencySettingType.HOMEPAGE_URL,
            value="barfoo",
        )

        child_agency_A = self.test_schema_objects.test_prison_child_agency_A
        child_agency_B = self.test_schema_objects.test_prison_child_agency_B
        child_agency_A.super_agency_id = super_agency.id
        child_agency_B.super_agency_id = super_agency.id
        self.session.add_all([child_agency_A, child_agency_B])
        self.session.commit()

        AgencySettingInterface.create_or_update_agency_setting(
            session=self.session,
            agency_id=child_agency_A.id,
            setting_type=schema.AgencySettingType.PURPOSE_AND_FUNCTIONS,
            value="foobar",
        )
        self.session.commit()

        response = self.client.get("/admin/agency/overview")
        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        agencies_data = response_json["agencies"]
        systems_data = response_json["systems"]

        self.assertEqual(len(agencies_data), 3)  # super agency + 2 child agencies

        super_agency_data = next(
            agency for agency in agencies_data if agency["id"] == super_agency.id
        )
        self.assertEqual(super_agency_data["id"], super_agency.id)
        self.assertEqual(super_agency_data["name"], super_agency.name)
        self.assertEqual(
            super_agency_data["child_agency_ids"],
            [child_agency_A.id, child_agency_B.id],
        )
        self.assertEqual(super_agency_data["agency_url"], "barfoo")

        child_agency_A_data = next(
            agency for agency in agencies_data if agency["id"] == child_agency_A.id
        )
        self.assertEqual(child_agency_A_data["id"], child_agency_A.id)
        self.assertEqual(child_agency_A_data["name"], child_agency_A.name)
        self.assertEqual(child_agency_A_data["child_agency_ids"], [])
        self.assertEqual(child_agency_A_data["agency_description"], "foobar")

        child_agency_B_data = next(
            agency for agency in agencies_data if agency["id"] == child_agency_B.id
        )
        self.assertEqual(child_agency_B_data["id"], child_agency_B.id)
        self.assertEqual(child_agency_B_data["name"], child_agency_B.name)
        self.assertEqual(child_agency_B_data["child_agency_ids"], [])

        self.assertEqual(systems_data, [system.value for system in VALID_SYSTEMS])

    def test_get_agency_reporting_agencies(self) -> None:
        """Test retrieval of reporting agency options and metrics for a specific child agency."""

        # Set up test agencies and commit to obtain their IDs
        super_agency = self.test_schema_objects.test_prison_super_agency
        vendor_A = self.test_schema_objects.vendor_A

        self.session.add_all([super_agency, vendor_A])
        self.session.commit()
        self.session.refresh(super_agency)
        self.session.refresh(vendor_A)

        # Configure child agency and assign the super agency
        child_agency_A = self.test_schema_objects.test_prison_child_agency_A
        child_agency_A.super_agency_id = super_agency.id
        self.session.add(child_agency_A)
        self.session.commit()
        self.session.refresh(child_agency_A)

        # Add metric settings to test self-reporting and reporting by other agencies
        MetricSettingInterface.add_or_update_agency_metric_setting(
            session=self.session,
            agency=child_agency_A,
            agency_metric_updates=MetricInterface(
                key=prisons.funding.key, is_self_reported=True
            ),
        )

        MetricSettingInterface.add_or_update_agency_metric_setting(
            session=self.session,
            agency=child_agency_A,
            agency_metric_updates=MetricInterface(
                key=prisons.expenses.key, reporting_agency_id=super_agency.id
            ),
        )

        MetricSettingInterface.add_or_update_agency_metric_setting(
            session=self.session,
            agency=child_agency_A,
            agency_metric_updates=MetricInterface(
                key=prisons.readmissions.key, reporting_agency_id=vendor_A.id
            ),
        )
        self.session.commit()

        # Make the GET request to the endpoint
        response = self.client.get(
            f"/admin/agency/{child_agency_A.id}/reporting-agency"
        )
        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)

        # Validate the metrics structure in the response
        metrics_json = response_json["metrics"]
        self.assertEqual(["PRISONS"], list(metrics_json.keys()))
        self.assertEqual(
            {metric["key"] for metric in metrics_json["PRISONS"]},
            {
                prisons.funding.key,
                prisons.expenses.key,
                prisons.staff.key,
                prisons.readmissions.key,
                prisons.grievances_upheld.key,
                prisons.admissions.key,
                prisons.releases.key,
                prisons.staff_use_of_force_incidents.key,
                prisons.daily_population.key,
            },
        )

        # Validate reporting agencies information in the response
        reporting_agencies_json = response_json["reporting_agencies"]
        self.assertIn("PRISONS", reporting_agencies_json)
        for metric in reporting_agencies_json["PRISONS"]:
            key = metric["metric_key"]
            if key == prisons.funding.key:
                self.assertTrue(metric["is_self_reported"])
                self.assertIsNone(metric["reporting_agency_id"])
                self.assertIsNone(metric["reporting_agency_name"])
            elif key == prisons.expenses.key:
                self.assertEqual(metric["reporting_agency_id"], super_agency.id)
                self.assertEqual(metric["reporting_agency_name"], super_agency.name)
            elif key == prisons.readmissions.key:
                self.assertEqual(metric["reporting_agency_id"], vendor_A.id)
                self.assertEqual(metric["reporting_agency_name"], vendor_A.name)
            else:
                self.assertEqual(metric["reporting_agency_id"], None)
                self.assertEqual(metric["reporting_agency_name"], None)

        # Validate reporting agency options in the response
        reporting_agency_options_json = response_json["reporting_agency_options"]
        expected_options = {
            (vendor_A.id, "VENDOR"),
            (super_agency.id, "AGENCY"),
        }
        actual_options = {
            (opt["reporting_agency_id"], opt["category"])
            for opt in reporting_agency_options_json
        }
        self.assertEqual(expected_options, actual_options)

    def test_update_agency_reporting_agencies(self) -> None:
        """Test updating reporting agencies for metrics of a specific agency."""
        self.load_users_and_agencies()

        agency = self.test_schema_objects.test_prison_child_agency_A
        super_agency = self.test_schema_objects.test_prison_super_agency
        vendor_A = self.test_schema_objects.vendor_A
        user_A = self.test_schema_objects.test_user_A
        self.session.add_all([agency, super_agency, vendor_A, user_A])
        self.session.commit()
        self.session.refresh(agency)
        self.session.refresh(super_agency)
        self.session.refresh(vendor_A)

        agency_id = agency.id
        super_agency_id = super_agency.id
        vendor_A_id = vendor_A.id
        MetricSettingInterface.add_or_update_agency_metric_setting(
            session=self.session,
            agency=agency,
            agency_metric_updates=MetricInterface(
                key=prisons.funding.key, is_self_reported=True
            ),
        )
        MetricSettingInterface.add_or_update_agency_metric_setting(
            session=self.session,
            agency=agency,
            agency_metric_updates=MetricInterface(
                key=prisons.expenses.key, reporting_agency_id=None
            ),
        )
        MetricSettingInterface.add_or_update_agency_metric_setting(
            session=self.session,
            agency=agency,
            agency_metric_updates=MetricInterface(
                key=prisons.staff.key, reporting_agency_id=None
            ),
        )
        self.session.commit()

        payload = {
            "reporting_agencies": [
                {
                    "metric_key": prisons.funding.key,
                    "reporting_agency_id": super_agency_id,
                    "is_self_reported": False,
                },
                {
                    "metric_key": prisons.expenses.key,
                    "reporting_agency_id": vendor_A_id,
                    "is_self_reported": False,
                },
                {
                    "metric_key": prisons.staff.key,
                    "reporting_agency_id": None,
                    "is_self_reported": True,
                },
            ]
        }
        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user_A.auth0_user_id)
            response = self.client.put(
                f"/admin/agency/{agency.id}/reporting-agency",
                json=payload,
            )

        # Validate the response
        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        self.assertEqual(response_json["status"], "ok")
        self.assertEqual(response_json["status_code"], 200)

        # Validate the updates in the database
        agency = self.session.query(schema.Agency).filter_by(id=agency_id).one()
        metric_settings = MetricSettingInterface.get_metric_key_to_metric_interface(
            session=self.session, agency=agency
        )
        funding_metric = metric_settings[prisons.funding.key]
        expenses_metric = metric_settings[prisons.expenses.key]
        staff_metric = metric_settings[prisons.staff.key]

        self.assertEqual(funding_metric.reporting_agency_id, super_agency_id)
        self.assertFalse(funding_metric.is_self_reported)

        self.assertEqual(expenses_metric.reporting_agency_id, vendor_A_id)
        self.assertFalse(expenses_metric.is_self_reported)

        self.assertEqual(staff_metric.reporting_agency_id, None)
        self.assertTrue(staff_metric.is_self_reported)

    def test_get_vendors(self) -> None:
        """Test fetching vendor id, name, and URL for all vendors."""
        self.load_users_and_agencies()

        vendor_A = self.test_schema_objects.vendor_A
        vendor_B = self.test_schema_objects.vendor_B

        # Add vendors to the session and commit
        self.session.add_all([vendor_A, vendor_B])
        self.session.commit()
        self.session.refresh(vendor_A)
        self.session.refresh(vendor_B)
        vendor_A_id = vendor_A.id
        vendor_B_id = vendor_B.id

        # Add homepage URLs for vendors as agency settings
        AgencySettingInterface.create_or_update_agency_setting(
            session=self.session,
            agency_id=vendor_A_id,
            setting_type=schema.AgencySettingType.HOMEPAGE_URL,
            value="https://vendorA.example.com",
        )

        AgencySettingInterface.create_or_update_agency_setting(
            session=self.session,
            agency_id=vendor_B_id,
            setting_type=schema.AgencySettingType.HOMEPAGE_URL,
            value="https://vendorB.example.com",
        )

        self.session.commit()

        # Make the GET request to the /vendors endpoint
        response = self.client.get("/admin/vendors")

        # Validate the response
        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, list)

        # Validate the structure and content of the response
        expected_response = [
            {
                "id": vendor_A_id,
                "name": vendor_A.name,
                "url": "https://vendorA.example.com",
            },
            {
                "id": vendor_B_id,
                "name": vendor_B.name,
                "url": "https://vendorB.example.com",
            },
        ]
        self.assertEqual(response_json, expected_response)

    def test_add_update_and_delete_vendor_with_cleanup(self) -> None:
        """Test adding, updating, and deleting a vendor, and cleaning up related MetricSettings."""
        self.load_users_and_agencies()

        # Step 1: Add a vendor
        add_payload = {"name": "Vendor A", "url": "https://vendor-a.com"}
        response = self.client.put("/admin/vendors", json=add_payload)

        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        self.assertIn("id", response_json)
        self.assertEqual(response_json["name"], "Vendor A")
        self.assertEqual(response_json["url"], "https://vendor-a.com")

        agency_A = self.test_schema_objects.test_agency_A
        self.session.add(agency_A)
        self.session.commit()
        self.session.refresh(agency_A)

        vendor_id = response_json["id"]

        # Step 2: Create a MetricSetting referencing the vendor
        MetricSettingInterface.add_or_update_agency_metric_setting(
            session=self.session,
            agency=agency_A,
            agency_metric_updates=MetricInterface(
                key=prisons.readmissions.key, reporting_agency_id=vendor_id
            ),
        )

        # Step 3: Update the vendor's name and URL
        update_payload = {
            "id": vendor_id,
            "name": "Updated Vendor A",
            "url": "https://updated-vendor-a.com",
        }

        response = self.client.put("/admin/vendors", json=update_payload)
        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        self.assertEqual(response_json["id"], vendor_id)
        self.assertEqual(response_json["name"], "Updated Vendor A")
        self.assertEqual(response_json["url"], "https://updated-vendor-a.com")

        # Validate that the database reflects the updated vendor information
        vendor = assert_type(
            AgencyInterface.get_vendor_by_id(session=self.session, vendor_id=vendor_id),
            schema.Vendor,
        )
        self.assertEqual(vendor.name, "Updated Vendor A")
        self.assertEqual(len(vendor.agency_settings), 1)
        self.assertEqual(
            vendor.agency_settings[0].value, "https://updated-vendor-a.com"
        )

        # Step 4: Delete the vendor
        response = self.client.delete(
            f"/admin/vendors/{vendor_id}", json=update_payload
        )
        self.assertEqual(response.status_code, 200)

        # Step 5: Verify that the MetricSetting referencing the deleted vendor was cleaned up
        metric_setting = (
            self.session.query(schema.MetricSetting)
            .filter(
                schema.MetricSetting.metric_definition_key == prisons.readmissions.key
            )
            .one()
        )
        metric_interface = MetricInterface.from_storage_json(
            json=metric_setting.metric_interface
        )
        self.assertIsNone(metric_interface.reporting_agency_id)
        self.assertIsNone(metric_interface.is_self_reported)

    def test_update_other_sub_dimensions(self) -> None:
        """Test updating other sub-dimensions for metric settings."""
        self.load_users_and_agencies()
        agency = self.test_schema_objects.test_agency_A
        user = self.test_schema_objects.test_user_A
        self.session.add_all([agency, user])
        self.session.commit()
        self.session.refresh(agency)
        self.session.refresh(user)

        agency_id = agency.id
        user_auth0_id = user.auth0_user_id

        # Prepopulate metric settings
        MetricSettingInterface.add_or_update_agency_metric_setting(
            session=self.session,
            agency=agency,
            agency_metric_updates=MetricInterface(
                key=law_enforcement.funding.key,
                is_metric_enabled=True,
                aggregated_dimensions=[
                    MetricAggregatedDimensionData(
                        dimension_to_enabled_status={FundingType.OTHER: True},
                        dimension_to_other_sub_dimension_to_enabled_status={},
                    )
                ],
            ),
        )
        MetricSettingInterface.add_or_update_agency_metric_setting(
            session=self.session,
            agency=agency,
            agency_metric_updates=MetricInterface(
                key=law_enforcement.use_of_force_incidents.key,
                is_metric_enabled=True,
                aggregated_dimensions=[
                    MetricAggregatedDimensionData(
                        dimension_to_enabled_status={
                            ForceType.OTHER: True,
                            ForceType.OTHER_WEAPON: True,
                        },
                        dimension_to_other_sub_dimension_to_enabled_status={
                            ForceType.OTHER: {"FOO": True, "BAR": False},
                            ForceType.OTHER_WEAPON: {"BAR": None},
                        },
                    )
                ],
            ),
        )
        MetricSettingInterface.add_or_update_agency_metric_setting(
            session=self.session,
            agency=agency,
            agency_metric_updates=MetricInterface(
                key=law_enforcement.expenses.key,
                is_metric_enabled=True,
                aggregated_dimensions=[
                    MetricAggregatedDimensionData(
                        dimension_to_enabled_status={ExpenseType.OTHER: True},
                        dimension_to_other_sub_dimension_to_enabled_status={
                            ExpenseType.OTHER: {
                                "ONE DIMENSION": True,
                                "TWO DIMENSION": False,
                            },
                        },
                    )
                ],
            ),
        )

        self.session.commit()

        updates_payload = {
            "auth0_user_id": user_auth0_id,
            "updates": [
                {
                    "metric_key": law_enforcement.funding.key,
                    "breakdowns": [
                        {
                            "dimension_id": FundingType.dimension_identifier(),
                            "sub_dimensions": [
                                {
                                    "dimension_key": "OTHER",
                                    "other_options": ["Another Option"],
                                }
                            ],
                        }
                    ],
                },
                {
                    "metric_key": law_enforcement.use_of_force_incidents.key,
                    "breakdowns": [
                        {
                            "dimension_id": ForceType.dimension_identifier(),
                            "sub_dimensions": [
                                {
                                    "dimension_key": "OTHER",
                                    "other_options": [],
                                },
                                {
                                    "dimension_key": "OTHER_WEAPON",
                                    "other_options": [
                                        "beep",
                                        "boop",
                                    ],
                                },
                            ],
                        }
                    ],
                },
                {
                    "metric_key": law_enforcement.expenses.key,
                    "breakdowns": [
                        {
                            "dimension_id": ExpenseType.dimension_identifier(),
                            "sub_dimensions": [
                                {
                                    "dimension_key": "OTHER",
                                    "other_options": [
                                        "One Dimension",
                                        "Three Dimension",
                                    ],
                                }
                            ],
                        }
                    ],
                },
            ],
        }

        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user.auth0_user_id)
            response = self.client.put(
                f"/admin/agency/{agency_id}/metric-setting",
                json=updates_payload,
            )

        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        self.assertEqual(response_json["status"], "ok")
        agency = AgencyInterface.get_agency_by_id(
            session=self.session, agency_id=agency_id
        )
        # Validate that the updates were persisted
        updated_settings = MetricSettingInterface.get_metric_key_to_metric_interface(
            session=self.session, agency=agency
        )

        funding_dim = updated_settings[
            law_enforcement.funding.key
        ].aggregated_dimensions[0]
        self.assertEqual(
            funding_dim.dimension_to_other_sub_dimension_to_enabled_status[
                FundingType.OTHER
            ],
            {"ANOTHER OPTION": None},  # added a new sub-dimension
        )

        expenses_dim = updated_settings[
            law_enforcement.expenses.key
        ].aggregated_dimensions[0]
        self.assertEqual(
            expenses_dim.dimension_to_other_sub_dimension_to_enabled_status[
                ExpenseType.OTHER
            ],
            {"ONE DIMENSION": True, "THREE DIMENSION": None},  # replaced one dimension
        )

        use_of_forces_discharges = updated_settings[
            law_enforcement.use_of_force_incidents.key
        ].aggregated_dimensions[0]
        self.assertEqual(
            use_of_forces_discharges.dimension_to_other_sub_dimension_to_enabled_status.get(
                ForceType.OTHER, {}
            ),
            {},  # deletes all sub-dimensions
        )
        self.assertEqual(
            use_of_forces_discharges.dimension_to_other_sub_dimension_to_enabled_status[
                ForceType.OTHER_WEAPON
            ],
            {
                "BEEP": None,
                "BOOP": None,
            },  # replaces all subdimensions
        )

    def test_get_other_sub_dimensions(self) -> None:
        """Test GET /agency/<agency_id>/metric-setting returns correct other sub-dimensions structure."""
        self.load_users_and_agencies()
        agency = self.test_schema_objects.test_agency_A
        user = self.test_schema_objects.test_user_A
        self.session.add_all([agency, user])
        self.session.commit()

        # Add metric setting with 'Other Funding'
        MetricSettingInterface.add_or_update_agency_metric_setting(
            session=self.session,
            agency=agency,
            agency_metric_updates=MetricInterface(
                key="LAW_ENFORCEMENT_FUNDING",
                is_metric_enabled=True,
                aggregated_dimensions=[
                    MetricAggregatedDimensionData(
                        dimension_to_enabled_status={FundingType.OTHER: True},
                        dimension_to_other_sub_dimension_to_enabled_status={
                            FundingType.OTHER: {"Santa Claus": None, "Gifts": True}
                        },
                    )
                ],
            ),
        )

        self.session.commit()

        response = self.client.get(f"/admin/agency/{agency.id}/metric-setting")
        response_json = assert_type(response.json, list)
        self.assertEqual(response.status_code, 200)
        law_enforcement_entry = next(
            (entry for entry in response_json if entry["system"] == "LAW_ENFORCEMENT"),
        )
        self.assertIsNotNone(law_enforcement_entry)

        funding_metric = next(
            (
                m
                for m in law_enforcement_entry["metric_settings"]
                if m["metric_key"] == "LAW_ENFORCEMENT_FUNDING"
            )
        )
        self.assertIsNotNone(funding_metric)

        disaggregations = funding_metric.get("disaggregations", [])
        self.assertEqual(len(disaggregations), 1)
        other_sub_dimensions = disaggregations[0]["other_sub_dimensions"]
        self.assertEqual(other_sub_dimensions[0]["dimension_key"], "OTHER")
        self.assertEqual(other_sub_dimensions[0]["dimension_name"], "Other Funding")
        self.assertEqual(
            other_sub_dimensions[0]["other_options"],
            ["Gifts", "Santa Claus"],
        )

        expense_metric = next(
            (
                m
                for m in law_enforcement_entry["metric_settings"]
                if m["metric_key"] == "LAW_ENFORCEMENT_EXPENSES"
            )
        )

        disaggregations = expense_metric.get("disaggregations", None)
        self.assertEqual(len(disaggregations), 1)
        other_sub_dimensions = disaggregations[0]["other_sub_dimensions"]
        self.assertEqual(other_sub_dimensions[0]["dimension_key"], "OTHER")
        self.assertEqual(other_sub_dimensions[0]["dimension_name"], "Other Expenses")
        self.assertEqual(
            other_sub_dimensions[0]["other_options"],
            [],
        )
