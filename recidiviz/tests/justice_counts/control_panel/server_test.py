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
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest import mock

import pandas as pd
import pytest
from flask import g, session
from freezegun import freeze_time
from mock import patch
from more_itertools import one
from sqlalchemy.engine import Engine

from recidiviz.auth.auth0_client import JusticeCountsAuth0AppMetadata
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.justice_counts import ContextKey
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.agency_user_account_association import (
    AgencyUserAccountAssociationInterface,
)
from recidiviz.justice_counts.bulk_upload.workbook_uploader import WorkbookUploader
from recidiviz.justice_counts.control_panel.config import Config
from recidiviz.justice_counts.control_panel.server import create_app
from recidiviz.justice_counts.control_panel.user_context import UserContext
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.dimensions.dimension_registry import (
    DIMENSION_IDENTIFIER_TO_DIMENSION,
)
from recidiviz.justice_counts.dimensions.law_enforcement import CallType
from recidiviz.justice_counts.dimensions.offense import OffenseType
from recidiviz.justice_counts.includes_excludes.prisons import (
    PrisonReleasesToParoleIncludesExcludes,
    PrisonStaffIncludesExcludes,
)
from recidiviz.justice_counts.metrics import law_enforcement, prisons
from recidiviz.justice_counts.metrics.custom_reporting_frequency import (
    CustomReportingFrequency,
)
from recidiviz.justice_counts.metrics.metric_definition import IncludesExcludesSetting
from recidiviz.justice_counts.metrics.metric_registry import METRICS_BY_SYSTEM
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.types import BulkUploadFileType
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.justice_counts.utils.constants import (
    REPORTING_FREQUENCY_CONTEXT_KEY,
    UploadMethod,
)
from recidiviz.justice_counts.utils.datapoint_utils import get_value
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import (
    Agency,
    AgencySettingType,
    AgencyUserAccountAssociation,
    Datapoint,
    DatapointHistory,
    Report,
    ReportingFrequency,
    ReportStatus,
    Source,
    Spreadsheet,
    SpreadsheetStatus,
    System,
    UserAccount,
    UserAccountInvitationStatus,
)
from recidiviz.tests.auth.utils import get_test_auth0_config
from recidiviz.tests.justice_counts.spreadsheet_helpers import (
    create_csv_file,
    create_excel_file,
)
from recidiviz.tests.justice_counts.utils.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils.auth.auth0 import passthrough_authorization_decorator
from recidiviz.utils.types import assert_type

NOW_TIME = datetime.datetime(2022, 2, 15, 0, 0, 0, 0, datetime.timezone.utc)


@pytest.mark.uses_db
@freeze_time(NOW_TIME)
class TestJusticeCountsControlPanelAPI(JusticeCountsDatabaseTestCase):
    """Implements tests for the Justice Counts Control Panel backend API."""

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        mock_project_id_fn = self.project_id_patcher.start()
        mock_project_id_fn.return_value = "justice-counts"

        self.bulk_upload_test_files = Path(
            "recidiviz/tests/justice_counts/bulk_upload/bulk_upload_fixtures"
        )
        self.client_patcher = patch("recidiviz.auth.auth0_client.Auth0")
        self.test_auth0_client = self.client_patcher.start().return_value
        self.secrets_patcher = patch("recidiviz.auth.auth0_client.secrets")
        self.mock_secrets = self.secrets_patcher.start()
        self.secrets = {
            "auth0_api_domain": "fake_api_domain",
            "auth0_api_client_id": "fake client id",
            "auth0_api_client_secret": "fake client secret",
        }
        self.mock_secrets.get_secret.side_effect = self.secrets.get
        test_config = Config(
            DB_URL=local_postgres_helpers.on_disk_postgres_db_url(),
            WTF_CSRF_ENABLED=False,
            AUTH_DECORATOR=passthrough_authorization_decorator(),
            AUTH0_CONFIGURATION=get_test_auth0_config(),
            AUTH0_CLIENT=self.test_auth0_client,
            SEGMENT_KEY="fake_segment_key",
        )
        self.fs = FakeGCSFileSystem()
        self.fs_patcher = mock.patch.object(GcsfsFactory, "build", return_value=self.fs)
        self.fs_patcher.start()

        self.app = create_app(config=test_config)
        self.client = self.app.test_client()
        self.app.secret_key = "NOT A SECRET"
        # `flask_sqlalchemy_session` sets the `scoped_session` attribute on the app,
        # even though this is not specified in the types for `app`.
        self.session = self.app.scoped_session  # type: ignore[attr-defined]
        self.test_schema_objects = JusticeCountsSchemaTestObjects()
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        self.project_id_patcher.stop()
        self.fs_patcher.stop()
        self.client_patcher.stop()
        self.secrets_patcher.stop()

    def get_engine(self) -> Engine:
        return self.session.get_bind()

    def test_logout(self) -> None:
        with self.app.test_client() as client:
            with client.session_transaction() as sess:
                sess["session_data"] = {"foo": "bar"}

            response = client.post("/auth/logout")
            self.assertEqual(response.status_code, 200)
            with self.app.test_request_context():
                self.assertEqual(0, len(session.keys()))

    def test_auth0_config(self) -> None:
        response = self.client.get("/app_public_config.js")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.data,
            b"window.APP_CONFIG = {'audience': 'http://localhost', 'clientId': 'test_client_id', 'domain': 'auth0.localhost'}; window.SEGMENT_KEY = 'fake_segment_key';",
        )

    def test_get_home_metadata(self) -> None:
        user_A = self.test_schema_objects.test_user_A
        agency_A = self.test_schema_objects.test_agency_A
        current_association = schema.AgencyUserAccountAssociation(
            user_account=user_A, agency=agency_A
        )
        monthly_report_old = self.test_schema_objects.test_report_monthly
        monthly_report_latest = self.test_schema_objects.test_report_monthly_two
        annual_report_calendar_old = self.test_schema_objects.test_report_annual_three
        annual_report_calendar_latest = self.test_schema_objects.test_report_annual_two
        annual_report_fiscal_old = self.test_schema_objects.test_report_annual_five
        annual_report_fiscal_latest = self.test_schema_objects.test_report_annual_four

        self.session.add_all(
            [
                user_A,
                agency_A,
                current_association,
                monthly_report_old,
                monthly_report_latest,
                annual_report_calendar_old,
                annual_report_calendar_latest,
                annual_report_fiscal_old,
                annual_report_fiscal_latest,
            ]
        )
        self.session.commit()

        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user_A.auth0_user_id)
            response = self.client.get(f"/api/home/{agency_A.id}")
            agency_metrics_response = self.client.get(
                f"/api/agencies/{agency_A.id}/metrics"
            )

        response_json = assert_type(response.json, dict)
        agency_metrics_response_json = assert_type(agency_metrics_response.json, list)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            len(response_json["agency_metrics"]), len(agency_metrics_response_json)
        )
        self.assertEqual(
            response_json["monthly_report"].get("id"), monthly_report_latest.id
        )
        self.assertEqual(
            response_json["annual_reports"]["1"].get("id"),
            annual_report_calendar_latest.id,
        )
        self.assertEqual(
            response_json["annual_reports"]["6"].get("id"),
            annual_report_fiscal_latest.id,
        )

    def test_get_superagency_home_metadata(self) -> None:
        user = self.test_schema_objects.test_user_A
        super_agency = self.test_schema_objects.test_prison_super_agency
        child_agency = self.test_schema_objects.test_prison_affiliate_A
        self.session.add_all(
            [
                user,
                super_agency,
                child_agency,
                schema.AgencyUserAccountAssociation(
                    user_account=user, agency=super_agency
                ),
            ]
        )
        self.session.commit()
        self.session.refresh(super_agency)
        child_agency.super_agency_id = super_agency.id
        self.session.commit()

        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user.auth0_user_id)
            response = self.client.get(f"/api/home/{super_agency.id}")

        response_json = assert_type(response.json, dict)
        self.assertEqual(len(response_json["child_agencies"]), 1)
        self.assertEqual(response_json["child_agencies"][0]["name"], child_agency.name)

    def test_get_guidance_progress(self) -> None:
        user_A = self.test_schema_objects.test_user_A
        agency_A = self.test_schema_objects.test_agency_A
        current_association = schema.AgencyUserAccountAssociation(
            user_account=user_A, agency=agency_A
        )
        current_association.guidance_progress = [
            {"topicID": "WELCOME", "topicCompleted": True},
            {"topicID": "AGENCY_SETUP", "topicCompleted": True},
            {"topicID": "METRIC_CONFIG", "topicCompleted": True},
            {"topicID": "ADD_DATA", "topicCompleted": False},
            {"topicID": "PUBLISH_DATA", "topicCompleted": False},
        ]

        self.session.add(current_association)
        self.session.commit()

        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user_A.auth0_user_id)
            response = self.client.get(f"/api/users/agencies/{agency_A.id}/guidance")

        response_json = assert_type(response.json, dict)
        guidance_progress = assert_type(response_json.get("guidance_progress"), list)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            guidance_progress[0], {"topicID": "WELCOME", "topicCompleted": True}
        )
        self.assertEqual(
            guidance_progress[1], {"topicID": "AGENCY_SETUP", "topicCompleted": True}
        )
        self.assertEqual(
            guidance_progress[2], {"topicID": "METRIC_CONFIG", "topicCompleted": True}
        )
        self.assertEqual(
            guidance_progress[3], {"topicID": "ADD_DATA", "topicCompleted": False}
        )
        self.assertEqual(
            guidance_progress[4], {"topicID": "PUBLISH_DATA", "topicCompleted": False}
        )

    def test_get_new_user_guidance_progress(self) -> None:
        user_B = self.test_schema_objects.test_user_B
        agency_B = self.test_schema_objects.test_agency_B
        current_association = schema.AgencyUserAccountAssociation(
            user_account=user_B, agency=agency_B
        )
        current_association.guidance_progress = None

        self.session.add(current_association)
        self.session.commit()

        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user_B.auth0_user_id)
            response = self.client.get(f"/api/users/agencies/{agency_B.id}/guidance")

        response_json = assert_type(response.json, dict)
        guidance_progress = assert_type(response_json.get("guidance_progress"), list)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            guidance_progress[0], {"topicID": "WELCOME", "topicCompleted": False}
        )
        self.assertEqual(
            guidance_progress[1], {"topicID": "AGENCY_SETUP", "topicCompleted": False}
        )
        self.assertEqual(
            guidance_progress[2], {"topicID": "METRIC_CONFIG", "topicCompleted": False}
        )
        self.assertEqual(
            guidance_progress[3], {"topicID": "ADD_DATA", "topicCompleted": False}
        )
        self.assertEqual(
            guidance_progress[4], {"topicID": "PUBLISH_DATA", "topicCompleted": False}
        )

    def test_update_guidance_progress(self) -> None:
        user_A = self.test_schema_objects.test_user_A
        agency_A = self.test_schema_objects.test_agency_A
        current_association = schema.AgencyUserAccountAssociation(
            user_account=user_A, agency=agency_A
        )
        current_association.guidance_progress = [
            {"topicID": "WELCOME", "topicCompleted": False},
            {"topicID": "AGENCY_SETUP", "topicCompleted": False},
            {"topicID": "METRIC_CONFIG", "topicCompleted": False},
            {"topicID": "ADD_DATA", "topicCompleted": False},
            {"topicID": "PUBLISH_DATA", "topicCompleted": False},
        ]
        self.session.add(current_association)
        self.session.commit()

        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user_A.auth0_user_id)
            response = self.client.put(
                f"/api/users/agencies/{agency_A.id}/guidance",
                json={"updated_topic": {"topicID": "WELCOME", "topicCompleted": True}},
            )

        response_json = assert_type(response.json, dict)
        guidance_progress = assert_type(response_json.get("guidance_progress"), list)
        self.assertEqual(guidance_progress[0]["topicCompleted"], True)
        self.assertEqual(guidance_progress[1]["topicCompleted"], False)
        self.assertEqual(guidance_progress[2]["topicCompleted"], False)
        self.assertEqual(guidance_progress[3]["topicCompleted"], False)
        self.assertEqual(guidance_progress[4]["topicCompleted"], False)

    def test_get_all_reports(self) -> None:
        user_A = self.test_schema_objects.test_user_A
        report = self.test_schema_objects.test_report_monthly
        self.session.add_all(
            [
                report,
                user_A,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_A,
                ),
            ]
        )
        self.session.commit()

        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user_A.auth0_user_id)
            response = self.client.get(f"/api/agencies/{report.source_id}/reports")

        self.assertEqual(response.status_code, 200)
        response_list = assert_type(response.json, list)
        self.assertEqual(len(response_list), 1)
        response_json = assert_type(response_list[0], dict)
        self.assertEqual(response_json["editors"], [])
        self.assertEqual(response_json["frequency"], ReportingFrequency.MONTHLY.value)
        self.assertEqual(response_json["last_modified_at"], None)
        self.assertEqual(response_json["month"], 6)
        self.assertEqual(response_json["status"], ReportStatus.NOT_STARTED.value)
        self.assertEqual(response_json["year"], 2022)

    def test_get_multiple_reports(self) -> None:
        user_A = self.test_schema_objects.test_user_A
        agency_A = self.test_schema_objects.test_agency_A
        monthly_report_1 = self.test_schema_objects.test_report_monthly
        monthly_report_2 = self.test_schema_objects.test_report_monthly_two
        self.session.add_all(
            [
                user_A,
                agency_A,
                monthly_report_1,
                monthly_report_2,
                schema.AgencyUserAccountAssociation(
                    user_account=user_A,
                    agency=agency_A,
                ),
            ]
        )

        # Update existing report by updating metric
        report_metric = self.test_schema_objects.arrests_metric
        inserts: List[schema.Datapoint] = []
        updates: List[schema.Datapoint] = []
        histories: List[schema.DatapointHistory] = []
        ReportInterface.add_or_update_metric(
            session=self.session,
            inserts=inserts,
            updates=updates,
            histories=histories,
            report=monthly_report_1,
            report_metric=report_metric,
            user_account=user_A,
            upload_method=UploadMethod.BULK_UPLOAD,
        )
        DatapointInterface.flush_report_datapoints(
            session=self.session,
            inserts=inserts,
            updates=updates,
            histories=histories,
        )
        self.session.commit()
        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user_A.auth0_user_id)
            response = self.client.get(
                f"/api/reports?agency_id={agency_A.id}&report_ids={monthly_report_1.id},{monthly_report_2.id}",
            )

        agency_datapoints = self.session.query(Datapoint).all()

        self.assertEqual(response.status_code, 200)
        response_list = assert_type(response.json, list)

        # Check multiple reports received that correspond with the request list of report IDs
        self.assertEqual(len(response_list), 2)
        self.assertEqual(
            {report["id"] for report in response_list},
            {monthly_report_1.id, monthly_report_2.id},
        )
        self.assertEqual({report["month"] for report in response_list}, {6, 7})

        # No metric property should exist in response for both reports
        self.assertEqual(response_list[0].get("metrics"), None)
        self.assertEqual(response_list[1].get("metrics"), None)

        # Check the presence of datapoints
        # 25 datapoints should be created for `monthly_report_1` (and the entire agency)
        self.assertEqual(len(agency_datapoints), len(response_list[0]["datapoints"]))
        self.assertEqual(
            response_list[0]["datapoints"][0]["end_date"],
            "Fri, 01 Jul 2022 00:00:00 GMT",
        )
        self.assertEqual(
            response_list[0]["datapoints"][0]["frequency"],
            ReportingFrequency.MONTHLY.value,
        )
        self.assertEqual(
            response_list[0]["datapoints"][0]["value"],
            5000.0,
        )

    def test_get_report_metrics(self) -> None:
        user_A = self.test_schema_objects.test_user_A
        report = self.test_schema_objects.test_report_monthly
        self.session.add_all(
            [
                report,
                user_A,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_A,
                ),
            ]
        )
        self.session.commit()

        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user_A.auth0_user_id)
            response = self.client.get(f"/api/reports/{report.id}")

        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        self.assertEqual(response_json["editors"], [])
        self.assertEqual(response_json["frequency"], ReportingFrequency.MONTHLY.value)
        self.assertEqual(response_json["last_modified_at"], None)
        self.assertEqual(response_json["month"], 6)
        self.assertEqual(response_json["status"], ReportStatus.NOT_STARTED.value)
        self.assertEqual(response_json["year"], 2022)
        metrics = assert_type(response_json["metrics"], list)
        self.assertEqual(len(metrics), 3)
        self.assertEqual(metrics[0]["key"], law_enforcement.calls_for_service.key)
        self.assertEqual(metrics[1]["key"], law_enforcement.arrests.key)
        self.assertEqual(metrics[2]["key"], law_enforcement.reported_crime.key)

    def shared_test_agency_metrics(self, metrics: List[Dict[str, Any]]) -> None:
        """shared function for testing test_get_agency_metrics and test_get_agency_published_data"""
        self.assertEqual(len(metrics), 9)
        # Annual Budget metric is turned off and has a fiscal year starting in February
        self.assertEqual(metrics[0]["key"], prisons.funding.key)
        self.assertEqual(metrics[0]["enabled"], False)
        self.assertEqual(metrics[0]["custom_frequency"], "ANNUAL")
        self.assertEqual(metrics[0]["starting_month"], 2)
        # Expenses metric has no changes two includes/excludes settings that
        # are different from the default.
        self.assertEqual(metrics[1]["key"], prisons.expenses.key)
        # Total Staff metric has two includes/excludes settings that
        # are different from the default.
        self.assertEqual(metrics[2]["key"], prisons.staff.key)
        self.assertEqual(
            metrics[2]["includes_excludes"][0]["settings"],
            [
                {
                    "key": "FILLED",
                    "label": PrisonStaffIncludesExcludes.FILLED.value,
                    "included": None,
                    "default": "Yes",
                },
                {
                    "key": "VACANT",
                    "label": PrisonStaffIncludesExcludes.VACANT.value,
                    "included": None,
                    "default": "Yes",
                },
                {
                    "key": "FULL_TIME",
                    "label": PrisonStaffIncludesExcludes.FULL_TIME.value,
                    "included": None,
                    "default": "Yes",
                },
                {
                    "key": "PART_TIME",
                    "label": PrisonStaffIncludesExcludes.PART_TIME.value,
                    "included": None,
                    "default": "Yes",
                },
                {
                    "key": "CONTRACTED",
                    "label": PrisonStaffIncludesExcludes.CONTRACTED.value,
                    "included": None,
                    "default": "Yes",
                },
                {
                    "key": "TEMPORARY",
                    "label": PrisonStaffIncludesExcludes.TEMPORARY.value,
                    "included": None,
                    "default": "Yes",
                },
                {
                    "key": "VOLUNTEER",
                    "label": PrisonStaffIncludesExcludes.VOLUNTEER.value,
                    "included": "Yes",
                    "default": "No",
                },
                {
                    "key": "INTERN",
                    "label": PrisonStaffIncludesExcludes.INTERN.value,
                    "included": "No",
                    "default": "No",
                },
            ],
        )
        # Admissions metric is enabled but OffenseType
        # disaggregation is disabled
        dimension_to_includes_excludes_lst = assert_type(
            prisons.admissions.aggregated_dimensions, list
        )[0].dimension_to_includes_excludes
        self.assertEqual(metrics[3]["key"], prisons.admissions.key)
        self.assertEqual(metrics[3]["enabled"], None)
        self.assertEqual(
            metrics[3]["disaggregations"][0]["key"],
            OffenseType.dimension_identifier(),
        )
        self.assertEqual(metrics[3]["disaggregations"][0]["enabled"], False)

        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][0]["enabled"], False
        )
        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][0]["label"],
            OffenseType.PERSON.value,
        )
        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][0]["key"],
            OffenseType.PERSON.value,
        )
        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][0]["includes_excludes"][0][
                "settings"
            ],
            [
                {
                    "key": member.name,
                    "label": member.value,
                    "included": None,
                    "default": default_setting.value,
                }
                for includes_excludes in dimension_to_includes_excludes_lst[
                    OffenseType.PERSON
                ]
                for member, default_setting in includes_excludes.member_to_default_inclusion_setting.items()
            ],
        )

        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][1]["enabled"], False
        )
        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][1]["label"],
            OffenseType.PROPERTY.value,
        )
        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][1]["key"],
            OffenseType.PROPERTY.value,
        )
        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][1]["includes_excludes"][0][
                "settings"
            ],
            [
                {
                    "key": member.name,
                    "label": member.value,
                    "included": None,
                    "default": default_setting.value,
                }
                for includes_excludes in dimension_to_includes_excludes_lst[
                    OffenseType.PROPERTY
                ]
                for member, default_setting in includes_excludes.member_to_default_inclusion_setting.items()
            ],
        )

        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][2]["enabled"], False
        )
        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][2]["label"],
            OffenseType.DRUG.value,
        )
        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][2]["key"],
            OffenseType.DRUG.value,
        )
        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][2]["includes_excludes"][0][
                "settings"
            ],
            [
                {
                    "key": member.name,
                    "label": member.value,
                    "included": None,
                    "default": default_setting.value,
                }
                for includes_excludes in dimension_to_includes_excludes_lst[
                    OffenseType.DRUG
                ]
                for member, default_setting in includes_excludes.member_to_default_inclusion_setting.items()
            ],
        )

        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][3]["enabled"], False
        )
        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][3]["label"],
            OffenseType.PUBLIC_ORDER.value,
        )
        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][3]["key"],
            OffenseType.PUBLIC_ORDER.value,
        )
        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][3]["includes_excludes"][0][
                "settings"
            ],
            [
                {
                    "key": member.name,
                    "label": member.value,
                    "included": None,
                    "default": default_setting.value,
                }
                for includes_excludes in dimension_to_includes_excludes_lst[
                    OffenseType.PUBLIC_ORDER
                ]
                for member, default_setting in includes_excludes.member_to_default_inclusion_setting.items()
            ],
        )

        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][4]["enabled"], False
        )
        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][4]["label"],
            OffenseType.OTHER.value,
        )
        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][4]["key"],
            OffenseType.OTHER.value,
        )
        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][4]["includes_excludes"], []
        )

        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][5]["enabled"], False
        )
        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][5]["label"],
            OffenseType.UNKNOWN.value,
        )
        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][5]["key"],
            OffenseType.UNKNOWN.value,
        )
        self.assertEqual(
            metrics[3]["disaggregations"][0]["dimensions"][5]["includes_excludes"],
            [],
        )

        self.assertEqual(metrics[4]["key"], prisons.daily_population.key)
        # For the release metric, two settings are excluded from the parole to supervision
        # disaggregation.
        self.assertEqual(metrics[5]["key"], prisons.releases.key)
        self.assertEqual(
            metrics[5]["disaggregations"][0]["dimensions"][1]["includes_excludes"][0][
                "settings"
            ],
            [
                {
                    "key": "AFTER_SANCTION",
                    "label": PrisonReleasesToParoleIncludesExcludes.AFTER_SANCTION.value,
                    "included": "No",
                    "default": "Yes",
                },
                {
                    "key": "ELIGIBLE",
                    "label": PrisonReleasesToParoleIncludesExcludes.ELIGIBLE.value,
                    "included": "No",
                    "default": "Yes",
                },
                {
                    "key": "COMMUTED_SENTENCE",
                    "label": PrisonReleasesToParoleIncludesExcludes.COMMUTED_SENTENCE.value,
                    "included": None,
                    "default": "Yes",
                },
                {
                    "key": "RELEASE_TO_PAROLE",
                    "label": PrisonReleasesToParoleIncludesExcludes.RELEASE_TO_PAROLE.value,
                    "included": None,
                    "default": "Yes",
                },
            ],
        )
        # Readmissions metric has a prefilled context.
        self.assertEqual(metrics[6]["key"], prisons.readmissions.key)
        self.assertEqual(
            metrics[6]["contexts"],
            [
                {
                    "display_name": "If the listed categories do not adequately describe your metric, please describe additional data elements included in your agency’s definition.",
                    "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                    "multiple_choice_options": [],
                    "reporting_note": None,
                    "required": False,
                    "type": "TEXT",
                    "value": None,
                },
            ],
        )
        self.assertEqual(metrics[7]["key"], prisons.staff_use_of_force_incidents.key)
        self.assertEqual(metrics[8]["key"], prisons.grievances_upheld.key)

        # test filenames
        self.assertEqual(metrics[0]["filenames"], ["funding", "funding_by_type"])
        self.assertEqual(metrics[2]["filenames"], ["staff", "staff_by_type"])
        self.assertEqual(metrics[3]["filenames"], ["admissions", "admissions_by_type"])
        self.assertEqual(
            metrics[4]["filenames"],
            [
                "population",
                "population_by_type",
                "population_by_race",
                "population_by_biological_sex",
            ],
        )
        self.assertEqual(metrics[5]["filenames"], ["releases", "releases_by_type"])
        self.assertEqual(
            metrics[6]["filenames"], ["readmissions", "readmissions_by_type"]
        )
        self.assertEqual(metrics[7]["filenames"], ["use_of_force"])
        self.assertEqual(
            metrics[8]["filenames"], ["grievances_upheld", "grievances_upheld_by_type"]
        )

    def test_get_agency_metrics(self) -> None:
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_agency_G,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_G,
                ),
            ]
        )
        self.session.commit()
        agency = self.session.query(Agency).one()
        agency_datapoints = self.test_schema_objects.get_test_agency_datapoints(
            agency_id=agency.id
        )
        self.session.add_all(agency_datapoints)
        self.session.commit()
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            )
            response = self.client.get(f"/api/agencies/{agency.id}/metrics")

        self.assertEqual(response.status_code, 200)
        metrics = assert_type(response.json, list)
        self.shared_test_agency_metrics(metrics=metrics)
        self.assertEqual(metrics[0]["datapoints"], None)
        self.assertEqual(metrics[1]["datapoints"], None)
        self.assertEqual(metrics[2]["datapoints"], None)
        self.assertEqual(metrics[3]["datapoints"], None)
        self.assertEqual(metrics[4]["datapoints"], None)
        self.assertEqual(metrics[5]["datapoints"], None)
        self.assertEqual(metrics[6]["datapoints"], None)
        self.assertEqual(metrics[7]["datapoints"], None)
        self.assertEqual(
            metrics[2]["disaggregations"][0]["dimensions"][0]["datapoints"], None
        )
        self.assertEqual(
            metrics[2]["disaggregations"][0]["dimensions"][1]["datapoints"], None
        )
        self.assertEqual(
            metrics[2]["disaggregations"][0]["dimensions"][2]["datapoints"], None
        )
        self.assertEqual(
            metrics[2]["disaggregations"][0]["dimensions"][3]["datapoints"], None
        )
        self.assertEqual(
            metrics[2]["disaggregations"][0]["dimensions"][4]["datapoints"], None
        )
        self.assertEqual(
            metrics[2]["disaggregations"][0]["dimensions"][5]["datapoints"], None
        )

    def check_agency_metric_datapoint(
        self,
        datapoint: schema.Datapoint,
        value: float,
        report_id: int,
        dimension_display_name: Optional[str] = None,
        disaggregation_display_name: Optional[str] = None,
    ) -> None:
        self.assertEqual(datapoint["dimension_display_name"], dimension_display_name)
        self.assertEqual(
            datapoint["disaggregation_display_name"], disaggregation_display_name
        )
        self.assertEqual(datapoint["end_date"], "Mon, 01 Aug 2022 00:00:00 GMT")
        self.assertEqual(datapoint["is_published"], True)
        self.assertEqual(datapoint["metric_definition_key"], "PRISONS_ADMISSIONS")
        self.assertEqual(datapoint["metric_display_name"], "Admissions")
        self.assertEqual(datapoint["old_value"], None)
        self.assertEqual(datapoint["report_id"], report_id)
        self.assertEqual(datapoint["start_date"], "Fri, 01 Jul 2022 00:00:00 GMT")
        self.assertEqual(datapoint["value"], value)

    def test_get_agency_published_data(self) -> None:
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_agency_G,
            ]
        )
        self.session.commit()
        agency = self.session.query(Agency).one()
        agency_datapoints = self.test_schema_objects.get_test_agency_datapoints(
            agency_id=agency.id
        )
        self.session.add_all(agency_datapoints)
        self.session.commit()

        user_A = self.test_schema_objects.test_user_A
        report_unpublished = self.test_schema_objects.test_report_monthly_prisons
        report_published = self.test_schema_objects.test_report_monthly_prisons
        report_published.status = schema.ReportStatus.PUBLISHED
        report_published.date_range_start = datetime.date.fromisoformat("2022-07-01")
        report_published.date_range_end = datetime.date.fromisoformat("2022-08-01")
        self.session.add_all([report_unpublished, report_published, user_A])

        report_metric = self.test_schema_objects.reported_admissions_metric
        inserts: List[schema.Datapoint] = []
        updates: List[schema.Datapoint] = []
        histories: List[schema.DatapointHistory] = []
        ReportInterface.add_or_update_metric(
            session=self.session,
            inserts=inserts,
            updates=updates,
            histories=histories,
            report=report_unpublished,
            report_metric=report_metric,
            user_account=user_A,
            upload_method=UploadMethod.BULK_UPLOAD,
        )
        ReportInterface.add_or_update_metric(
            session=self.session,
            inserts=inserts,
            updates=updates,
            histories=histories,
            report=report_published,
            report_metric=report_metric,
            user_account=user_A,
            upload_method=UploadMethod.BULK_UPLOAD,
        )
        DatapointInterface.flush_report_datapoints(
            session=self.session,
            inserts=inserts,
            updates=updates,
            histories=histories,
        )
        self.session.commit()
        report_published_id = report_published.id
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            )

            response = self.client.get(
                f"/api/agencies/{agency.id}/agency%20prison/published_data"
            )

        self.assertEqual(response.status_code, 200)
        result = assert_type(response.json, dict)
        agency = result["agency"]
        metrics = result["metrics"]

        self.assertEqual(agency["name"], "Agency Prison")
        self.assertEqual(agency["systems"], ["PRISONS"])

        self.shared_test_agency_metrics(metrics=metrics)

        self.assertEqual(metrics[0]["datapoints"], None)
        self.assertEqual(metrics[1]["datapoints"], None)
        self.assertEqual(metrics[2]["datapoints"], None)
        for datapoint in metrics[3]["datapoints"]:
            self.check_agency_metric_datapoint(
                datapoint=datapoint, value=1000.0, report_id=report_published_id
            )

        self.assertEqual(metrics[4]["datapoints"], None)
        self.assertEqual(metrics[5]["datapoints"], None)
        self.assertEqual(metrics[6]["datapoints"], None)
        self.assertEqual(metrics[7]["datapoints"], None)
        self.assertEqual(metrics[8]["datapoints"], None)

        self.assertEqual(metrics[3]["key"], prisons.admissions.key)
        self.assertEqual(metrics[3]["enabled"], None)
        self.assertEqual(
            metrics[3]["disaggregations"][0]["key"],
            OffenseType.dimension_identifier(),
        )
        self.assertEqual(metrics[3]["disaggregations"][0]["enabled"], False)
        self.check_agency_metric_datapoint(
            datapoint=metrics[3]["disaggregations"][0]["dimensions"][0]["datapoints"][
                0
            ],
            value=3.0,
            report_id=report_published.id,
            dimension_display_name="Person Offenses",
            disaggregation_display_name="Offense Type",
        )
        self.check_agency_metric_datapoint(
            datapoint=metrics[3]["disaggregations"][0]["dimensions"][1]["datapoints"][
                0
            ],
            value=4.0,
            report_id=report_published.id,
            dimension_display_name="Property Offenses",
            disaggregation_display_name="Offense Type",
        )
        self.check_agency_metric_datapoint(
            datapoint=metrics[3]["disaggregations"][0]["dimensions"][2]["datapoints"][
                0
            ],
            value=1.0,
            report_id=report_published.id,
            dimension_display_name="Drug Offenses",
            disaggregation_display_name="Offense Type",
        )
        self.check_agency_metric_datapoint(
            datapoint=metrics[3]["disaggregations"][0]["dimensions"][3]["datapoints"][
                0
            ],
            value=5.0,
            report_id=report_published.id,
            dimension_display_name="Public Order Offenses",
            disaggregation_display_name="Offense Type",
        )
        self.check_agency_metric_datapoint(
            datapoint=metrics[3]["disaggregations"][0]["dimensions"][4]["datapoints"][
                0
            ],
            value=2.0,
            report_id=report_published.id,
            dimension_display_name="Other Offenses",
            disaggregation_display_name="Offense Type",
        )
        self.check_agency_metric_datapoint(
            datapoint=metrics[3]["disaggregations"][0]["dimensions"][5]["datapoints"][
                0
            ],
            value=6.0,
            report_id=report_published.id,
            dimension_display_name="Unknown Offenses",
            disaggregation_display_name="Offense Type",
        )

    def test_get_all_agencies_metadata(self) -> None:
        user_A = self.test_schema_objects.test_user_A
        agency_G = self.test_schema_objects.test_agency_G
        report_published = self.test_schema_objects.get_report_for_agency(
            agency=agency_G
        )
        report_published.status = schema.ReportStatus.PUBLISHED
        report_2_published = self.test_schema_objects.get_report_for_agency(
            agency=agency_G
        )
        report_2_published.status = schema.ReportStatus.PUBLISHED
        report_2_published.date_range_start = datetime.date.fromisoformat("2022-07-01")
        report_2_published.date_range_end = datetime.date.fromisoformat("2022-08-01")
        self.session.add_all([report_published, report_2_published, user_A, agency_G])

        reported_admissions_metric = self.test_schema_objects.reported_admissions_metric
        arrests_metric = self.test_schema_objects.arrests_metric
        inserts: List[schema.Datapoint] = []
        updates: List[schema.Datapoint] = []
        histories: List[schema.DatapointHistory] = []
        ReportInterface.add_or_update_metric(
            session=self.session,
            inserts=inserts,
            updates=updates,
            histories=histories,
            report=report_published,
            report_metric=reported_admissions_metric,
            user_account=user_A,
            upload_method=UploadMethod.BULK_UPLOAD,
        )
        ReportInterface.add_or_update_metric(
            session=self.session,
            inserts=inserts,
            updates=updates,
            histories=histories,
            report=report_published,
            report_metric=arrests_metric,
            user_account=user_A,
            upload_method=UploadMethod.BULK_UPLOAD,
        )
        ReportInterface.add_or_update_metric(
            session=self.session,
            inserts=inserts,
            updates=updates,
            histories=histories,
            report=report_2_published,
            report_metric=reported_admissions_metric,
            user_account=user_A,
            upload_method=UploadMethod.BULK_UPLOAD,
        )
        ReportInterface.add_or_update_metric(
            session=self.session,
            inserts=inserts,
            updates=updates,
            histories=histories,
            report=report_2_published,
            report_metric=arrests_metric,
            user_account=user_A,
            upload_method=UploadMethod.BULK_UPLOAD,
        )
        DatapointInterface.flush_report_datapoints(
            session=self.session,
            inserts=inserts,
            updates=updates,
            histories=histories,
        )
        self.session.commit()

        with self.app.test_request_context():
            response = self.client.get("/api/agencies")

        self.assertEqual(response.status_code, 200)
        result = assert_type(response.json, dict)
        agencies = result["agencies"]
        agency_id = agencies[0]["id"]
        agency_name = agencies[0]["name"]
        number_of_published_records = agencies[0]["number_of_published_records"]
        self.assertEqual(agency_id, agency_G.id)
        self.assertEqual(agency_name, agency_G.name)
        self.assertEqual(number_of_published_records, 2)

    def test_create_report_invalid_permissions(self) -> None:
        user = self.test_schema_objects.test_user_A
        agency = self.test_schema_objects.test_agency_A
        self.session.add_all(
            [
                agency,
                user,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_A,
                ),
            ]
        )
        self.session.commit()
        month = 3
        year = 2022
        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user.auth0_user_id)
            response = self.client.post(
                "/api/reports",
                json={
                    "agency_id": agency.id,
                    "month": month,
                    "year": year,
                    "frequency": ReportingFrequency.MONTHLY.value,
                },
            )
        self.assertEqual(response.status_code, 500)

    def test_create_report(self) -> None:
        user = self.test_schema_objects.test_user_A
        agency = self.test_schema_objects.test_agency_A
        self.session.add_all(
            [
                agency,
                user,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_A,
                    role=schema.UserAccountRole.AGENCY_ADMIN,
                ),
            ]
        )
        self.session.commit()
        self.session.refresh(user)
        month = 3
        year = 2022
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=user.auth0_user_id,
            )

            response = self.client.post(
                "/api/reports",
                json={
                    "agency_id": agency.id,
                    "month": month,
                    "year": year,
                    "frequency": ReportingFrequency.MONTHLY.value,
                },
            )
        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        self.assertEqual(
            response_json["editors"], [{"name": "Jane Doe", "role": "AGENCY_ADMIN"}]
        )
        self.assertEqual(response_json["frequency"], ReportingFrequency.MONTHLY.value)
        self.assertIsNotNone(response_json["last_modified_at"])
        self.assertEqual(response_json["month"], 3)
        self.assertEqual(response_json["status"], ReportStatus.NOT_STARTED.value)
        self.assertEqual(response_json["year"], 2022)

    def test_cannot_get_another_users_reports(self) -> None:
        user_C = self.test_schema_objects.test_user_C
        report = self.test_schema_objects.test_report_monthly

        # user belongs to the wrong agency
        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user_C.auth0_user_id)
            response = self.client.get(f"/api/agencies/{report.source_id}/reports")

        self.assertEqual(response.status_code, 500)

    def test_create_user_if_necessary(self) -> None:
        name = self.test_schema_objects.test_user_A.name
        email = "test@email.com"
        auth0_user_id = self.test_schema_objects.test_user_A.auth0_user_id
        agency = self.test_schema_objects.test_agency_A
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                agency,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_A,
                ),
            ]
        )
        self.session.commit()

        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=auth0_user_id,
            )
            user_response = self.client.put(
                "/api/users",
                json={
                    "name": name,
                    "email": email,
                },
            )

        self.assertEqual(user_response.status_code, 200)
        response_json = assert_type(user_response.json, dict)
        user_A = UserAccountInterface.get_user_by_auth0_user_id(
            session=self.session, auth0_user_id=auth0_user_id
        )
        self.assertEqual(
            response_json["agencies"][0]["team"],
            [
                {
                    "name": name,
                    "email": "test@email.com",  # email set
                    "auth0_user_id": auth0_user_id,
                    "invitation_status": None,
                    "role": None,
                    "user_account_id": user_A.id,
                }
            ],
        )

    def test_update_user_name_and_email(self) -> None:
        new_email_address = "newuser@fake.com"
        new_name = "NEW NAME"
        auth0_user = self.test_schema_objects.test_auth0_user
        db_user = self.test_schema_objects.test_user_A
        self.session.add_all(
            [
                self.test_schema_objects.test_agency_A,
                db_user,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_A,
                ),
            ]
        )
        self.session.commit()

        agency = self.session.query(Agency).one_or_none()
        auth0_user["name"] = new_name
        auth0_user["email"] = new_email_address
        auth0_user["app_metadata"] = JusticeCountsAuth0AppMetadata(
            agency_ids=[agency.id], has_seen_onboarding={}
        )
        self.test_auth0_client.update_user.return_value = auth0_user
        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=auth0_user["user_id"])
            response = self.client.patch(
                "/api/users",
                json={
                    "name": new_name,
                    "email": new_email_address,
                    "auth0_user_id": auth0_user.get("id"),
                },
            )
        self.assertEqual(response.status_code, 200)
        self.test_auth0_client.update_user.assert_called_once_with(
            user_id=auth0_user.get("user_id"),
            name=new_name,
            email=new_email_address,
            email_verified=False,
        )
        self.test_auth0_client.send_verification_email.assert_called_once_with(
            user_id=auth0_user.get("user_id")
        )
        db_user = self.session.query(UserAccount).one()
        self.assertEqual(db_user.name, new_name)
        self.assertEqual(db_user.email, new_email_address)

    def test_invite_and_remove_user_from_agency(self) -> None:
        agency_A = self.test_schema_objects.test_agency_A
        agency_B = self.test_schema_objects.test_agency_B
        user_A = self.test_schema_objects.test_user_A
        self.session.add_all(
            [
                agency_A,
                agency_B,
                user_A,
                # Put user A in both agencies A and B, that way they have
                # permission to invite a new user to both of those agencies
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_A,
                    role=schema.UserAccountRole.AGENCY_ADMIN,
                ),
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_B,
                    role=schema.UserAccountRole.AGENCY_ADMIN,
                ),
            ]
        )
        self.session.commit()
        self.session.refresh(agency_A)
        self.session.refresh(agency_B)
        email_address = "newuser@fake.com"
        name = "NAME"
        auth0_id = "auth0_id_B"
        auth0_user = {
            "email": email_address,
            "user_id": auth0_id,
            "name": name,
        }
        self.test_auth0_client.create_JC_user.return_value = auth0_user
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=user_A.auth0_user_id,
            )
            response = self.client.post(
                f"/api/agencies/{agency_A.id}/users",
                json={
                    "invite_name": name,
                    "invite_email": email_address,
                },
            )

            self.assertEqual(response.status_code, 200)

            # Creates new user in DB with auth0_id
            db_user = UserAccountInterface.get_user_by_auth0_user_id(
                session=self.session, auth0_user_id=auth0_id
            )
            self.assertEqual(db_user.name, name)
            self.assertEqual(db_user.email, email_address)
            self.assertEqual(db_user.auth0_user_id, auth0_id)
            user_account_association = (
                self.session.query(AgencyUserAccountAssociation)
                .filter(AgencyUserAccountAssociation.user_account_id == db_user.id)
                .one()
            )
            self.assertEqual(user_account_association.agency_id, agency_A.id)
            self.assertEqual(
                user_account_association.invitation_status,
                UserAccountInvitationStatus.PENDING,
            )

            # Updates existing user with new agency
            updated_auth0_user = {
                "email": email_address,
                "user_id": auth0_id,
                "name": name,
            }
            self.test_auth0_client.create_JC_user.return_value = updated_auth0_user
            response = self.client.post(
                f"/api/agencies/{agency_B.id}/users",
                json={
                    "invite_name": name,
                    "invite_email": email_address,
                },
            )

            db_user = UserAccountInterface.get_user_by_auth0_user_id(
                session=self.session, auth0_user_id=auth0_id
            )
            user_account_associations = (
                self.session.query(AgencyUserAccountAssociation)
                .filter(AgencyUserAccountAssociation.user_account_id == db_user.id)
                .all()
            )
            self.assertEqual(len(user_account_associations), 2)
            self.assertEqual(user_account_associations[1].agency_id, agency_B.id)
            self.assertEqual(user_account_associations[1].user_account_id, db_user.id)
            self.assertEqual(
                user_account_associations[1].invitation_status,
                UserAccountInvitationStatus.PENDING,
            )

            # Remove user from first agency.
            response = self.client.delete(
                f"/api/agencies/{agency_A.id}/users",
                json={
                    "email": email_address,
                },
            )

            db_user = UserAccountInterface.get_user_by_auth0_user_id(
                session=self.session, auth0_user_id=auth0_id
            )
            user_account_associations = (
                self.session.query(AgencyUserAccountAssociation)
                .filter(AgencyUserAccountAssociation.user_account_id == db_user.id)
                .all()
            )
            self.assertEqual(len(db_user.agency_assocs), 1)
            self.assertEqual(db_user.agency_assocs[0].agency_id, agency_B.id)
            self.assertEqual(user_account_associations[0].agency_id, agency_B.id)
            self.assertEqual(user_account_associations[0].user_account_id, db_user.id)
            self.assertEqual(
                user_account_associations[0].invitation_status,
                UserAccountInvitationStatus.PENDING,
            )

    def test_update_user_role(self) -> None:
        agency_A = self.test_schema_objects.test_agency_A
        user_A = self.test_schema_objects.test_user_A
        user_A.email = "newuser@fake.com"
        self.session.add_all(
            [
                agency_A,
                user_A,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_A,
                    role=schema.UserAccountRole.AGENCY_ADMIN,
                ),
            ]
        )
        self.session.commit()
        self.session.refresh(agency_A)
        self.session.refresh(user_A)
        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user_A.auth0_user_id)
            response = self.client.patch(
                f"/api/agencies/{agency_A.id}/users",
                json={
                    "role": schema.UserAccountRole.AGENCY_ADMIN.value,
                    "email": user_A.email,
                },
            )

            self.assertEqual(response.status_code, 200)
            # Updates the AgencyUserAccountAssociation with the new invitation status
            db_user = self.session.query(UserAccount).one()
            user_account_association = self.session.query(
                AgencyUserAccountAssociation
            ).one()
            self.assertEqual(user_account_association.user_account_id, db_user.id)
            self.assertEqual(user_account_association.agency_id, agency_A.id)
            self.assertEqual(
                user_account_association.role,
                schema.UserAccountRole.AGENCY_ADMIN,
            )

    def test_update_invitation_status(self) -> None:
        agency_A = self.test_schema_objects.test_agency_A
        user_A = self.test_schema_objects.test_user_A
        self.session.add_all([agency_A, user_A])
        self.session.commit()
        self.session.refresh(agency_A)
        self.session.refresh(user_A)
        user_agency_association = AgencyUserAccountAssociation(
            user_account_id=user_A.id,
            agency_id=agency_A.id,
            invitation_status=UserAccountInvitationStatus.PENDING,
        )
        self.session.add(user_agency_association)
        self.session.commit()
        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user_A.auth0_user_id)
            response = self.client.put(
                "/api/users",
                json={
                    "email_verified": True,
                    "agency_id": agency_A.id,
                },
            )

            self.assertEqual(response.status_code, 200)
            # Updates the AgencyUserAccountAssociation with the new invitation status
            db_user = self.session.query(UserAccount).one()
            user_account_association = self.session.query(
                AgencyUserAccountAssociation
            ).one()
            self.assertEqual(user_account_association.user_account_id, db_user.id)
            self.assertEqual(user_account_association.agency_id, agency_A.id)
            self.assertEqual(
                user_account_association.invitation_status,
                UserAccountInvitationStatus.ACCEPTED,
            )

    def test_update_report(self) -> None:
        report = self.test_schema_objects.test_report_monthly
        user = self.test_schema_objects.test_user_A
        self.session.add_all(
            [
                user,
                report,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_A,
                ),
            ]
        )
        self.session.commit()
        with self.app.test_request_context():
            user_account = UserAccountInterface.get_user_by_auth0_user_id(
                session=self.session, auth0_user_id=user.auth0_user_id
            )
            g.user_context = UserContext(
                auth0_user_id=user.auth0_user_id,
            )
            value = 100
            endpoint = f"/api/reports/{report.id}"
            response = self.client.patch(
                endpoint,
                json={
                    "status": "DRAFT",
                    "time_loaded": datetime.datetime.now(
                        tz=datetime.timezone.utc
                    ).timestamp(),
                    "metrics": [
                        {
                            "key": law_enforcement.calls_for_service.key,
                            "value": value,
                        }
                    ],
                },
            )
            self.assertEqual(response.status_code, 200)
            report = self.session.query(Report).one_or_none()
            self.assertEqual(report.status, ReportStatus.DRAFT)
            self.assertEqual(report.last_modified_at.timestamp(), NOW_TIME.timestamp())
            self.assertEqual(get_value(report.datapoints[0]), value)
            self.assertEqual(
                report.modified_by,
                [user_account.id],
            )
            response = self.client.patch(
                endpoint,
                json={
                    "status": "PUBLISHED",
                    "time_loaded": datetime.datetime.now(
                        tz=datetime.timezone.utc
                    ).timestamp(),
                    "metrics": [
                        {
                            "key": law_enforcement.calls_for_service.key,
                            "value": value + 10,
                            "disaggregations": [
                                {
                                    "key": CallType.dimension_identifier(),
                                    "dimensions": [
                                        {
                                            "key": CallType.EMERGENCY.value,
                                            "value": value,
                                        },
                                        {
                                            "key": CallType.NON_EMERGENCY.value,
                                            "value": 10,
                                        },
                                        {
                                            "key": CallType.UNKNOWN.value,
                                            "value": None,
                                        },
                                    ],
                                }
                            ],
                        },
                        {
                            "key": law_enforcement.funding.key,
                            "value": 2000000,
                            "contexts": [
                                {
                                    "key": ContextKey.INCLUDES_EXCLUDES_DESCRIPTION.value,
                                    "value": "our metrics are different because xyz",
                                },
                            ],
                        },
                    ],
                },
            )
            self.assertEqual(response.status_code, 200)
            report = self.session.query(Report).one_or_none()
            self.assertEqual(report.status, ReportStatus.PUBLISHED)
            datapoints = report.datapoints
            self.assertEqual(len(datapoints), 7)
            # Aggregate Value
            self.assertEqual(get_value(datapoints[0]), 110)
            # Emergency Calls
            self.assertEqual(get_value(datapoints[1]), value)
            # Non Emergency Calls
            self.assertEqual(get_value(datapoints[2]), 10)
            # Other Calls
            self.assertEqual(get_value(datapoints[3]), None)
            # Unknown Calls
            self.assertEqual(get_value(datapoints[4]), None)
            # Funding
            self.assertEqual(get_value(datapoints[5]), 2000000)
            # INCLUDES_EXCLUDES_DESCRIPTION
            self.assertEqual(
                get_value(datapoints[6]), "our metrics are different because xyz"
            )

    def test_update_multiple_report_statuses(self) -> None:
        monthly_report_1 = self.test_schema_objects.test_report_monthly
        monthly_report_2 = self.test_schema_objects.test_report_monthly_two
        annual_report_2 = self.test_schema_objects.test_report_annual_two
        user = self.test_schema_objects.test_user_A
        agency = self.test_schema_objects.test_agency_A

        self.session.add_all(
            [
                user,
                monthly_report_1,
                monthly_report_2,
                annual_report_2,
                schema.AgencyUserAccountAssociation(
                    user_account=user,
                    agency=agency,
                    role=schema.UserAccountRole.JUSTICE_COUNTS_ADMIN,
                ),
            ]
        )
        self.session.commit()
        with self.app.test_request_context():
            user_account = UserAccountInterface.get_user_by_auth0_user_id(
                session=self.session, auth0_user_id=user.auth0_user_id
            )
            g.user_context = UserContext(
                auth0_user_id=user.auth0_user_id,
            )

            response = self.client.patch(
                "/api/reports",
                json={
                    "status": "PUBLISHED",
                    "report_ids": [
                        monthly_report_1.id,
                        monthly_report_2.id,
                        annual_report_2.id,
                    ],
                    "agency_id": agency.id,
                },
            )

            self.assertEqual(response.status_code, 200)
            reports = self.session.query(Report).all()
            for current_report in reports:
                self.assertEqual(current_report.status, ReportStatus.PUBLISHED)
            self.assertEqual(
                monthly_report_1.modified_by,
                [user_account.id],
            )

    def test_get_metric_settings_contexts(self) -> None:
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_agency_G,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_G,
                ),
            ]
        )
        self.session.commit()
        agency = self.session.query(Agency).one_or_none()
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            )
            # GET request
            response = self.client.get(f"/api/agencies/{agency.id}/metrics")
            # Check that GET request suceeded
            self.assertEqual(response.status_code, 200)
            # For all dimensions, their 'context' field has 2 possible cases
            # dimension has singleton list [{"key": "ADDITIONAL_CONTEXT", "value": None}] (is an OTHER or UNNOWN member)
            # dimension has singleton list [{"key": "INCLUDES_EXCLUDES_DESCRIPTION", "value": None}] (is not an OTHER or UNKNOWN member)
            if response.json is not None:
                for settings in response.json:
                    for disaggregations in settings["disaggregations"]:
                        for dimension in disaggregations["dimensions"]:
                            dimension_class = DIMENSION_IDENTIFIER_TO_DIMENSION[
                                disaggregations["key"]
                            ]
                            dimension_enum = dimension_class(
                                dimension["key"]
                            )  # type: ignore[abstract]
                            if dimension_enum.name.strip() in [  # type: ignore[attr-defined]
                                "OTHER",
                                "UNKNOWN",
                            ]:
                                # OTHER or UNKNOWN dimension within the aggregation
                                self.assertEqual(
                                    dimension["contexts"],
                                    [
                                        {
                                            "key": "ADDITIONAL_CONTEXT",
                                            "value": None,
                                            "label": "Please describe what data is being included in this breakdown.",
                                        },
                                    ],
                                )
                            else:
                                # not OTHER or UNKNOWN dimension within the aggregation
                                self.assertEqual(
                                    dimension["contexts"],
                                    [
                                        {
                                            "key": "INCLUDES_EXCLUDES_DESCRIPTION",
                                            "value": None,
                                            "label": "If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                                        }
                                    ],
                                )

    def test_update_and_get_metric_settings(self) -> None:
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_agency_G,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_G,
                ),
            ]
        )
        self.session.commit()
        agency = self.session.query(Agency).one_or_none()
        request_body = self.test_schema_objects.get_agency_datapoints_request(
            agency_id=agency.id
        )
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            )

            # PUT request
            response = self.client.put(
                f"/api/agencies/{agency.id}/metrics",
                json=request_body,
            )

            # Check that PUT request suceeded
            self.assertEqual(response.status_code, 200)
            agency_datapoints = self.session.query(Datapoint).all()
            datapoints_with_additional_context = []
            for d in agency_datapoints:
                if (
                    d.context_key is not None
                    and d.value is not None
                    and d.dimension_identifier_to_member is not None
                ):
                    datapoints_with_additional_context.append(d)
            self.assertEqual(len(datapoints_with_additional_context), 2)
            self.assertEqual(
                datapoints_with_additional_context[0].metric_definition_key,
                "PRISONS_TOTAL_STAFF",
            )
            self.assertEqual(datapoints_with_additional_context[0].source, agency)
            self.assertEqual(
                datapoints_with_additional_context[0].context_key, "ADDITIONAL_CONTEXT"
            )
            self.assertEqual(
                datapoints_with_additional_context[0].value,
                "Other user entered text...",
            )
            self.assertEqual(
                datapoints_with_additional_context[0].dimension_identifier_to_member,
                {"metric/prisons/staff/type": "OTHER"},
            )
            self.assertEqual(
                datapoints_with_additional_context[1].value,
                "Unknown user entered text...",
            )
            self.assertEqual(
                datapoints_with_additional_context[1].dimension_identifier_to_member,
                {"metric/prisons/staff/type": "UNKNOWN"},
            )

            # GET request
            response = self.client.get(f"/api/agencies/{agency.id}/metrics")
            # Check that GET request suceeded
            self.assertEqual(response.status_code, 200)
            # Check that the we can get the previoulsy stored additional context
            if response.json is None:
                raise ValueError("Expected nonnull response.json")
            other_context = response.json[2]["disaggregations"][0]["dimensions"][4][
                "contexts"
            ]
            unknown_context = response.json[2]["disaggregations"][0]["dimensions"][5][
                "contexts"
            ]
            self.assertEqual(
                other_context,
                [
                    {
                        "key": "ADDITIONAL_CONTEXT",
                        "value": "Other user entered text...",
                        "label": "Please describe what data is being included in this breakdown.",
                    }
                ],
            )
            self.assertEqual(
                unknown_context,
                [
                    {
                        "key": "ADDITIONAL_CONTEXT",
                        "value": "Unknown user entered text...",
                        "label": "Please describe what data is being included in this breakdown.",
                    }
                ],
            )

    def test_update_metric_settings(self) -> None:
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_agency_G,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_G,
                ),
            ]
        )
        self.session.commit()
        agency = self.session.query(Agency).one_or_none()
        request_body = self.test_schema_objects.get_agency_datapoints_request(
            agency_id=agency.id
        )
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            )
            response = self.client.put(
                f"/api/agencies/{agency.id}/metrics",
                json=request_body,
            )
            self.assertEqual(response.status_code, 200)
            agency_datapoint_histories = self.session.query(DatapointHistory).all()
            self.assertEqual(len(agency_datapoint_histories), 0)
            agency_datapoints = self.session.query(Datapoint).all()

            # 21 total datapoints:
            #  3 enabled/disabled metric datapoints (one for each metric): PRISONS_BUDGET, PRISONS_TOTAL_STAFF, PRISONS_GRIEVANCES_UPHELD
            #  7 enabled/disabled dimension datapoints (one for each dimension)
            #  8 includes/excludes datapoints (2 at the metric level, 6 at the disaggregation level)
            #  3 context datapoint
            self.assertEqual(len(agency_datapoints), 21)
            includes_excludes_key_and_dimension_to_datapoint = {
                (
                    d.includes_excludes_key,
                    (
                        list(d.dimension_identifier_to_member.values())[0]
                        if d.dimension_identifier_to_member is not None
                        else None
                    ),
                ): d
                for d in agency_datapoints
            }
            # Volunteer and Intern includes/excludes datapoints will be created because
            # their default was IncludesExcludesSetting.NO and they are now included. Their
            # IncludeExcludesSetting is now IncludesExcludesSetting.YES
            self.assertEqual(
                includes_excludes_key_and_dimension_to_datapoint[
                    (PrisonStaffIncludesExcludes.VOLUNTEER.name, None)
                ].value,
                "Yes",
            )
            self.assertEqual(
                includes_excludes_key_and_dimension_to_datapoint[
                    (PrisonStaffIncludesExcludes.INTERN.name, None)
                ].value,
                "Yes",
            )
            # All includes/excludes that were set to YES should have a datapoint
            # with a dimension_identifier_to_member.
            for dimension, includes_excludes_lst in assert_type(
                prisons.staff.aggregated_dimensions, list
            )[0].dimension_to_includes_excludes.items():
                for includes_excludes in includes_excludes_lst:
                    for (
                        member,
                        default_includes_excludes_setting,
                    ) in includes_excludes.member_to_default_inclusion_setting.items():
                        if (
                            default_includes_excludes_setting
                            is not IncludesExcludesSetting.YES
                        ):
                            saved_datapoint = (
                                includes_excludes_key_and_dimension_to_datapoint[
                                    (member.name, dimension.name)
                                ]
                            )
                            self.assertEqual(
                                saved_datapoint.value,
                                "Yes",
                            )
                            self.assertEqual(
                                saved_datapoint.dimension_identifier_to_member,
                                {
                                    dimension.dimension_identifier(): dimension.dimension_name
                                },
                            )
            # Reset includes/excludes settings at the metric setting back to
            # their default.
            update_request_body = (
                self.test_schema_objects.get_agency_datapoints_request(
                    agency_id=agency.id, reset_to_default=True
                )
            )
            response = self.client.put(
                f"/api/agencies/{agency.id}/metrics",
                json=update_request_body,
            )
            self.assertEqual(response.status_code, 200)
            agency_datapoint_histories = self.session.query(DatapointHistory).all()
            # Two includes/excludes settings were changed from "Yes" -> "No"
            self.assertEqual(len(agency_datapoint_histories), 2)
            agency_datapoints = self.session.query(Datapoint).all()
            # Amount of agency_datapoints won't change. Only two datapoints were updated.
            self.assertEqual(len(agency_datapoints), 21)
            includes_excludes_key_and_dimension_to_datapoint = {
                (
                    d.includes_excludes_key,
                    (
                        list(d.dimension_identifier_to_member.values())[0]
                        if d.dimension_identifier_to_member is not None
                        else None
                    ),
                ): d
                for d in agency_datapoints
            }
            # Volunteer and Intern includes/excludes datapoints will be saved as "No"
            # even though "No" is their default because we do not delete includes/excludes
            # datapoints when settings are reset to default
            self.assertEqual(
                includes_excludes_key_and_dimension_to_datapoint[
                    (PrisonStaffIncludesExcludes.VOLUNTEER.name, None)
                ].value,
                "No",
            )
            self.assertEqual(
                includes_excludes_key_and_dimension_to_datapoint[
                    (PrisonStaffIncludesExcludes.INTERN.name, None)
                ].value,
                "No",
            )

    def test_upload_spreadsheet_wrong_filetype(self) -> None:
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_agency_A,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_A,
                ),
            ]
        )
        self.session.commit()
        agency = self.session.query(Agency).one_or_none()
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            )

            response = self.client.post(
                "/api/spreadsheets",
                data={
                    "agency_id": agency.id,
                    "system": System.LAW_ENFORCEMENT.value,
                    "file": (
                        self.bulk_upload_test_files / "law_enforcement/arrests.pdf"
                    ).open("rb"),
                },
            )
            self.assertEqual(response.status_code, 500)

    def test_upload_spreadsheet(self) -> None:
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_agency_A,
                AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_A,
                ),
            ]
        )
        self.session.commit()
        agency = self.session.query(Agency).one_or_none()
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            )

            response = self.client.post(
                "/api/spreadsheets",
                data={
                    "agency_id": agency.id,
                    "system": System.LAW_ENFORCEMENT.value,
                    "file": (
                        self.bulk_upload_test_files
                        / "law_enforcement/law_enforcement_metrics.xlsx"
                    ).open("rb"),
                },
            )
            self.assertEqual(response.status_code, 200)
            response_dict = assert_type(response.json, dict)
            self.assertEqual(
                response_dict.get("file_name"), "law_enforcement_metrics.xlsx"
            )
            spreadsheet = self.session.query(Spreadsheet).one()
            self.assertEqual(spreadsheet.system, System.LAW_ENFORCEMENT)
            self.assertEqual(
                spreadsheet.uploaded_by,
                self.test_schema_objects.test_user_A.auth0_user_id,
            )
            self.assertEqual(
                spreadsheet.ingested_at,
                None,
            )
            self.assertEqual(spreadsheet.original_name, "law_enforcement_metrics.xlsx")
            standardized_name = f"{agency.id}:LAW_ENFORCEMENT:{datetime.datetime.now(tz=datetime.timezone.utc).timestamp()}.xlsx"
            self.assertEqual(
                spreadsheet.standardized_name,
                standardized_name,
            )
            self.assertEqual(1, len(self.fs.uploaded_paths))
            path = one(self.fs.uploaded_paths)
            self.assertEqual(
                GcsfsFilePath(
                    bucket_name="justice-counts-staging-publisher-uploads",
                    blob_name=standardized_name,
                ),
                path,
            )

    def test_upload_csv(self) -> None:
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_agency_A,
                AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_A,
                ),
            ]
        )
        self.session.commit()
        agency = self.session.query(Agency).one_or_none()
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            )
            file_name = "arrests.csv"
            file_path = create_csv_file(
                file_name=file_name,
                system=schema.System.LAW_ENFORCEMENT,
                metric="arrests",
            )
            with open(Path(file_path), "rb") as file:
                response = self.client.post(
                    "/api/spreadsheets",
                    data={
                        "agency_id": agency.id,
                        "system": System.LAW_ENFORCEMENT.value,
                        "file": file,
                    },
                )
            self.assertEqual(response.status_code, 200)
            response_dict = assert_type(response.json, dict)
            self.assertEqual(response_dict.get("file_name"), file_name)
            spreadsheet = self.session.query(Spreadsheet).one()
            self.assertEqual(spreadsheet.system, System.LAW_ENFORCEMENT)
            self.assertEqual(
                spreadsheet.uploaded_by,
                self.test_schema_objects.test_user_A.auth0_user_id,
            )
            # Commenting this out since we're running into some weird timezone-related
            # behavior. This test passes on GitHub, but not locally. Locally, it seems
            # that the datetime is stripped of the timezone.
            # self.assertEqual(
            #     spreadsheet.ingested_at, datetime.datetime.now()
            # )
            self.assertEqual(spreadsheet.original_name, file_name)
            standardized_name = f"{agency.id}:LAW_ENFORCEMENT:{datetime.datetime.now(tz=datetime.timezone.utc).timestamp()}.xlsx"
            self.assertEqual(
                spreadsheet.standardized_name,
                standardized_name,
            )
            self.assertEqual(1, len(self.fs.uploaded_paths))
            path = one(self.fs.uploaded_paths)
            self.assertEqual(
                GcsfsFilePath(
                    bucket_name="justice-counts-staging-publisher-uploads",
                    blob_name=standardized_name,
                ),
                path,
            )
            # Ensure no warnings were triggered for this valid CSV file upload.
            arrests_metric = [
                x
                for x in response_dict["metrics"]
                if x["key"] == "LAW_ENFORCEMENT_ARRESTS"
            ]
            self.assertEqual(len(arrests_metric), 1)
            self.assertEqual(len(arrests_metric[0]["metric_errors"]), 0)

    def test_upload_and_ingest_spreadsheet(self) -> None:
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_agency_A,
            ]
        )
        self.session.commit()
        agency = self.session.query(Agency).one_or_none()
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            )
            file_path = create_excel_file(
                system=schema.System.LAW_ENFORCEMENT,
                file_name="test_upload_and_ingest_spreadsheet.xlsx",
            )
            with open(Path(file_path), "rb") as file:
                response = self.client.post(
                    "/api/spreadsheets",
                    data={
                        "agency_id": agency.id,
                        "system": System.LAW_ENFORCEMENT.value,
                        "ingest_on_upload": True,
                        "file": file,
                    },
                )
            self.assertEqual(response.status_code, 200)
            response_dict = assert_type(response.json, dict)
            self.assertEqual(len(response_dict["metrics"]), 8)
            self.assertEqual(len(response_dict["non_metric_errors"]), 0)
            self.assertEqual(len(response_dict["updated_reports"]), 0)
            self.assertEqual(len(response_dict["unchanged_reports"]), 0)
            self.assertEqual(len(response_dict["new_reports"]), 9)
            spreadsheet = self.session.query(Spreadsheet).one()
            self.assertEqual(spreadsheet.system, System.LAW_ENFORCEMENT)
            self.assertEqual(
                spreadsheet.uploaded_by,
                self.test_schema_objects.test_user_A.auth0_user_id,
            )
            self.assertEqual(
                spreadsheet.ingested_by,
                self.test_schema_objects.test_user_A.auth0_user_id,
            )
            self.assertEqual(
                spreadsheet.ingested_at.timestamp(),
                NOW_TIME.timestamp(),
            )
            self.assertEqual(
                spreadsheet.original_name, "test_upload_and_ingest_spreadsheet.xlsx"
            )
            standardized_name = f"{agency.id}:LAW_ENFORCEMENT:{datetime.datetime.now(tz=datetime.timezone.utc).timestamp()}.xlsx"
            self.assertEqual(
                spreadsheet.standardized_name,
                standardized_name,
            )
            self.assertEqual(1, len(self.fs.uploaded_paths))
            path = one(self.fs.uploaded_paths)
            self.assertEqual(
                GcsfsFilePath(
                    bucket_name="justice-counts-staging-publisher-uploads",
                    blob_name=standardized_name,
                ),
                path,
            )
            reports = ReportInterface.get_reports_by_agency_id(
                session=self.session,
                agency_id=agency.id,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}
            self.assertEqual(
                set(reports_by_instance.keys()),
                {
                    "2021 Annual Metrics",
                    "2022 Annual Metrics",
                    "2023 Annual Metrics",
                    "01 2021 Metrics",
                    "02 2021 Metrics",
                    "01 2022 Metrics",
                    "02 2022 Metrics",
                    "01 2023 Metrics",
                    "02 2023 Metrics",
                },
            )
            self.assertIsNotNone(response_dict.get("updated_reports"))
            self.assertIsNotNone(response_dict.get("new_reports"))
            self.assertIsNotNone(response_dict.get("unchanged_reports"))

    def test_get_spreadsheets(self) -> None:
        user_agency = self.test_schema_objects.test_agency_E
        not_user_agency = self.test_schema_objects.test_agency_A
        user = self.test_schema_objects.test_user_A
        association = AgencyUserAccountAssociation(
            user_account=user, agency=user_agency
        )
        self.session.add_all([user_agency, user, not_user_agency, association])
        self.session.commit()
        self.session.refresh(user_agency)
        self.session.refresh(not_user_agency)
        self.session.add_all(
            [
                self.test_schema_objects.get_test_spreadsheet(
                    system=System.LAW_ENFORCEMENT,
                    user_id=user.auth0_user_id,
                    agency_id=not_user_agency.id,
                ),
                self.test_schema_objects.get_test_spreadsheet(
                    system=System.SUPERVISION,
                    user_id=user.auth0_user_id,
                    agency_id=user_agency.id,
                ),
                self.test_schema_objects.get_test_spreadsheet(
                    system=System.PAROLE,
                    user_id=user.auth0_user_id,
                    agency_id=user_agency.id,
                    upload_offset=25,
                ),
                self.test_schema_objects.get_test_spreadsheet(
                    system=System.PROBATION,
                    is_ingested=True,
                    user_id=user.auth0_user_id,
                    agency_id=user_agency.id,
                ),
            ]
        )
        self.session.commit()
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=user.auth0_user_id,
            )

            response = self.client.get(
                f"/api/agencies/{user_agency.id}/spreadsheets",
            )
            self.assertEqual(response.status_code, 200)
            spreadsheets = assert_type(response.json, list)
            self.assertEqual(len(spreadsheets), 3)
            probation_spreadsheet = spreadsheets[0]
            self.assertEqual(
                probation_spreadsheet.get("name"), "PROBATION_metrics.xlsx"
            )
            self.assertEqual(
                probation_spreadsheet.get("uploaded_at"),
                NOW_TIME.timestamp() * 1000,
            )
            self.assertEqual(
                probation_spreadsheet.get("uploaded_by_v2"),
                {"name": self.test_schema_objects.test_user_A.name, "role": None},
            )

            self.assertEqual(
                probation_spreadsheet.get("ingested_at"),
                (NOW_TIME + (datetime.timedelta(50))).timestamp() * 1000,
            )
            self.assertEqual(probation_spreadsheet.get("status"), "INGESTED")
            self.assertEqual(probation_spreadsheet.get("system"), "PROBATION")
            parole_spreadsheet = spreadsheets[1]
            self.assertEqual(parole_spreadsheet.get("name"), "PAROLE_metrics.xlsx")

            self.assertEqual(
                parole_spreadsheet.get("uploaded_at"),
                (NOW_TIME + (datetime.timedelta(25))).timestamp() * 1000,
            )
            self.assertEqual(parole_spreadsheet.get("ingested_at"), None)
            self.assertEqual(parole_spreadsheet.get("status"), "UPLOADED")
            self.assertEqual(parole_spreadsheet.get("system"), "PAROLE")
            supervision_spreadsheet = spreadsheets[2]
            self.assertEqual(
                supervision_spreadsheet.get("name"),
                "SUPERVISION_metrics.xlsx",
            )
            self.assertEqual(
                supervision_spreadsheet.get("uploaded_at"),
                NOW_TIME.timestamp() * 1000,
            )
            self.assertEqual(supervision_spreadsheet.get("ingested_at"), None)
            self.assertEqual(supervision_spreadsheet.get("status"), "UPLOADED")
            self.assertEqual(supervision_spreadsheet.get("system"), "SUPERVISION")

    def test_download_spreadsheet_fail_without_permissions(self) -> None:
        agency_A = self.test_schema_objects.test_agency_A
        agency_B = self.test_schema_objects.test_agency_B
        self.session.add_all([agency_A, agency_B])
        self.session.commit()
        self.session.refresh(agency_A)
        self.session.refresh(agency_B)
        spreadsheet = self.test_schema_objects.get_test_spreadsheet(
            system=System.LAW_ENFORCEMENT,
            user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            agency_id=agency_A.id,
        )
        self.session.add(spreadsheet)
        self.session.commit()
        self.session.refresh(spreadsheet)
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            )

            response = self.client.get(f"/api/spreadsheets/{spreadsheet.id}")
            self.assertEqual(response.status_code, 500)

    def test_download_spreadsheet(self) -> None:
        agency = self.test_schema_objects.test_agency_A
        user = self.test_schema_objects.test_user_A
        self.session.add_all(
            [
                user,
                agency,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_A,
                ),
            ]
        )
        self.session.commit()
        self.session.refresh(agency)
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=user.auth0_user_id,
            )
            # Upload spreadsheet
            upload_response = self.client.post(
                "/api/spreadsheets",
                data={
                    "agency_id": agency.id,
                    "system": System.LAW_ENFORCEMENT.value,
                    "file": (
                        self.bulk_upload_test_files
                        / "law_enforcement/law_enforcement_metrics.xlsx"
                    ).open("rb"),
                },
            )
            self.assertEqual(upload_response.status_code, 200)
            spreadsheet = self.session.query(Spreadsheet).one()
            # Download spreadsheet
            download_response = self.client.get(f"/api/spreadsheets/{spreadsheet.id}")
            self.assertEqual(download_response.status_code, 200)

    def test_update_spreadsheet(self) -> None:
        agency = self.test_schema_objects.test_agency_E
        user = self.test_schema_objects.test_user_A
        self.session.add_all(
            [
                agency,
                user,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_E,
                    role=schema.UserAccountRole.JUSTICE_COUNTS_ADMIN,
                ),
            ]
        )
        self.session.commit()
        self.session.refresh(agency)
        self.session.refresh(user)
        spreadsheet = self.test_schema_objects.get_test_spreadsheet(
            system=System.SUPERVISION,
            user_id=user.auth0_user_id,
            agency_id=agency.id,
        )
        self.session.add(spreadsheet)
        self.session.commit()
        self.session.refresh(spreadsheet)

        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=user.auth0_user_id,
            )

            response = self.client.patch(
                f"/api/spreadsheets/{spreadsheet.id}",
                json={"status": SpreadsheetStatus.INGESTED.value},
            )
            self.assertEqual(response.status_code, 200)
            spreadsheet_json = assert_type(response.json, dict)
            self.assertEqual(
                spreadsheet_json.get("status"), SpreadsheetStatus.INGESTED.value
            )
            db_spreadsheet = self.session.query(Spreadsheet).one()
            self.assertEqual(db_spreadsheet.ingested_by, user.auth0_user_id)
            self.assertEqual(
                db_spreadsheet.ingested_at.timestamp(), NOW_TIME.timestamp()
            )

    def test_delete_spreadsheet(self) -> None:
        agency = self.test_schema_objects.test_agency_A
        user = self.test_schema_objects.test_user_A
        user_agency_association = AgencyUserAccountAssociation(
            user_account=user,
            agency=agency,
            invitation_status=UserAccountInvitationStatus.PENDING,
        )
        self.session.add_all([agency, user, user_agency_association])
        self.session.commit()
        self.session.refresh(agency)
        self.session.refresh(user)
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=user.auth0_user_id,
            )
            upload_response = self.client.post(
                "/api/spreadsheets",
                data={
                    "agency_id": agency.id,
                    "system": System.LAW_ENFORCEMENT.value,
                    "file": (
                        self.bulk_upload_test_files
                        / "law_enforcement/law_enforcement_metrics.xlsx"
                    ).open("rb"),
                },
            )
            self.assertEqual(upload_response.status_code, 200)
            spreadsheet = self.session.query(Spreadsheet).one()
            response = self.client.delete(f"/api/spreadsheets/{spreadsheet.id}")
            self.assertEqual(response.status_code, 200)
            db_spreadsheet = self.session.query(Spreadsheet).one_or_none()
            self.assertEqual(db_spreadsheet, None)
            path = GcsfsFilePath(
                bucket_name="justice-counts-staging-publisher-uploads",
                blob_name=f"{str(agency.id)}:{System.LAW_ENFORCEMENT.value}:{NOW_TIME.timestamp()}.xlsx",
            )
            self.assertEqual(
                self.fs.exists(path),
                False,
            )

    def test_get_datapoints_by_agency_id(self) -> None:
        user_A = self.test_schema_objects.test_user_A
        report = self.test_schema_objects.test_report_monthly
        self.session.add_all(
            [
                report,
                user_A,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_A,
                ),
            ]
        )

        report_metric = self.test_schema_objects.arrests_metric
        inserts: List[schema.Datapoint] = []
        updates: List[schema.Datapoint] = []
        histories: List[schema.DatapointHistory] = []
        ReportInterface.add_or_update_metric(
            session=self.session,
            inserts=inserts,
            updates=updates,
            histories=histories,
            report=report,
            report_metric=report_metric,
            user_account=user_A,
            upload_method=UploadMethod.BULK_UPLOAD,
        )
        DatapointInterface.flush_report_datapoints(
            session=self.session,
            inserts=inserts,
            updates=updates,
            histories=histories,
        )
        self.session.commit()

        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user_A.auth0_user_id)
            response = self.client.get(f"/api/agencies/{report.source_id}/datapoints")

        self.assertEqual(response.status_code, 200)
        agency_datapoints = self.session.query(Datapoint).all()
        response_json = assert_type(response.json, dict)
        response_json_datapoints = assert_type(response_json["datapoints"], list)
        self.assertEqual(len(agency_datapoints), len(response_json_datapoints))

        response_json_datapoint = assert_type(response_json_datapoints[0], dict)
        self.assertEqual(response_json_datapoint["dimension_display_name"], None)
        self.assertEqual(response_json_datapoint["disaggregation_display_name"], None)
        self.assertEqual(
            response_json_datapoint["end_date"], "Fri, 01 Jul 2022 00:00:00 GMT"
        )
        self.assertEqual(
            response_json_datapoint["frequency"], ReportingFrequency.MONTHLY.value
        )
        self.assertEqual(response_json_datapoint["is_published"], False)
        self.assertEqual(
            response_json_datapoint["metric_definition_key"],
            "LAW_ENFORCEMENT_ARRESTS",
        )
        self.assertEqual(response_json_datapoint["metric_display_name"], "Arrests")
        self.assertEqual(
            response_json_datapoint["start_date"], "Wed, 01 Jun 2022 00:00:00 GMT"
        )
        self.assertEqual(response_json_datapoint["value"], 5000)

        response_json_dimensions = response_json[
            "dimension_names_by_metric_and_disaggregation"
        ]

        self.assertEqual(
            response_json_dimensions,
            {
                "LAW_ENFORCEMENT_ARRESTS": {
                    "Biological Sex": [
                        "Male Biological Sex",
                        "Female Biological Sex",
                        "Unknown Biological Sex",
                    ],
                    "Offense Type": [
                        "Person Offenses",
                        "Property Offenses",
                        "Drug Offenses",
                        "Public Order Offenses",
                        "Other Offenses",
                        "Unknown Offenses",
                    ],
                    "Race / Ethnicity": [
                        "American Indian or Alaska Native / Hispanic or Latino",
                        "Asian / Hispanic or Latino",
                        "Black / Hispanic or Latino",
                        "More than one race / Hispanic or Latino",
                        "Native Hawaiian or Pacific Islander / Hispanic or Latino",
                        "White / Hispanic or Latino",
                        "Other / Hispanic or Latino",
                        "Unknown / Hispanic or Latino",
                        "American Indian or Alaska Native / Not Hispanic or Latino",
                        "Asian / Not Hispanic or Latino",
                        "Black / Not Hispanic or Latino",
                        "More than one race / Not Hispanic or Latino",
                        "Native Hawaiian or Pacific Islander / Not Hispanic or Latino",
                        "White / Not Hispanic or Latino",
                        "Other / Not Hispanic or Latino",
                        "Unknown / Not Hispanic or Latino",
                        "American Indian or Alaska Native / Unknown Ethnicity",
                        "Asian / Unknown Ethnicity",
                        "Black / Unknown Ethnicity",
                        "More than one race / Unknown Ethnicity",
                        "Native Hawaiian or Pacific Islander / Unknown Ethnicity",
                        "White / Unknown Ethnicity",
                        "Other / Unknown Ethnicity",
                        "Unknown / Unknown Ethnicity",
                    ],
                },
                "LAW_ENFORCEMENT_FUNDING": {
                    "Funding Type": [
                        "State Appropriation",
                        "County or Municipal Appropriation",
                        "Asset Forfeiture",
                        "Grants",
                        "Other Funding",
                        "Unknown Funding",
                    ]
                },
                "LAW_ENFORCEMENT_EXPENSES": {
                    "Expense Type": [
                        "Personnel",
                        "Training",
                        "Facilities and Equipment",
                        "Other Expenses",
                        "Unknown Expenses",
                    ]
                },
                "LAW_ENFORCEMENT_CALLS_FOR_SERVICE": {
                    "Call Type": [
                        "Emergency Calls",
                        "Non-emergency Calls",
                        "Other Calls",
                        "Unknown Calls",
                    ]
                },
                "LAW_ENFORCEMENT_COMPLAINTS_SUSTAINED": {
                    "Complaint Type": [
                        "Excessive Uses of Force",
                        "Discrimination or Racial Bias",
                        "Other Complaints",
                        "Unknown Complaints",
                    ]
                },
                "LAW_ENFORCEMENT_REPORTED_CRIME": {
                    "Offense Type": [
                        "Person Offenses",
                        "Property Offenses",
                        "Drug Offenses",
                        "Public Order Offenses",
                        "Other Offenses",
                        "Unknown Offenses",
                    ]
                },
                "LAW_ENFORCEMENT_TOTAL_STAFF": {
                    "Staff Type": [
                        "Sworn/Uniformed Police Officers",
                        "Civilian Staff",
                        "Mental Health and Crisis Intervention Team Staff",
                        "Victim Advocate Staff",
                        "Other Staff",
                        "Unknown Staff",
                        "Vacant Positions (Any Staff Type)",
                    ],
                    "Race / Ethnicity": [
                        "American Indian or Alaska Native / Hispanic or Latino",
                        "Asian / Hispanic or Latino",
                        "Black / Hispanic or Latino",
                        "More than one race / Hispanic or Latino",
                        "Native Hawaiian or Pacific Islander / Hispanic or Latino",
                        "White / Hispanic or Latino",
                        "Other / Hispanic or Latino",
                        "Unknown / Hispanic or Latino",
                        "American Indian or Alaska Native / Not Hispanic or Latino",
                        "Asian / Not Hispanic or Latino",
                        "Black / Not Hispanic or Latino",
                        "More than one race / Not Hispanic or Latino",
                        "Native Hawaiian or Pacific Islander / Not Hispanic or Latino",
                        "White / Not Hispanic or Latino",
                        "Other / Not Hispanic or Latino",
                        "Unknown / Not Hispanic or Latino",
                        "American Indian or Alaska Native / Unknown Ethnicity",
                        "Asian / Unknown Ethnicity",
                        "Black / Unknown Ethnicity",
                        "More than one race / Unknown Ethnicity",
                        "Native Hawaiian or Pacific Islander / Unknown Ethnicity",
                        "White / Unknown Ethnicity",
                        "Other / Unknown Ethnicity",
                        "Unknown / Unknown Ethnicity",
                    ],
                    "Biological Sex": [
                        "Male Biological Sex",
                        "Female Biological Sex",
                        "Unknown Biological Sex",
                    ],
                },
                "LAW_ENFORCEMENT_USE_OF_FORCE_INCIDENTS": {
                    "Force Type": [
                        "Physical Force",
                        "Restraint",
                        "Firearm",
                        "Other Weapon",
                        "Other Force",
                        "Unknown Force",
                    ]
                },
            },
        )

    def test_session(self) -> None:
        # Add data
        name = "Agency Alpha"
        self.session.add(Source(name=name))
        self.session.commit()

        # Query data
        source = self.session.query(Source).one_or_none()

        self.assertEqual(source.name, name)

    def test_feed_unauthenticated(self) -> None:
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_agency_A,
            ]
        )
        self.session.commit()
        self.session.flush()
        agency = self.test_schema_objects.test_agency_A

        law_enforcement_excel = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                "..",
                "bulk_upload/bulk_upload_fixtures/law_enforcement/law_enforcement_metrics.xlsx",
            )
        )
        uploader = WorkbookUploader(
            agency=agency,
            system=schema.System.LAW_ENFORCEMENT,
            user_account=self.test_schema_objects.test_user_A,
            metric_key_to_metric_interface={},
        )
        uploader.upload_workbook(
            session=self.session,
            xls=pd.ExcelFile(law_enforcement_excel),
            metric_definitions=METRICS_BY_SYSTEM[schema.System.LAW_ENFORCEMENT.value],
            filename=law_enforcement_excel,
            upload_filetype=BulkUploadFileType.XLSX,
            upload_method=UploadMethod.BULK_UPLOAD,
        )
        self.session.commit()

        # No data has been published; feed should be empty
        empty_feed_response = self.client.get(
            f"/feed/{agency.id}", query_string={"metric": "arrests"}
        )
        self.assertEqual(empty_feed_response.status_code, 200)
        self.assertEqual(empty_feed_response.data, b"")

        # Some data has been published
        report = self.session.query(schema.Report).limit(1).one()
        report.status = ReportStatus.PUBLISHED
        self.session.commit()
        partial_feed_response = self.client.get(
            f"/feed/{agency.id}", query_string={"metric": "arrests"}
        )
        self.assertEqual(partial_feed_response.status_code, 200)

        # Data has been published; feed should not be empty
        self.session.query(schema.Report).update({"status": ReportStatus.PUBLISHED})
        self.session.commit()

        feed_response_no_metric = self.client.get(f"/feed/{agency.id}")
        self.assertEqual(feed_response_no_metric.status_code, 200)

        feed_response_with_metric = self.client.get(
            f"/feed/{agency.id}", query_string={"metric": "arrests"}
        )
        self.assertEqual(feed_response_with_metric.status_code, 200)

    def test_feed_authenticated(self) -> None:
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_agency_A,
            ]
        )
        self.session.commit()
        self.session.flush()
        agency = self.test_schema_objects.test_agency_A

        law_enforcement_excel = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                "..",
                "bulk_upload/bulk_upload_fixtures/law_enforcement/law_enforcement_metrics.xlsx",
            )
        )
        uploader = WorkbookUploader(
            agency=agency,
            system=schema.System.LAW_ENFORCEMENT,
            user_account=self.test_schema_objects.test_user_A,
            metric_key_to_metric_interface={},
        )
        uploader.upload_workbook(
            session=self.session,
            xls=pd.ExcelFile(law_enforcement_excel),
            metric_definitions=METRICS_BY_SYSTEM[schema.System.LAW_ENFORCEMENT.value],
            filename=law_enforcement_excel,
            upload_method=UploadMethod.BULK_UPLOAD,
            upload_filetype=BulkUploadFileType.XLSX,
        )
        self.session.commit()

        # No data has been published; feed should not be empty
        empty_feed_response = self.client.get(
            f"api/feed/{agency.id}", query_string={"metric": "arrests"}
        )
        self.assertEqual(empty_feed_response.status_code, 200)
        self.assertNotEqual(empty_feed_response.data, b"")

    def test_update_agency_systems(self) -> None:
        user = self.test_schema_objects.test_user_A
        agency = self.test_schema_objects.test_agency_C
        self.session.add_all(
            [
                user,
                agency,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_C,
                ),
            ]
        )
        self.session.commit()
        self.session.refresh(agency)
        agency_id = agency.id

        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user.auth0_user_id)
            response = self.client.patch(
                f"/api/agencies/{agency_id}",
                json={"systems": [schema.System.PAROLE.value]},
            )

            self.assertEqual(response.status_code, 500)

            # Now give the user permission
            assoc = self.session.query(AgencyUserAccountAssociation).one()
            assoc.role = schema.UserAccountRole.AGENCY_ADMIN
            g.user_context = UserContext(auth0_user_id=user.auth0_user_id)
            response = self.client.patch(
                f"/api/agencies/{agency_id}",
                json={"systems": [schema.System.PAROLE.value]},
            )

            self.assertEqual(response.status_code, 200)

            g.user_context = UserContext(auth0_user_id=user.auth0_user_id)
            response = self.client.patch(
                f"/api/agencies/{agency_id}",
                json={"systems": [schema.System.PAROLE.value]},
            )

            self.assertEqual(response.status_code, 200)

            agency = AgencyInterface.get_agency_by_id(
                session=self.session, agency_id=agency_id
            )
            self.assertEqual(
                set(agency.systems),
                {schema.System.PAROLE.value, schema.System.SUPERVISION.value},
            )

    def test_update_and_get_agency_settings(self) -> None:
        user = self.test_schema_objects.test_user_A
        agency = self.test_schema_objects.test_agency_C
        self.session.add_all(
            [
                user,
                agency,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_C,
                    subscribed=True,
                    days_after_time_period_to_send_email=12,
                ),
            ]
        )
        self.session.commit()
        self.session.refresh(agency)
        agency_id = agency.id

        def update_agency_settings() -> Any:
            return self.client.patch(
                f"/api/agencies/{agency_id}",
                json={
                    "settings": [
                        {
                            "setting_type": AgencySettingType.PURPOSE_AND_FUNCTIONS.value,
                            "value": "My agency has the following purpose and functions ...",
                        },
                        {
                            "setting_type": AgencySettingType.HOMEPAGE_URL.value,
                            "value": "www.agencyhomepage.com",
                        },
                    ]
                },
            )

        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user.auth0_user_id)
            update_response = update_agency_settings()
            self.assertEqual(update_response.status_code, 500)

            # Now give the user permission
            assoc = self.session.query(AgencyUserAccountAssociation).one()
            assoc.role = schema.UserAccountRole.AGENCY_ADMIN
            g.user_context = UserContext(auth0_user_id=user.auth0_user_id)
            update_response = update_agency_settings()
            self.assertEqual(update_response.status_code, 200)

            # First, update agency settings
            update_response = update_agency_settings()
            g.user_context = UserContext(auth0_user_id=user.auth0_user_id)

            # First, update agency settings
            update_response = update_agency_settings()
            # Next, get the agency setting
            get_response = self.client.get(f"/api/agencies/{agency_id}")

            self.assertEqual(update_response.status_code, 200)
            self.assertEqual(get_response.status_code, 200)

            self.assertEqual(
                get_response.json,
                {
                    "settings": [
                        {
                            "setting_type": AgencySettingType.PURPOSE_AND_FUNCTIONS.value,
                            "value": "My agency has the following purpose and functions ...",
                            "source_id": agency_id,
                        },
                        {
                            "setting_type": AgencySettingType.HOMEPAGE_URL.value,
                            "value": "www.agencyhomepage.com",
                            "source_id": agency_id,
                        },
                    ],
                    "jurisdictions": {"excluded": [], "included": []},
                    "is_subscribed_to_emails": True,
                    "days_after_time_period_to_send_email": 12,
                },
            )

    def test_update_jurisdictions(self) -> None:
        agency = self.test_schema_objects.test_agency_A
        user = self.test_schema_objects.test_user_A
        self.session.add_all(
            [
                agency,
                user,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_A,
                    role=schema.UserAccountRole.AGENCY_ADMIN,
                    subscribed=True,
                ),
            ]
        )
        self.session.commit()
        self.session.refresh(agency)
        agency_id = agency.id

        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=user.auth0_user_id,
            )

            # add included jurisdictions
            included_ids = ["0100000000", "0103100000", "0104700000"]
            excluded_ids: List[str] = []
            patch_response = self.client.patch(
                f"/api/agencies/{agency_id}",
                json={
                    "jurisdictions": {
                        "included": included_ids,
                        "excluded": excluded_ids,
                    }
                },
            )
            self.assertEqual(patch_response.status_code, 200)
            get_response = self.client.get(
                f"/api/agencies/{agency_id}",
            )
            self.assertEqual(get_response.status_code, 200)
            jurisdictions = get_response.json
            self.assertEqual(
                {
                    "days_after_time_period_to_send_email": 15,
                    "is_subscribed_to_emails": True,
                    "jurisdictions": {
                        "included": ["0100000000", "0103100000", "0104700000"],
                        "excluded": [],
                    },
                    "settings": [],
                },
                jurisdictions,
            )

            # add excluded jurisdictions
            excluded_ids = ["0105500000", "0105900000"]
            patch_response = self.client.patch(
                f"/api/agencies/{agency_id}",
                json={
                    "jurisdictions": {
                        "included": included_ids,
                        "excluded": excluded_ids,
                    }
                },
            )
            self.assertEqual(patch_response.status_code, 200)
            get_response = self.client.get(
                f"/api/agencies/{agency_id}",
            )
            self.assertEqual(get_response.status_code, 200)
            jurisdictions = get_response.json
            self.assertEqual(
                {
                    "days_after_time_period_to_send_email": 15,
                    "is_subscribed_to_emails": True,
                    "jurisdictions": {
                        "included": ["0100000000", "0103100000", "0104700000"],
                        "excluded": ["0105500000", "0105900000"],
                    },
                    "settings": [],
                },
                jurisdictions,
            )

            # remove included jurisdictions
            included_ids = []
            patch_response = self.client.patch(
                f"/api/agencies/{agency_id}",
                json={
                    "jurisdictions": {
                        "included": included_ids,
                        "excluded": excluded_ids,
                    }
                },
            )
            self.assertEqual(patch_response.status_code, 200)
            get_response = self.client.get(
                f"/api/agencies/{agency_id}",
            )
            self.assertEqual(get_response.status_code, 200)
            jurisdictions = get_response.json
            self.assertEqual(
                {
                    "days_after_time_period_to_send_email": 15,
                    "is_subscribed_to_emails": True,
                    "jurisdictions": {
                        "included": [],
                        "excluded": ["0105500000", "0105900000"],
                    },
                    "settings": [],
                },
                jurisdictions,
            )

            # remove excluded jurisdictions
            excluded_ids = []
            patch_response = self.client.patch(
                f"/api/agencies/{agency_id}",
                json={
                    "jurisdictions": {
                        "included": included_ids,
                        "excluded": excluded_ids,
                    }
                },
            )
            self.assertEqual(patch_response.status_code, 200)
            get_response = self.client.get(
                f"/api/agencies/{agency_id}",
            )
            self.assertEqual(get_response.status_code, 200)
            jurisdictions = get_response.json
            self.assertEqual(
                {
                    "days_after_time_period_to_send_email": 15,
                    "is_subscribed_to_emails": True,
                    "jurisdictions": {
                        "included": [],
                        "excluded": [],
                    },
                    "settings": [],
                },
                jurisdictions,
            )

    def test_get_subscribed_user_emails_by_agency_id(self) -> None:
        agency = self.test_schema_objects.test_agency_A
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_user_B,
                self.test_schema_objects.test_user_C,
                agency,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_A,
                    subscribed=True,
                ),
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_B,
                    agency=self.test_schema_objects.test_agency_A,
                    subscribed=False,
                ),
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_C,
                    agency=self.test_schema_objects.test_agency_A,
                    subscribed=None,
                ),
            ]
        )
        self.session.commit()

        self.test_schema_objects.test_user_A.email = "testA@email.com"
        self.test_schema_objects.test_user_B.email = "testB@email.com"
        self.test_schema_objects.test_user_C.email = "testC@email.com"

        subscribed_user_emails = AgencyUserAccountAssociationInterface.get_subscribed_user_emails_by_agency_id(
            session=self.session,
            agency_id=agency.id,
        )

        subscribed_users = AgencyUserAccountAssociationInterface.get_subscribed_user_associations_by_agency_id(
            session=self.session, agency_id=agency.id
        )

        self.assertEqual(subscribed_user_emails, ["testA@email.com"])
        self.assertEqual(len(subscribed_users), 1)
        self.assertEqual(subscribed_users[0].user_account.email, "testA@email.com")

    def test_update_user_agency_visit(self) -> None:
        user = self.test_schema_objects.test_user_A
        agency = self.test_schema_objects.test_agency_A
        self.session.add_all(
            [
                agency,
                user,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_A,
                    role=schema.UserAccountRole.AGENCY_ADMIN,
                ),
            ]
        )
        self.session.commit()
        user_id = user.id
        agency_id = agency.id

        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=user.auth0_user_id,
            )

            response = self.client.put(
                f"/api/{user_id}/{agency_id}/page_visit",
            )
        self.assertEqual(response.status_code, 200)

        # Get the user's last visit to the agency
        association = AgencyUserAccountAssociationInterface.get_associations_by_ids(
            user_account_ids=[user_id],
            agency_id=agency_id,
            session=self.session,
        )[0]

        self.assertEqual(
            association.last_visit, datetime.datetime.now(tz=datetime.timezone.utc)
        )

    def test_update_user_subscribed(self) -> None:
        user = self.test_schema_objects.test_user_A
        agency = self.test_schema_objects.test_agency_A
        self.session.add_all(
            [
                agency,
                user,
                schema.AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_agency_A,
                    role=schema.UserAccountRole.AGENCY_ADMIN,
                    subscribed=False,
                ),
            ]
        )
        self.session.commit()
        user_id = user.id
        agency_id = agency.id

        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=user.auth0_user_id,
            )

            response = self.client.put(
                f"/api/agency/{agency_id}/subscription/{user_id}",
                json={
                    "is_subscribed": True,
                    "days_after_time_period_to_send_email": 15,
                },
            )
        self.assertEqual(response.status_code, 200)

        # Get the user's last visit to the agency
        association = AgencyUserAccountAssociationInterface.get_associations_by_ids(
            user_account_ids=[user_id],
            agency_id=agency_id,
            session=self.session,
        )[0]

        self.assertEqual(association.subscribed, True)
        self.assertEqual(association.days_after_time_period_to_send_email, 15)

        response = self.client.put(
            f"/api/agency/{agency_id}/subscription/{user_id}",
            json={
                "is_subscribed": True,
                "days_after_time_period_to_send_email": -15,
            },
        )

        # We do not accept negative numbers for days_after_time_period_to_send_email
        self.assertEqual(response.status_code, 500)

    def test_upload_superagency_spreadsheet(self) -> None:
        self.session.add_all(
            [
                self.test_schema_objects.test_prison_super_agency,
                self.test_schema_objects.test_prison_affiliate_A,
                AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_prison_super_agency,
                ),
                AgencyUserAccountAssociation(
                    user_account=self.test_schema_objects.test_user_A,
                    agency=self.test_schema_objects.test_prison_affiliate_A,
                ),
            ]
        )
        self.session.commit()
        super_agency = self.test_schema_objects.test_prison_super_agency
        child_agency = self.test_schema_objects.test_prison_affiliate_A
        child_agency.super_agency_id = super_agency.id

        # Set superagency to have the following metric config
        # - funding reported annually
        # - total staff reported monthly
        # - grievances upheld reported annually
        super_funding_datapoint = schema.Datapoint(
            metric_definition_key=prisons.funding.key,
            source_id=super_agency.id,
            context_key=REPORTING_FREQUENCY_CONTEXT_KEY,
            value=CustomReportingFrequency(
                frequency=schema.ReportingFrequency.ANNUAL
            ).to_json_str(),
            is_report_datapoint=False,
        )
        super_staff_datapoint = schema.Datapoint(
            metric_definition_key=prisons.staff.key,
            source_id=super_agency.id,
            context_key=REPORTING_FREQUENCY_CONTEXT_KEY,
            value=CustomReportingFrequency(
                frequency=schema.ReportingFrequency.MONTHLY
            ).to_json_str(),
            is_report_datapoint=False,
        )
        super_grievances_datapoint = schema.Datapoint(
            metric_definition_key=prisons.grievances_upheld.key,
            source_id=super_agency.id,
            context_key=REPORTING_FREQUENCY_CONTEXT_KEY,
            value=CustomReportingFrequency(
                frequency=schema.ReportingFrequency.ANNUAL
            ).to_json_str(),
            is_report_datapoint=False,
        )
        self.session.add_all(
            [super_funding_datapoint, super_staff_datapoint, super_grievances_datapoint]
        )

        # Set child agency to have the following metric config (different from super agency)
        # - funding reported monthly
        # - total staff reported annually
        # - grievances upheld reported monthly
        child_funding_datapoint = schema.Datapoint(
            metric_definition_key=prisons.funding.key,
            source_id=child_agency.id,
            context_key=REPORTING_FREQUENCY_CONTEXT_KEY,
            value=CustomReportingFrequency(
                frequency=schema.ReportingFrequency.MONTHLY
            ).to_json_str(),
            is_report_datapoint=False,
        )
        child_staff_datapoint = schema.Datapoint(
            metric_definition_key=prisons.staff.key,
            source_id=child_agency.id,
            context_key=REPORTING_FREQUENCY_CONTEXT_KEY,
            value=CustomReportingFrequency(
                frequency=schema.ReportingFrequency.ANNUAL
            ).to_json_str(),
            is_report_datapoint=False,
        )
        child_grievances_datapoint = schema.Datapoint(
            metric_definition_key=prisons.grievances_upheld.key,
            source_id=child_agency.id,
            context_key=REPORTING_FREQUENCY_CONTEXT_KEY,
            value=CustomReportingFrequency(
                frequency=schema.ReportingFrequency.MONTHLY
            ).to_json_str(),
            is_report_datapoint=False,
        )
        self.session.add_all(
            [child_funding_datapoint, child_staff_datapoint, child_grievances_datapoint]
        )
        self.session.commit()

        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            )

            file_path = create_excel_file(
                system=schema.System.PRISONS,
                file_name="test_super_child_upload.xlsx",
                child_agencies=[child_agency],
                custom_frequency_dict={
                    "funding": schema.ReportingFrequency.MONTHLY,
                    "funding_by_type": schema.ReportingFrequency.MONTHLY,
                    "staff": schema.ReportingFrequency.ANNUAL,
                    "staff_by_type": schema.ReportingFrequency.ANNUAL,
                    "grievances_upheld": schema.ReportingFrequency.MONTHLY,
                    "grievances_upheld_by_type": schema.ReportingFrequency.MONTHLY,
                },
                sheet_names_to_skip={
                    "expenses",
                    "expenses_by_type",
                    "readmissions",
                    "readmissions_by_type",
                    "admissions",
                    "admissions_by_type",
                    "population",
                    "population_by_type",
                    "population_by_race",
                    "population_by_biological_sex",
                    "releases",
                    "releases_by_type",
                    "use_of_force",
                },
            )

            # Upload file via superagency for child agency
            # File contains monthly funding data, annual total staff data, and monthly grievances upheld data
            # Should not result in any errors/warnings since child agency has those metrics configured accordingly
            with open(Path(file_path), "rb") as file:
                response = self.client.post(
                    "/api/spreadsheets",
                    data={
                        "agency_id": super_agency.id,
                        "system": System.PRISONS.value,
                        "file": file,
                    },
                )
            self.assertEqual(response.status_code, 200)
            response_dict = assert_type(response.json, dict)
            funding_errors = response_dict["metrics"][0]["metric_errors"]
            self.assertEqual(len(funding_errors), 0)
            staff_errors = response_dict["metrics"][2]["metric_errors"]
            self.assertEqual(len(staff_errors), 0)
            grievances_errors = response_dict["metrics"][8]["metric_errors"]
            self.assertEqual(len(grievances_errors), 0)

    def test_get_child_agencies_for_superagency(self) -> None:
        user = self.test_schema_objects.test_user_A
        super_agency = self.test_schema_objects.test_prison_super_agency
        child_agency = self.test_schema_objects.test_prison_affiliate_A
        self.session.add_all(
            [
                user,
                super_agency,
                child_agency,
                schema.AgencyUserAccountAssociation(
                    user_account=user, agency=super_agency
                ),
            ]
        )
        self.session.commit()
        self.session.refresh(super_agency)
        child_agency.super_agency_id = super_agency.id
        self.session.commit()

        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user.auth0_user_id)
            response = self.client.get(f"/api/agencies/{super_agency.id}/children")

        response_json = assert_type(response.json, dict)
        self.assertEqual(len(response_json["child_agencies"]), 1)
        self.assertEqual(response_json["child_agencies"][0]["name"], child_agency.name)


def test_frozen_now_is_not_global_now() -> None:
    """Tests that the use of @freeze_time(NOW_TIME) is local to the wrapped tests."""
    assert datetime.date.today() > NOW_TIME.date()
