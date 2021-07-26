# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Tests for reporting/email_generation.py."""
import json
import os
from datetime import date
from typing import Optional
from unittest import TestCase
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from recidiviz.case_triage.opportunities.types import OpportunityType
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.reporting.context.po_monthly_report.constants import ReportType
from recidiviz.reporting.data_retrieval import (
    _get_mismatch_data_for_officer,
    filter_recipients,
    retrieve_data,
    start,
)
from recidiviz.reporting.recipient import Recipient
from recidiviz.reporting.region_codes import REGION_CODES, InvalidRegionCodeException
from recidiviz.tests.case_triage.case_triage_helpers import (
    generate_fake_client,
    generate_fake_etl_opportunity,
    generate_fake_officer,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tools.postgres import local_postgres_helpers

FIXTURE_FILE = "po_monthly_report_data_fixture.json"


def build_report_json_fixture(email_address: str) -> str:
    return json.dumps(
        {
            "email_address": email_address,
            "state_code": "US_ID",
            "district": "DISTRICT OFFICE 3, CALDWELL",
        }
    )


@pytest.mark.uses_db
class EmailGenerationTests(TestCase):
    """Tests for reporting/email_generation.py."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.email_generation_patcher = patch(
            "recidiviz.reporting.email_generation.generate"
        )
        self.gcs_file_system_patcher = patch(
            "recidiviz.cloud_storage.gcsfs_factory.GcsfsFactory.build"
        )
        self.project_id_patcher.start().return_value = "recidiviz-test"
        self.mock_email_generation = self.email_generation_patcher.start()
        self.gcs_file_system = FakeGCSFileSystem()
        self.mock_gcs_file_system = self.gcs_file_system_patcher.start()
        self.mock_gcs_file_system.return_value = self.gcs_file_system

        self.get_secret_patcher = patch("recidiviz.utils.secrets.get_secret")
        self.get_secret_patcher.start()

        self.state_code = StateCode.US_ID

        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

        self.officer = generate_fake_officer("officer_id_1", "officer1@recidiviz.org")
        self.client_downgradable_high = generate_fake_client(
            client_id="client_1",
            supervising_officer_id=self.officer.external_id,
            supervision_level=StateSupervisionLevel.HIGH,
            last_assessment_date=date(2021, 1, 2),
            assessment_score=1,
        )
        self.client_downgradable_medium_1 = generate_fake_client(
            client_id="client_2",
            supervising_officer_id=self.officer.external_id,
            supervision_level=StateSupervisionLevel.MEDIUM,
            last_assessment_date=date(2021, 1, 2),
            assessment_score=1,
        )
        self.client_downgradable_medium_2 = generate_fake_client(
            client_id="client_3",
            supervising_officer_id=self.officer.external_id,
            supervision_level=StateSupervisionLevel.MEDIUM,
            last_assessment_date=date(2021, 1, 2),
            assessment_score=1,
        )
        self.client_no_downgrade = generate_fake_client(
            client_id="client_4",
            supervising_officer_id=self.officer.external_id,
            supervision_level=StateSupervisionLevel.HIGH,
            last_assessment_date=date(2021, 1, 2),
            assessment_score=100,
        )
        self.opportunities = [
            generate_fake_etl_opportunity(
                officer_id=self.officer.external_id,
                person_external_id=client.person_external_id,
                opportunity_type=OpportunityType.OVERDUE_DOWNGRADE,
                opportunity_metadata={
                    "assessmentScore": client.assessment_score,
                    "latestAssessmentDate": str(client.most_recent_assessment_date),
                    "recommendedSupervisionLevel": "MINIMUM",
                },
            )
            for client in [
                self.client_downgradable_high,
                self.client_downgradable_medium_1,
                self.client_downgradable_medium_2,
            ]
        ]

        with SessionFactory.using_database(self.database_key) as session:
            session.expire_on_commit = False
            session.add_all(
                [
                    self.officer,
                    self.client_downgradable_high,
                    self.client_downgradable_medium_1,
                    self.client_downgradable_medium_2,
                    self.client_no_downgrade,
                    *self.opportunities,
                ]
            )

        self.top_opps_email_recipient_patcher = patch(
            "recidiviz.reporting.data_retrieval._top_opps_email_recipient_addresses"
        )
        self.top_opps_email_recipient_patcher.start().return_value = [
            self.officer.email_address
        ]

    def _write_test_data(self, test_data: str) -> None:
        self.gcs_file_system.upload_from_string(
            GcsfsFilePath.from_absolute_path(
                "gs://recidiviz-test-report-data/po_monthly_report/US_ID/po_monthly_report_data.json"
            ),
            test_data,
            "text/json",
        )

    def tearDown(self) -> None:
        self.email_generation_patcher.stop()
        self.project_id_patcher.stop()
        self.gcs_file_system_patcher.stop()
        self.get_secret_patcher.stop()
        self.top_opps_email_recipient_patcher.stop()

        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_start(self) -> None:
        """Test that the prepared html is added to Google Cloud Storage with the correct bucket name, filepath,
        and prepared html template for the report context."""
        with open(
            os.path.join(
                f"{os.path.dirname(__file__)}/context/po_monthly_report", FIXTURE_FILE
            )
        ) as fixture_file:
            # Remove newlines
            self._write_test_data(json.dumps(json.loads(fixture_file.read())))

        self.mock_email_generation.side_effect = ValueError(
            "This email failed to generate!"
        )

        result = start(
            state_code=StateCode.US_ID,
            report_type=ReportType.POMonthlyReport,
            region_code="US_ID_D3",
            test_address="dan@recidiviz.org",
        )

        self.assertListEqual(result.failures, ["dan+letter@recidiviz.org"])
        self.assertEqual(len(result.successes), 0)

        # Email generated for recipient matching US_ID_3
        self.mock_email_generation.assert_called()

        self.mock_email_generation.reset_mock()

        start(
            state_code=StateCode.US_ID,
            report_type=ReportType.POMonthlyReport,
            email_allowlist=["excluded@recidiviz.org"],
        )

        # No recipients to email (none match `email_allowlist`)
        self.mock_email_generation.assert_not_called()

    def test_metadata_added(self) -> None:
        """Tests that the metadata.json file is correctly added."""
        with open(
            os.path.join(
                f"{os.path.dirname(__file__)}/context/po_monthly_report", FIXTURE_FILE
            )
        ) as fixture_file:
            # Remove newlines
            self._write_test_data(json.dumps(json.loads(fixture_file.read())))

        result = start(
            batch_id="fake-batch-id",
            state_code=StateCode.US_ID,
            report_type=ReportType.POMonthlyReport,
            region_code="US_ID_D3",
        )
        self.assertEqual(len(result.successes), 1)

        # Test that metadata file is created correctly
        metadata_file = self.gcs_file_system.download_as_string(
            GcsfsFilePath.from_absolute_path(
                "gs://recidiviz-test-report-html/US_ID/fake-batch-id/metadata.json"
            )
        )
        self.assertEqual(
            json.loads(metadata_file),
            {
                "report_type": ReportType.POMonthlyReport.value,
                "review_month": "5",
                "review_year": "2021",
            },
        )

        # Try again for Top Opps email
        result = start(
            batch_id="fake-batch-id-2",
            state_code=StateCode.US_ID,
            report_type=ReportType.TopOpportunities,
            region_code="US_ID_D3",
        )

        metadata_file = self.gcs_file_system.download_as_string(
            GcsfsFilePath.from_absolute_path(
                "gs://recidiviz-test-report-html/US_ID/fake-batch-id-2/metadata.json"
            )
        )
        self.assertEqual(
            json.loads(metadata_file),
            {"report_type": ReportType.TopOpportunities.value},
        )

    def test_retrieve_data_po_monthly_report(self) -> None:
        batch_id = "123"
        test_data = "\n".join(
            [
                build_report_json_fixture("first@recidiviz.org"),
                "my invalid json",
                build_report_json_fixture("second@recidiviz.org"),
            ]
        )
        self._write_test_data(test_data)

        recipients = retrieve_data(
            state_code=self.state_code,
            report_type=ReportType.POMonthlyReport,
            batch_id=batch_id,
        )

        # Invalid JSON lines are ignored; warnings are logged
        self.assertEqual(len(recipients), 2)
        self.assertEqual(recipients[0].email_address, "first@recidiviz.org")
        self.assertEqual(recipients[1].email_address, "second@recidiviz.org")

        # An archive of report JSON is stored
        self.assertEqual(
            self.gcs_file_system.download_as_string(
                GcsfsFilePath.from_absolute_path(
                    f"gs://recidiviz-test-report-data-archive/{self.state_code.value}/123.json"
                )
            ),
            test_data,
        )

    def test_retrieve_data_top_opps(self) -> None:
        batch_id = "123"
        recipients = retrieve_data(
            state_code=self.state_code,
            report_type=ReportType.TopOpportunities,
            batch_id=batch_id,
        )

        self.assertEqual(len(recipients), 1)
        recipient = recipients[0]

        self.assertEqual(
            recipient.data["mismatches"],
            [
                {
                    "name": "Test Name",
                    "last_score": 1,
                    "last_assessment_date": "2021-01-02",
                    "person_external_id": "client_1",
                    "current_supervision_level": "High",
                    "recommended_level": "Low",
                },
                {
                    "name": "Test Name",
                    "last_score": 1,
                    "last_assessment_date": "2021-01-02",
                    "person_external_id": "client_2",
                    "current_supervision_level": "Moderate",
                    "recommended_level": "Low",
                },
                {
                    "name": "Test Name",
                    "last_score": 1,
                    "last_assessment_date": "2021-01-02",
                    "person_external_id": "client_3",
                    "current_supervision_level": "Moderate",
                    "recommended_level": "Low",
                },
            ],
        )

    def test_retrieve_po_report_mismatch(self) -> None:
        batch_id = "123"
        self._write_test_data(build_report_json_fixture(self.officer.email_address))

        recipients = retrieve_data(
            state_code=self.state_code,
            report_type=ReportType.POMonthlyReport,
            batch_id=batch_id,
        )
        self.assertEqual(len(recipients), 1)
        recipient = recipients[0]

        self.assertEqual(
            recipient.data["mismatches"],
            [
                {
                    "name": "Test Name",
                    "last_score": 1,
                    "last_assessment_date": "2021-01-02",
                    "person_external_id": "client_1",
                    "current_supervision_level": "High",
                    "recommended_level": "Low",
                },
                {
                    "name": "Test Name",
                    "last_score": 1,
                    "last_assessment_date": "2021-01-02",
                    "person_external_id": "client_2",
                    "current_supervision_level": "Moderate",
                    "recommended_level": "Low",
                },
                {
                    "name": "Test Name",
                    "last_score": 1,
                    "last_assessment_date": "2021-01-02",
                    "person_external_id": "client_3",
                    "current_supervision_level": "Moderate",
                    "recommended_level": "Low",
                },
            ],
        )

    def test_filter_recipients(self) -> None:
        dev_from_idaho = Recipient.from_report_json(
            {
                "email_address": "dev@idaho.gov",
                "state_code": "US_ID",
                "district": REGION_CODES["US_ID_D3"],
            }
        )
        dev_from_iowa = Recipient.from_report_json(
            {"email_address": "dev@iowa.gov", "state_code": "US_IA", "district": None}
        )
        recipients = [dev_from_idaho, dev_from_iowa]

        self.assertEqual(filter_recipients(recipients), recipients)
        self.assertEqual(
            filter_recipients(recipients, region_code="US_ID_D3"), [dev_from_idaho]
        )
        self.assertEqual(
            filter_recipients(
                recipients, region_code="US_ID_D3", email_allowlist=["dev@iowa.gov"]
            ),
            [],
        )
        self.assertEqual(
            filter_recipients(recipients, email_allowlist=["dev@iowa.gov"]),
            [dev_from_iowa],
        )
        self.assertEqual(
            filter_recipients(recipients, email_allowlist=["fake@iowa.gov"]), []
        )

        with self.assertRaises(InvalidRegionCodeException):
            filter_recipients(recipients, region_code="gibberish")

    def test_less_than_6_all_mismatches_reported(self) -> None:
        mismatches = _get_mismatch_data_for_officer(self.officer.email_address)
        self.assertCountEqual(
            [mismatch["person_external_id"] for mismatch in mismatches],
            [opp.person_external_id for opp in self.opportunities],
        )

    def test_no_opportunities_reported(self) -> None:
        no_opportunity_officer = generate_fake_officer(
            "no_opportunity_officer", "no-opportunity@recidiviz.org"
        )
        with SessionFactory.using_database(self.database_key) as session:
            session.expire_on_commit = False
            session.add(no_opportunity_officer)

        self.assertEqual(
            [], _get_mismatch_data_for_officer(no_opportunity_officer.email_address)
        )

    @freeze_time("2023-01-02 00:00")
    def test_only_5_mismatches_reported(self) -> None:
        many_opportunities_officer = generate_fake_officer(
            "many_opportunities_officer", "many-opportunities@recidiviz.org"
        )
        many_opportunity_clients = [
            generate_fake_client(
                client_id=f"many_opportunities_client_{i}",
                supervising_officer_id=many_opportunities_officer.external_id,
                supervision_level=StateSupervisionLevel.MEDIUM,
                last_assessment_date=date(2021, 1, i + 1),
                assessment_score=1,
            )
            for i in range(6)
        ]
        opportunities = [
            generate_fake_etl_opportunity(
                officer_id=many_opportunities_officer.external_id,
                person_external_id=client.person_external_id,
                opportunity_type=OpportunityType.OVERDUE_DOWNGRADE,
                opportunity_metadata={
                    "assessmentScore": client.assessment_score,
                    "latestAssessmentDate": str(client.most_recent_assessment_date),
                    "recommendedSupervisionLevel": "MINIMUM",
                },
            )
            for client in many_opportunity_clients
        ]
        with SessionFactory.using_database(self.database_key) as session:
            session.expire_on_commit = False
            session.add_all(
                [many_opportunities_officer, *many_opportunity_clients, *opportunities]
            )

        mismatches = _get_mismatch_data_for_officer(
            many_opportunities_officer.email_address
        )
        self.assertCountEqual(
            [mismatch["person_external_id"] for mismatch in mismatches],
            [client.person_external_id for client in many_opportunity_clients[1:]],
        )

    @freeze_time("2021-01-15 00:00")
    def test_newest_5_mismatches_reported(self) -> None:
        # Since all opportunities are sooner than the cutoff, we take the 5 oldest.
        many_opportunities_officer = generate_fake_officer(
            "many_opportunities_officer", "many-opportunities@recidiviz.org"
        )
        many_opportunity_clients = [
            generate_fake_client(
                client_id=f"many_opportunities_client_{i}",
                supervising_officer_id=many_opportunities_officer.external_id,
                supervision_level=StateSupervisionLevel.MEDIUM,
                last_assessment_date=date(2021, 1, i + 1),
                assessment_score=1,
            )
            for i in range(6)
        ]
        opportunities = [
            generate_fake_etl_opportunity(
                officer_id=many_opportunities_officer.external_id,
                person_external_id=client.person_external_id,
                opportunity_type=OpportunityType.OVERDUE_DOWNGRADE,
                opportunity_metadata={
                    "assessmentScore": client.assessment_score,
                    "latestAssessmentDate": str(client.most_recent_assessment_date),
                    "recommendedSupervisionLevel": "MINIMUM",
                },
            )
            for client in many_opportunity_clients
        ]
        with SessionFactory.using_database(self.database_key) as session:
            session.expire_on_commit = False
            session.add_all(
                [many_opportunities_officer, *many_opportunity_clients, *opportunities]
            )

        mismatches = _get_mismatch_data_for_officer(
            many_opportunities_officer.email_address
        )
        self.assertCountEqual(
            [mismatch["person_external_id"] for mismatch in mismatches],
            [client.person_external_id for client in many_opportunity_clients[:5]],
        )

    @freeze_time("2021-02-15 00:00")
    def test_5_after_threshold_reported(self) -> None:
        many_opportunities_officer = generate_fake_officer(
            "many_opportunities_officer", "many-opportunities@recidiviz.org"
        )
        many_opportunity_clients = [
            generate_fake_client(
                client_id=f"many_opportunities_client_{i}",
                supervising_officer_id=many_opportunities_officer.external_id,
                supervision_level=StateSupervisionLevel.MEDIUM,
                last_assessment_date=date(2021, 1, i + 1),
                assessment_score=1,
            )
            for i in range(31)
        ]
        opportunities = [
            generate_fake_etl_opportunity(
                officer_id=many_opportunities_officer.external_id,
                person_external_id=client.person_external_id,
                opportunity_type=OpportunityType.OVERDUE_DOWNGRADE,
                opportunity_metadata={
                    "assessmentScore": client.assessment_score,
                    "latestAssessmentDate": str(client.most_recent_assessment_date),
                    "recommendedSupervisionLevel": "MINIMUM",
                },
            )
            for client in many_opportunity_clients
        ]
        with SessionFactory.using_database(self.database_key) as session:
            session.expire_on_commit = False
            session.add_all(
                [many_opportunities_officer, *many_opportunity_clients, *opportunities]
            )

        mismatches = _get_mismatch_data_for_officer(
            many_opportunities_officer.email_address
        )
        self.assertCountEqual(
            [mismatch["person_external_id"] for mismatch in mismatches],
            [client.person_external_id for client in many_opportunity_clients[11:16]],
        )
