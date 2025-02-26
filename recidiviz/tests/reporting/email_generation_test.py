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
from abc import abstractmethod
from typing import List, Type
from unittest import TestCase
from unittest.mock import patch

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.outliers.constants import INCARCERATION_STARTS_TECHNICAL_VIOLATION
from recidiviz.outliers.types import OfficerSupervisorReportData, OutliersConfig
from recidiviz.reporting.constants import ReportType
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.constants import (
    ADDITIONAL_EMAIL_ADDRESSES_KEY,
    SUBJECT_LINE_KEY,
)
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.context import (
    OutliersSupervisionOfficerSupervisorContext,
)
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.fixtures import (
    metric_fixtures,
)
from recidiviz.reporting.context.report_context import ReportContext
from recidiviz.reporting.email_generation import generate
from recidiviz.reporting.email_reporting_utils import Batch
from recidiviz.reporting.recipient import Recipient


class EmailGenerationTests(TestCase):
    """Tests for reporting/email_generation.py."""

    # We set __test__ to False to tell `pytest` not to collect this class for running tests
    # (as successful runs rely on the implementation of an abstract method).
    # In sub-classes, __test__ should be re-set to True.
    __test__ = False

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.get_secret_patcher = patch("recidiviz.utils.secrets.get_secret")
        self.gcs_file_system_patcher = patch(
            "recidiviz.reporting.email_generation.GcsfsFactory.build"
        )
        test_secrets = {"po_report_cdn_static_IP": "123.456.7.8"}
        self.get_secret_patcher.start().side_effect = test_secrets.get
        self.project_id_patcher.start().return_value = "recidiviz-test"
        self.gcs_file_system = FakeGCSFileSystem()
        self.mock_gcs_file_system = self.gcs_file_system_patcher.start()
        self.mock_gcs_file_system.return_value = self.gcs_file_system
        self.mock_batch_id = "20211029135032"

    def tearDown(self) -> None:
        self.get_secret_patcher.stop()
        self.project_id_patcher.stop()
        self.gcs_file_system_patcher.stop()

    @property
    @abstractmethod
    def report_context_type(self) -> Type[ReportContext]:
        raise NotImplementedError

    @property
    @abstractmethod
    def report_type(self) -> ReportType:
        raise NotImplementedError


class OutliersSupervisionOfficerSupervisorGenerationTest(EmailGenerationTests):
    """Tests specific to the Outliers Supervision Officer Supervisor report"""

    __test__ = True

    def get_batch_for_state(self, state_code: StateCode) -> Batch:
        return Batch(
            state_code=state_code,
            batch_id=self.mock_batch_id,
            report_type=self.report_type,
        )

    @property
    def config(self) -> OutliersConfig:
        return OutliersConfig(
            metrics=[
                metric_fixtures[INCARCERATION_STARTS_TECHNICAL_VIOLATION],
            ],
            supervision_officer_label="officer",
            learn_more_url="https://recidiviz.org",
        )

    @property
    def report(self) -> OfficerSupervisorReportData:
        return OfficerSupervisorReportData(
            metrics=[],
            metrics_without_outliers=[],
            recipient_email_address="recidiviz@recidiviz.org",
            additional_recipients=["additional@recidiviz.org"],
        )

    @property
    def report_context_type(self) -> Type[ReportContext]:
        return OutliersSupervisionOfficerSupervisorContext

    @property
    def supported_states(self) -> List[StateCode]:
        return [StateCode.US_IX, StateCode.US_PA]

    @property
    def report_type(self) -> ReportType:
        return ReportType.OutliersSupervisionOfficerSupervisor

    def test_generate(self) -> None:
        """Test that the prepared html is added to Google Cloud Storage with the correct bucket name, filepath,
        and prepared html template for the report context."""

        for state_code in self.supported_states:
            batch = self.get_batch_for_state(state_code)

            recipient = Recipient(
                email_address=self.report.recipient_email_address,
                state_code=state_code,
                data={
                    "report": self.report,
                    "config": self.config,
                    "review_month": 6,
                    "review_year": 2023,
                },
            )

            report_context = self.report_context_type(batch, recipient)

            prepared_html = report_context.render_html()
            generate(batch, recipient, report_context)

            bucket_name = "recidiviz-test-report-html"
            bucket_filepath = f"{state_code.value}/{self.mock_batch_id}/html/{recipient.email_address}.html"
            path = GcsfsFilePath.from_absolute_path(
                f"gs://{bucket_name}/{bucket_filepath}"
            )
            self.assertEqual(
                self.gcs_file_system.download_as_string(path), prepared_html
            )

            self.assertEqual(
                self.gcs_file_system.get_metadata(path),
                {SUBJECT_LINE_KEY: "Your October Unit Alert"},
            )

    def test_generate_with_additional_email_address(self) -> None:
        """
        Test that the metadata with the additional email address is written in GCS
        """
        for state_code in self.supported_states:
            batch = self.get_batch_for_state(state_code)

            recipient = Recipient(
                email_address=self.report.recipient_email_address,
                state_code=state_code,
                data={
                    "report": self.report,
                    "config": self.config,
                    "review_month": 6,
                    "review_year": 2023,
                },
                additional_email_addresses=["additional@recidiviz.org"],
            )

            report_context = self.report_context_type(batch, recipient)

            prepared_html = report_context.render_html()
            generate(batch, recipient, report_context)

            bucket_name = "recidiviz-test-report-html"
            bucket_filepath = f"{state_code.value}/{self.mock_batch_id}/html/{recipient.email_address}.html"
            path = GcsfsFilePath.from_absolute_path(
                f"gs://{bucket_name}/{bucket_filepath}"
            )
            self.assertEqual(
                self.gcs_file_system.download_as_string(path), prepared_html
            )

            self.assertEqual(
                self.gcs_file_system.get_metadata(path),
                {
                    ADDITIONAL_EMAIL_ADDRESSES_KEY: '["additional@recidiviz.org"]',
                    SUBJECT_LINE_KEY: "Your October Unit Alert",
                },
            )

    def test_generate_incomplete_data(self) -> None:
        """Test that no files are added to Google Cloud Storage and a KeyError is raised
        if the recipient data is missing a key needed for the HTML template."""

        for state_code in self.supported_states:
            with self.assertRaises(KeyError):
                batch = self.get_batch_for_state(state_code)

                recipient = Recipient(
                    email_address=self.report.recipient_email_address,
                    state_code=state_code,
                    data={
                        # Missing config key
                        "report": self.report,
                        "review_month": 6,
                        "review_year": 2023,
                    },
                    additional_email_addresses=["additional@recidiviz.org"],
                )

                report_context = self.report_context_type(batch, recipient)
                generate(batch, recipient, report_context)

            self.assertEqual(self.gcs_file_system.all_paths, [])
