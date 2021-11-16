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
from abc import abstractmethod
from typing import List, Type
from unittest import TestCase
from unittest.mock import patch

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.reporting.context.overdue_discharge_alert.context import (
    OverdueDischargeAlertContext,
)
from recidiviz.reporting.context.po_monthly_report.constants import ReportType
from recidiviz.reporting.context.po_monthly_report.context import PoMonthlyReportContext
from recidiviz.reporting.context.report_context import ReportContext
from recidiviz.reporting.context.top_opportunities.context import (
    TopOpportunitiesReportContext,
)
from recidiviz.reporting.email_generation import generate
from recidiviz.reporting.email_reporting_utils import Batch
from recidiviz.reporting.recipient import Recipient
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


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
        self.mock_batch_id = "1"

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

    @classmethod
    @abstractmethod
    def fixture_file_path(cls) -> str:
        raise NotImplementedError

    @staticmethod
    def fixture_for_report_type(report_type: ReportType) -> str:
        subclasses: List[
            Type[EmailGenerationTests]
        ] = EmailGenerationTests.__subclasses__()

        for subclass in subclasses:
            if subclass().report_type == report_type:
                return subclass.fixture_file_path()

        raise ValueError(f"Could not find subclass for report type {report_type}")

    @property
    @abstractmethod
    def supported_states(self) -> List[StateCode]:
        raise NotImplementedError

    def test_generate(self) -> None:
        """Test that the prepared html is added to Google Cloud Storage with the correct bucket name, filepath,
        and prepared html template for the report context."""
        with open(self.fixture_file_path(), encoding="utf-8") as fixture_file:
            fixture_data = json.loads(fixture_file.read())

        for state_code in self.supported_states:
            recipient = Recipient.from_report_json(
                {
                    **fixture_data,
                    **{"state_code": state_code.value, "batch_id": self.mock_batch_id},
                }
            )

            batch = Batch(
                state_code=state_code,
                batch_id=self.mock_batch_id,
                report_type=self.report_type,
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

    def test_generate_incomplete_data(self) -> None:
        """Test that no files are added to Google Cloud Storage and a KeyError is raised
        if the recipient data is missing a key needed for the HTML template."""

        for state_code in self.supported_states:
            with self.assertRaises(KeyError):
                recipient = Recipient.from_report_json(
                    {
                        "email_address": "letter@kenny.ca",
                        "state_code": state_code.value,
                        "district": "DISTRICT OFFICE 3",
                    }
                )

                batch = Batch(
                    state_code=state_code,
                    batch_id=self.mock_batch_id,
                    report_type=self.report_type,
                )

                report_context = self.report_context_type(batch, recipient)
                generate(batch, recipient, report_context)

            self.assertEqual(self.gcs_file_system.all_paths, [])


class POMonthlyReportGenerationTest(EmailGenerationTests):
    """Tests specific to the PO Monthly report"""

    __test__ = True

    @property
    def report_context_type(self) -> Type[ReportContext]:
        return PoMonthlyReportContext

    @property
    def supported_states(self) -> List[StateCode]:
        return [StateCode.US_ID, StateCode.US_PA]

    @property
    def report_type(self) -> ReportType:
        return ReportType.POMonthlyReport

    @classmethod
    def fixture_file_path(cls) -> str:
        return os.path.join(
            f"{os.path.dirname(__file__)}/context/po_monthly_report/po_monthly_report_data_fixture.json"
        )


class TopOpportunityGenerationTest(EmailGenerationTests):
    """Tests specific to the Top Opps email"""

    __test__ = True

    @property
    def report_context_type(self) -> Type[ReportContext]:
        return TopOpportunitiesReportContext

    @property
    def supported_states(self) -> List[StateCode]:
        return [StateCode.US_ID]

    @property
    def report_type(self) -> ReportType:
        return ReportType.TopOpportunities

    @classmethod
    def fixture_file_path(cls) -> str:
        return os.path.join(
            f"{os.path.dirname(__file__)}/context/top_opportunities/fixtures.json"
        )


class OverdueDischargeAlertGenerationTest(EmailGenerationTests):
    __test__ = True

    @property
    def report_context_type(self) -> Type[ReportContext]:
        return OverdueDischargeAlertContext

    @property
    def report_type(self) -> ReportType:
        return ReportType.OverdueDischargeAlert

    @property
    def supported_states(self) -> List[StateCode]:
        return [StateCode.US_ID]

    @classmethod
    def fixture_file_path(cls) -> str:
        return os.path.join(
            f"{os.path.dirname(__file__)}/context/overdue_discharge_alert/overdue_discharge_alert_data_fixture.json"
        )
