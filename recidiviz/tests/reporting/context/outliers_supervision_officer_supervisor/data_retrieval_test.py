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
"""Tests for the Outliers supervisor report data retrieval function"""
from unittest import TestCase
from unittest.mock import MagicMock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.constants import (
    ABSCONSIONS_BENCH_WARRANTS,
    INCARCERATION_STARTS,
    INCARCERATION_STARTS_TECHNICAL_VIOLATION,
)
from recidiviz.outliers.types import OfficerSupervisorReportData
from recidiviz.reporting.constants import Batch, ReportType
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.data_retrieval import (
    retrieve_data_for_outliers_supervision_officer_supervisor,
)
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.fixtures import (
    create_fixture,
    get_config_fixture_for_state,
    get_metric_fixtures_for_state,
    highlighted_officers_fixture_adverse,
    other_officers_fixture_adverse,
    target_fixture_adverse,
)
from recidiviz.reporting.data_retrieval import start
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


@patch(
    "recidiviz.reporting.context.outliers_supervision_officer_supervisor.data_retrieval.OutliersQuerier.get_officer_level_report_data_for_all_officer_supervisors"
)
@patch(
    "recidiviz.reporting.context.outliers_supervision_officer_supervisor.data_retrieval.OutliersQuerier.get_product_configuration"
)
class RetrieveDataTest(TestCase):
    """Tests for the Outliers supervisor report data retrieval"""

    def setUp(self) -> None:
        self.gcs_factory_patcher = patch(
            "recidiviz.reporting.email_reporting_handler.GcsfsFactory.build"
        )
        self.fake_gcs = FakeGCSFileSystem()
        self.gcs_factory_patcher.start().return_value = self.fake_gcs

        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-test"

        self.email_generation_patcher = patch(
            "recidiviz.reporting.email_generation.generate"
        )
        self.mock_email_generation = self.email_generation_patcher.start()

    def tearDown(self) -> None:
        self.gcs_factory_patcher.stop()
        self.project_id_patcher.stop()
        self.email_generation_patcher.stop()

    def test_exclude_recipients_without_metrics(
        self, config_mock: MagicMock, query_mock: MagicMock
    ) -> None:
        metric_fixtures = get_metric_fixtures_for_state(StateCode.US_XX)
        report_without_outliers = OfficerSupervisorReportData(
            metrics=[],
            metrics_without_outliers=[
                metric_fixtures[INCARCERATION_STARTS],
                metric_fixtures[INCARCERATION_STARTS_TECHNICAL_VIOLATION],
            ],
            recipient_email_address="test@recidiviz.org",
            additional_recipients=[],
        )
        query_mock.return_value = {"abc": report_without_outliers}

        config_mock.return_value = get_config_fixture_for_state(StateCode.US_XX)

        test_batch = Batch(
            StateCode.US_XX,
            report_type=ReportType.OutliersSupervisionOfficerSupervisor,
            batch_id="20230614123033",
        )

        self.assertEqual(
            retrieve_data_for_outliers_supervision_officer_supervisor(test_batch), {}
        )

    def test_start(self, config_mock: MagicMock, query_mock: MagicMock) -> None:
        """Test that the start() function succeeds for the recipient."""

        metric_fixtures = get_metric_fixtures_for_state(StateCode.US_IX)
        report_with_outliers = OfficerSupervisorReportData(
            metrics=[
                create_fixture(
                    metric_fixtures[INCARCERATION_STARTS_TECHNICAL_VIOLATION],
                    target_fixture_adverse,
                    other_officers_fixture_adverse,
                    highlighted_officers_fixture_adverse,
                )
            ],
            metrics_without_outliers=[
                metric_fixtures[ABSCONSIONS_BENCH_WARRANTS],
                metric_fixtures[INCARCERATION_STARTS],
                metric_fixtures[INCARCERATION_STARTS_TECHNICAL_VIOLATION],
            ],
            recipient_email_address="test@recidiviz.org",
            additional_recipients=[],
        )
        query_mock.return_value = {"abc": report_with_outliers}

        config_mock.return_value = get_config_fixture_for_state(StateCode.US_IX)

        test_batch = Batch(
            StateCode.US_IX,
            report_type=ReportType.OutliersSupervisionOfficerSupervisor,
            batch_id="20230614123033",
        )

        result = start(
            batch=test_batch,
            region_code=None,
            test_address="alexa@recidiviz.org",
        )

        self.assertListEqual(result.successes, ["alexa+test@recidiviz.org"])
        self.assertEqual(len(result.failures), 0)

        self.mock_email_generation.assert_called()
        self.mock_email_generation.reset_mock()

        # No recipients to email (none match `email_allowlist`)
        start(
            batch=test_batch,
            email_allowlist=["excluded@recidiviz.org"],
        )

        self.mock_email_generation.assert_not_called()

        # Message body override is added to Recipient.data field
        result = start(
            batch=test_batch,
            region_code=None,
            message_body_override="override",
        )

        self.assertListEqual(result.successes, ["test@recidiviz.org"])
        self.assertEqual(len(result.failures), 0)

        self.assertEqual(
            "override",
            self.mock_email_generation.call_args.args[1].data["message_body_override"],
        )
        self.mock_email_generation.reset_mock()
