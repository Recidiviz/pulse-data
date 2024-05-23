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
"""Tests for update_state_dataset.py"""
import unittest
import uuid
from typing import Optional
from unittest import mock
from unittest.mock import Mock, create_autospec

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.success_persister import RefreshBQDatasetSuccessPersister
from recidiviz.entrypoints.ingest import update_state_dataset
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.utils.environment import GCPEnvironment

REFRESH_ENTRYPOINT_PACKAGE_NAME = update_state_dataset.__name__


@mock.patch(
    f"{REFRESH_ENTRYPOINT_PACKAGE_NAME}.BigQueryClientImpl",
    create_autospec(BigQueryClientImpl),
)
@mock.patch("time.sleep", Mock(side_effect=lambda _: None))
@mock.patch(
    "uuid.uuid4", Mock(return_value=uuid.UUID("8367379ff8674b04adfb9b595b277dc3"))
)
class TestExecuteStateDatasetRefresh(unittest.TestCase):
    """Tests for execute_state_dataset_refresh()."""

    def setUp(self) -> None:
        self.mock_combine_patcher = mock.patch(
            f"{REFRESH_ENTRYPOINT_PACKAGE_NAME}.combine_ingest_sources_into_single_state_dataset"
        )
        self.mock_combine = self.mock_combine_patcher.start()

        self.mock_refresh_bq_dataset_persister = create_autospec(
            RefreshBQDatasetSuccessPersister
        )
        self.mock_refresh_bq_dataset_persister_patcher = mock.patch(
            f"{REFRESH_ENTRYPOINT_PACKAGE_NAME}.RefreshBQDatasetSuccessPersister"
        )
        self.mock_refresh_bq_dataset_persister_patcher.start().return_value = (
            self.mock_refresh_bq_dataset_persister
        )
        self.environment_patcher = mock.patch(
            "recidiviz.utils.environment.get_gcp_environment",
            return_value=GCPEnvironment.PRODUCTION.value,
        )
        self.environment_patcher.start()

    def tearDown(self) -> None:
        self.environment_patcher.stop()
        self.mock_combine_patcher.stop()
        self.mock_refresh_bq_dataset_persister_patcher.stop()

    def test_execute_state_dataset_refresh(self) -> None:
        def mock_record_success(
            # pylint: disable=unused-argument
            schema_type: SchemaType,
            direct_ingest_instance: DirectIngestInstance,
            dataset_override_prefix: Optional[str],
            runtime_sec: int,
        ) -> None:
            return None

        self.mock_refresh_bq_dataset_persister.record_success_in_bq.side_effect = (
            mock_record_success
        )

        update_state_dataset.execute_state_dataset_refresh(
            ingest_instance=DirectIngestInstance.PRIMARY,
            sandbox_prefix=None,
        )

        self.mock_combine.assert_called_once()
        self.mock_refresh_bq_dataset_persister.record_success_in_bq.assert_called_once()

    def test_execute_state_dataset_refresh_combine_fails(self) -> None:
        self.mock_combine.side_effect = Exception("Fail")
        with self.assertRaisesRegex(Exception, "Fail"):
            update_state_dataset.execute_state_dataset_refresh(
                ingest_instance=DirectIngestInstance.PRIMARY,
                sandbox_prefix=None,
            )

        self.mock_combine.assert_called_once()
        self.mock_refresh_bq_dataset_persister.record_success_in_bq.assert_not_called()

    def test_execute_state_dataset_refresh_record_success_fails(self) -> None:
        self.mock_refresh_bq_dataset_persister.record_success_in_bq.side_effect = (
            Exception("Fail")
        )
        with self.assertRaisesRegex(Exception, "Fail"):
            update_state_dataset.execute_state_dataset_refresh(
                ingest_instance=DirectIngestInstance.PRIMARY,
                sandbox_prefix=None,
            )

        self.mock_combine.assert_called_once()
        self.mock_refresh_bq_dataset_persister.record_success_in_bq.assert_called_once()

    def test_execute_state_dataset_refresh_secondary_no_sandbox(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "Refresh can only proceed for secondary databases into a sandbox.",
        ):
            update_state_dataset.execute_state_dataset_refresh(
                ingest_instance=DirectIngestInstance.SECONDARY,
                sandbox_prefix=None,
            )

        self.mock_combine.assert_not_called()
        self.mock_refresh_bq_dataset_persister.record_success_in_bq.assert_not_called()
