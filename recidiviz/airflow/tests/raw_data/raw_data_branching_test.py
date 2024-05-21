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
"""Unit tests for raw data branch creation"""
import unittest
from typing import Callable, Union
from unittest.mock import MagicMock, patch

from airflow.models import BaseOperator
from airflow.utils.task_group import TaskGroup

from recidiviz.airflow.dags.raw_data.raw_data_branching import (
    create_raw_data_branch_map,
    get_raw_data_branch_filter,
    get_raw_data_import_branch_key,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


@patch(
    "recidiviz.airflow.dags.raw_data.raw_data_branching.get_raw_data_dag_enabled_state_and_instance_pairs",
    MagicMock(
        return_value=[
            (StateCode.US_XX, DirectIngestInstance.PRIMARY),
            (StateCode.US_XX, DirectIngestInstance.SECONDARY),
            (StateCode.US_YY, DirectIngestInstance.PRIMARY),
            (StateCode.US_YY, DirectIngestInstance.SECONDARY),
        ]
    ),
)
def test_create_raw_data_branch_map() -> None:
    mock_pipeline_function: Callable[
        [StateCode, DirectIngestInstance], Union[BaseOperator, TaskGroup]
    ] = MagicMock()  # type: ignore

    result = create_raw_data_branch_map(mock_pipeline_function)

    assert len(result) == 4
    assert (
        get_raw_data_import_branch_key(
            StateCode.US_XX.value, DirectIngestInstance.PRIMARY.value
        )
        in result
    )
    assert (
        get_raw_data_import_branch_key(
            StateCode.US_XX.value, DirectIngestInstance.SECONDARY.value
        )
        in result
    )
    assert (
        get_raw_data_import_branch_key(
            StateCode.US_YY.value, DirectIngestInstance.PRIMARY.value
        )
        in result
    )
    assert (
        get_raw_data_import_branch_key(
            StateCode.US_YY.value, DirectIngestInstance.SECONDARY.value
        )
        in result
    )


class RawDataBranchFilterTest(unittest.TestCase):
    """Tests the for raw data branch filters."""

    def setUp(self) -> None:
        self.get_all_enabled_state_and_instance_pairs_patcher = patch(
            "recidiviz.airflow.dags.raw_data.raw_data_branching.get_raw_data_dag_enabled_states",
            return_value=[StateCode.US_XX, StateCode.US_YY],
        )
        self.get_all_enabled_state_and_instance_pairs_patcher.start()
        self.get_ingest_instance = patch(
            "recidiviz.airflow.dags.raw_data.raw_data_branching.get_ingest_instance",
            return_value=None,
        )
        self.get_ingest_instance_mock = self.get_ingest_instance.start()
        self.get_state_code_filter = patch(
            "recidiviz.airflow.dags.raw_data.raw_data_branching.get_state_code_filter",
            return_value=None,
        )
        self.get_state_code_filter_mock = self.get_state_code_filter.start()

    def tearDown(self) -> None:
        self.get_all_enabled_state_and_instance_pairs_patcher.stop()
        self.get_state_code_filter.stop()
        self.get_ingest_instance.stop()

    def test_get_raw_data_branch_filter_no_params(self) -> None:
        result = get_raw_data_branch_filter(MagicMock())
        assert result is None

    def test_get_raw_data_branch_filter_ingest_instance(self) -> None:
        self.get_ingest_instance_mock.return_value = DirectIngestInstance.PRIMARY.value
        result = get_raw_data_branch_filter(MagicMock())
        assert result is not None
        assert len(result) == 2
        assert (
            get_raw_data_import_branch_key(
                StateCode.US_XX.value, DirectIngestInstance.PRIMARY.value
            )
            in result
        )
        assert (
            get_raw_data_import_branch_key(
                StateCode.US_YY.value, DirectIngestInstance.PRIMARY.value
            )
            in result
        )

    def test_get_raw_data_branch_filter_ingest_instance_and_state_code(self) -> None:
        self.get_state_code_filter_mock.return_value = StateCode.US_XX.value
        self.get_ingest_instance_mock.return_value = (
            DirectIngestInstance.SECONDARY.value
        )
        result = get_raw_data_branch_filter(MagicMock())
        assert result is not None
        assert len(result) == 1
        assert (
            get_raw_data_import_branch_key(
                StateCode.US_XX.value, DirectIngestInstance.SECONDARY.value
            )
            in result
        )

    def test_get_raw_data_branch_filter_state_code(self) -> None:
        self.get_state_code_filter_mock.return_value = StateCode.US_XX.value
        with self.assertRaisesRegex(
            ValueError, r"Cannot build branch filter with only a state code"
        ):
            _ = get_raw_data_branch_filter(MagicMock())
