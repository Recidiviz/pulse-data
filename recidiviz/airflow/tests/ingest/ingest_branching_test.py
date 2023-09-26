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
"""
Unit tests to test the ingest branch creation logic.
"""
import unittest
from typing import Callable, Union
from unittest.mock import MagicMock, patch

from airflow.models import BaseOperator
from airflow.utils.task_group import TaskGroup

from recidiviz.airflow.dags.ingest.ingest_branching import (
    create_ingest_branch_map,
    get_ingest_branch_key,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class TestIngestBranching(unittest.TestCase):
    """Tests the Ingest Branching functions."""

    def setUp(self) -> None:
        self.get_all_enabled_state_and_instance_pairs_patcher = patch(
            "recidiviz.airflow.dags.ingest.ingest_branching.get_all_enabled_state_and_instance_pairs",
            return_value=[
                (StateCode.US_XX, DirectIngestInstance.PRIMARY),
                (StateCode.US_XX, DirectIngestInstance.SECONDARY),
                (StateCode.US_YY, DirectIngestInstance.PRIMARY),
                (StateCode.US_YY, DirectIngestInstance.SECONDARY),
            ],
        )
        self.get_all_enabled_state_and_instance_pairs_patcher.start()

    def tearDown(self) -> None:
        self.get_all_enabled_state_and_instance_pairs_patcher.stop()

    def test_ingest_branching_map_created(self) -> None:
        mock_pipeline_function: Callable[
            [StateCode, DirectIngestInstance], Union[BaseOperator, TaskGroup]
        ] = MagicMock()  # type: ignore

        result = create_ingest_branch_map(mock_pipeline_function)

        self.assertEqual(len(result), 4)
        self.assertIn(
            get_ingest_branch_key(
                StateCode.US_XX.value, DirectIngestInstance.PRIMARY.value
            ),
            result,
        )
        self.assertIn(
            get_ingest_branch_key(
                StateCode.US_XX.value, DirectIngestInstance.SECONDARY.value
            ),
            result,
        )
        self.assertIn(
            get_ingest_branch_key(
                StateCode.US_YY.value, DirectIngestInstance.PRIMARY.value
            ),
            result,
        )
        self.assertIn(
            get_ingest_branch_key(
                StateCode.US_YY.value, DirectIngestInstance.SECONDARY.value
            ),
            result,
        )
