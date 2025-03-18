# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Unit tests for config_utils."""


from unittest import TestCase

from recidiviz.airflow.dags.utils.config_utils import (
    QueuingActionType,
    handle_queueing_result,
)


class TestHandleQueuingResult(TestCase):
    """Unit tests for handle_queueing_result operator"""

    def test_invalid_inputs(self) -> None:
        with self.assertLogs() as logs:
            assert handle_queueing_result.function(action_type=None) is False

        assert len(logs.output) == 1
        assert (
            logs.output[0]
            == "ERROR:root:Found null action_type, indicating that the queueing sensor failed (crashed) failed - do not continue."
        )

        with self.assertLogs() as logs:
            assert handle_queueing_result.function(action_type="DEFER") is False

        assert len(logs.output) == 1
        assert (
            logs.output[0]
            == "ERROR:root:Found unrecognized action_type [DEFER] -- do not continue."
        )

    def test_inputs(self) -> None:
        assert (
            handle_queueing_result.function(action_type=QueuingActionType.CANCEL.value)
            is False
        )

        assert (
            handle_queueing_result.function(
                action_type=QueuingActionType.CONTINUE.value
            )
            is True
        )
