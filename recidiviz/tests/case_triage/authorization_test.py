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
"""Implements tests for the core AuthorizationStore of Case Triage."""
import os
from datetime import date
from pathlib import Path
from typing import Optional
from unittest import mock, TestCase

from freezegun import freeze_time
from parameterized import parameterized

from recidiviz.case_triage.authorization import AuthorizationStore
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath


def _test_get_local_file(file_path: GcsfsFilePath) -> str:
    local_path = os.path.join(
        os.path.realpath(os.path.dirname(os.path.realpath(__file__))), "auth_fixtures"
    )
    return Path(os.path.join(local_path, file_path.abs_path())).read_text()


class TestAuthorizationStore(TestCase):
    """Class to test AuthorizationStore"""

    def setUp(self) -> None:
        self.get_local_patcher = mock.patch(
            "recidiviz.case_triage.authorization.get_local_file",
            new=_test_get_local_file,
        )
        self.get_local_patcher.start()

    def tearDown(self) -> None:
        self.get_local_patcher.stop()

    @parameterized.expand(
        [
            ("non-user@recidiviz.org", False, False, False),
            ("user@recidiviz.org", True, False, False),
            ("demoer@recidiviz.org", True, True, False),
            ("admin@recidiviz.org", True, True, True),
        ]
    )
    def test_basic_auth(
        self, email: str, is_allowed: bool, can_see_demo: bool, can_impersonate: bool
    ) -> None:
        auth_store = AuthorizationStore()
        auth_store.refresh()

        self.assertEqual(email in auth_store.allowed_users, is_allowed)
        self.assertEqual(auth_store.can_see_demo_data(email), can_see_demo)
        self.assertEqual(auth_store.can_impersonate_others(email), can_impersonate)

    @parameterized.expand(
        [
            ("non-experiment@recidiviz.org", None, None),
            ("eventually@recidiviz.org", None, "in-experiment"),
            ("always@recidiviz.org", "in-experiment", "in-experiment"),
            ("second-variant@recidiviz.org", "second-variant", "second-variant"),
        ]
    )
    @freeze_time("2021-01-01 00:00:00")
    def test_feature_gating(
        self, email: str, current_variant: Optional[str], future_variant: Optional[str]
    ) -> None:
        auth_store = AuthorizationStore()
        auth_store.refresh()
        feature = "can-see-test-feature"

        self.assertEqual(
            auth_store.get_feature_variant(feature, email),
            current_variant,
            msg="Incorrect variant returned for current date",
        )
        self.assertEqual(
            auth_store.get_feature_variant(feature, email, on_date=date(2022, 2, 2)),
            future_variant,
            msg="Incorrect variant returned for future date",
        )
