# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for utils/regions.py."""
from contextlib import contextmanager
from typing import IO, Any, Callable, Iterator, List
from unittest import TestCase
from unittest.mock import MagicMock, patch

from mock import Mock, PropertyMock, mock_open

from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct import regions as direct_ingest_regions_module
from recidiviz.tests.utils.fake_region import fake_region

BAD_ENV_BOOL_MANIFEST_CONTENTS = """
    agency_name: Corrections
    environment: true
    """
BAD_ENV_STR_MANIFEST_CONTENTS = """
    agency_name: Corrections
    environment: unknown
    """

REGION_TO_MANIFEST = {
    "bad_env_bool": BAD_ENV_BOOL_MANIFEST_CONTENTS,
    "bad_env_str": BAD_ENV_STR_MANIFEST_CONTENTS,
}


def fake_modules(*names: str) -> List[MagicMock]:
    modules = []
    for name in names:
        fake_module = Mock()
        type(fake_module).name = PropertyMock(return_value=name)
        modules.append(fake_module)
    return modules


class TestRegions(TestCase):
    """Tests for regions.py."""

    def setup_method(self, _test_method: Callable) -> None:
        direct_ingest_regions.REGIONS = {}

    def teardown_method(self, _test_method: Callable) -> None:
        direct_ingest_regions.REGIONS = {}

    def test_get_region_manifest(self) -> None:
        manifest = with_manifest(
            direct_ingest_regions.get_direct_ingest_region_manifest,
            "us_nd",
            direct_ingest_regions_module,
        )
        assert manifest == {
            "agency_name": "North Dakota Department of Corrections and Rehabilitation",
            "environment": "production",
        }

    def test_get_region_manifest_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            with_manifest(
                direct_ingest_regions.get_direct_ingest_region_manifest,
                "us_ab",
                direct_ingest_regions_module,
            )

    def test_invalid_region_error_bool(self) -> None:
        with self.assertRaisesRegex(ValueError, "environment"):
            with_manifest(
                direct_ingest_regions.get_direct_ingest_region, "bad_env_bool"
            )

    def test_invalid_region_error_str(self) -> None:
        with self.assertRaisesRegex(ValueError, "environment"):
            with_manifest(direct_ingest_regions.get_direct_ingest_region, "bad_env_str")

    @patch("recidiviz.utils.environment.get_gcp_environment")
    def test_is_ingest_launched_in_env_production(
        self, mock_environment: MagicMock
    ) -> None:
        mock_environment.return_value = "production"

        region = fake_region()
        self.assertFalse(region.is_ingest_launched_in_env())

        region = fake_region(
            environment="staging",
        )
        self.assertFalse(region.is_ingest_launched_in_env())

        region = fake_region(
            environment="production",
        )
        self.assertTrue(region.is_ingest_launched_in_env())

    @patch("recidiviz.utils.environment.get_gcp_environment")
    def test_test_is_ingest_launched_in_env_staging(
        self, mock_environment: MagicMock
    ) -> None:
        mock_environment.return_value = "staging"

        region = fake_region()
        self.assertTrue(region.is_ingest_launched_in_env())

        region = fake_region(
            environment="staging",
        )
        self.assertTrue(region.is_ingest_launched_in_env())

        region = fake_region(
            environment="production",
        )
        self.assertTrue(region.is_ingest_launched_in_env())


@contextmanager
def mock_manifest_open(filename: str, *args: Any, **kwargs: Any) -> Iterator[IO]:
    if filename.endswith("manifest.yaml"):
        region = filename.split("/")[-2]
        if region in REGION_TO_MANIFEST:
            content = REGION_TO_MANIFEST[region]
            file_object = mock_open(read_data=content).return_value
            file_object.__iter__.return_value = content.splitlines(True)
            yield file_object
            return
    if "encoding" not in kwargs:
        kwargs["encoding"] = "utf-8"
    yield open(filename, *args, **kwargs)  # pylint: disable=W1514


def with_manifest(func: Callable, *args: Any, **kwargs: Any) -> Any:
    with patch(
        "recidiviz.ingest.direct.direct_ingest_regions.open", new=mock_manifest_open
    ):
        return func(*args, **kwargs)
