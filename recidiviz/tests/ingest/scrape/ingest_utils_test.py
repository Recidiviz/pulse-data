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

"""Tests for ingest/ingest_utils.py."""
from typing import List

from mock import Mock, PropertyMock, patch

from recidiviz.ingest.scrape import constants, ingest_utils


def fake_modules(*names: str) -> List[Mock]:
    modules = []
    for name in names:
        fake_module = Mock()
        type(fake_module).name = PropertyMock(return_value=name)
        modules.append(fake_module)
    return modules


class TestIngestUtils:
    """Tests for regions.py."""

    @patch(
        "pkgutil.iter_modules",
        return_value=fake_modules("us_ny", "us_pa", "us_vt", "us_pa_greene"),
    )
    def test_validate_regions_one_ok(self, _mock_modules: Mock) -> None:
        assert ingest_utils.validate_regions(["us_ny"]) == {"us_ny"}

    @patch(
        "pkgutil.iter_modules",
        return_value=fake_modules("us_ny", "us_pa", "us_vt", "us_pa_greene"),
    )
    def test_validate_regions_one_all(self, _mock_modules: Mock) -> None:
        assert ingest_utils.validate_regions(["all"]) == {
            "us_ny",
            "us_pa",
            "us_vt",
            "us_pa_greene",
        }

    @patch(
        "pkgutil.iter_modules",
        return_value=fake_modules("us_ny", "us_pa", "us_vt", "us_pa_greene"),
    )
    def test_validate_regions_one_invalid(self, _mock_modules: Mock) -> None:
        assert not ingest_utils.validate_regions(["ca_bc"])

    @patch(
        "pkgutil.iter_modules",
        return_value=fake_modules("us_ny", "us_pa", "us_vt", "us_pa_greene"),
    )
    def test_validate_regions_multiple_ok(self, _mock_modules: Mock) -> None:
        assert ingest_utils.validate_regions(["us_pa", "us_ny"]) == {"us_pa", "us_ny"}

    @patch(
        "pkgutil.iter_modules",
        return_value=fake_modules("us_ny", "us_pa", "us_vt", "us_pa_greene"),
    )
    def test_validate_regions_multiple_invalid(self, _mock_modules: Mock) -> None:
        assert not ingest_utils.validate_regions(["us_pa", "invalid"])

    @patch(
        "pkgutil.iter_modules",
        return_value=fake_modules("us_ny", "us_pa", "us_vt", "us_pa_greene"),
    )
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_validate_regions_multiple_all(
        self, mock_region: Mock, mock_env: Mock, _mock_modules: Mock
    ) -> None:
        fake_region = Mock()
        mock_region.return_value = fake_region
        fake_region.environment = "production"
        mock_env.return_value = "production"

        assert ingest_utils.validate_regions(["us_pa", "all"]) == {
            "us_ny",
            "us_pa",
            "us_vt",
            "us_pa_greene",
        }

    @patch(
        "pkgutil.iter_modules",
        return_value=fake_modules("us_a", "us_b", "us_c", "us_d"),
    )
    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_validate_regions_environments(
        self, mock_region: Mock, mock_env: Mock, _mock_modules: Mock
    ) -> None:
        region_prod, region_staging, region_none = Mock(), Mock(), Mock()
        region_prod.environment = "production"
        region_staging.environment = "staging"
        region_none.environment = False

        mock_region.side_effect = [
            region_prod,
            region_none,
            region_prod,
            region_staging,
        ]
        mock_env.return_value = "production"

        result = ingest_utils.validate_regions(["all"])
        assert isinstance(result, set)
        assert len(result) == 2

    @patch(
        "pkgutil.iter_modules",
        return_value=fake_modules("us_ny", "us_pa", "us_vt", "us_pa_greene"),
    )
    def test_validate_regions_multiple_all_invalid(self, _mock_modules: Mock) -> None:
        assert not ingest_utils.validate_regions(["all", "invalid"])

    @patch(
        "pkgutil.iter_modules",
        return_value=fake_modules("us_ny", "us_pa", "us_vt", "us_pa_greene"),
    )
    def test_validate_regions_empty(self, _mock_modules: Mock) -> None:
        assert ingest_utils.validate_regions([]) == set()

    def test_validate_scrape_types_one_ok(self) -> None:
        assert ingest_utils.validate_scrape_types(
            [constants.ScrapeType.SNAPSHOT.value]
        ) == [constants.ScrapeType.SNAPSHOT]

    def test_validate_scrape_types_one_all(self) -> None:
        assert ingest_utils.validate_scrape_types(["all"]) == [
            constants.ScrapeType.BACKGROUND,
            constants.ScrapeType.SNAPSHOT,
        ]

    def test_validate_scrape_types_one_invalid(self) -> None:
        assert not ingest_utils.validate_scrape_types(["When You Were Young"])

    def test_validate_scrape_types_multiple_ok(self) -> None:
        assert ingest_utils.validate_scrape_types(
            [constants.ScrapeType.BACKGROUND.value, constants.ScrapeType.SNAPSHOT.value]
        ) == [constants.ScrapeType.BACKGROUND, constants.ScrapeType.SNAPSHOT]

    def test_validate_scrape_types_multiple_invalid(self) -> None:
        assert not ingest_utils.validate_scrape_types(
            [constants.ScrapeType.BACKGROUND.value, "invalid"]
        )

    def test_validate_scrape_types_multiple_all(self) -> None:
        assert ingest_utils.validate_scrape_types(
            [constants.ScrapeType.BACKGROUND.value, "all"]
        ) == [constants.ScrapeType.BACKGROUND, constants.ScrapeType.SNAPSHOT]

    def test_validate_scrape_types_multiple_all_invalid(self) -> None:
        assert not ingest_utils.validate_scrape_types(["all", "invalid"])

    def test_validate_scrape_types_empty(self) -> None:
        assert ingest_utils.validate_scrape_types([]) == [
            constants.ScrapeType.BACKGROUND
        ]
