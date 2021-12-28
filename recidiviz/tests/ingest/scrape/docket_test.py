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

"""Tests for ingest/docket.py."""


import json
from typing import Callable, Dict, List

import pytest
from mock import Mock, patch

from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape import constants, docket
from recidiviz.utils import pubsub_helper

REGIONS = ["us_ny", "us_va"]


@pytest.mark.usefixtures("emulator")
@pytest.mark.emulator
class TestDocket:
    """Tests for the methods related to population of items in the docket."""

    def setup_method(self, _test_method: Callable) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "test-project"
        self.scrape_key_write = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)
        self.scrape_key_read = ScrapeKey(REGIONS[1], constants.ScrapeType.BACKGROUND)
        pubsub_helper.create_topic_and_subscription(
            self.scrape_key_write, docket.PUBSUB_TYPE
        )
        pubsub_helper.create_topic_and_subscription(
            self.scrape_key_read, docket.PUBSUB_TYPE
        )

    def teardown_method(self, _test_method: Callable) -> None:
        for region in REGIONS:
            docket.purge_query_docket(
                ScrapeKey(region, constants.ScrapeType.BACKGROUND)
            )
        self.project_id_patcher.stop()

    def test_add_to_query_docket_background(self) -> None:
        docket.add_to_query_docket(self.scrape_key_write, get_payload()[0]).result()
        docket.add_to_query_docket(self.scrape_key_write, get_payload()[1]).result()

        items = [
            docket.get_new_docket_item(self.scrape_key_write),
            docket.get_new_docket_item(self.scrape_key_write),
        ]
        assert len(items) == 2

        for i, item in enumerate(items):
            assert item is not None
            assert item.message.data.decode() == json.dumps(get_payload()[i])

    @patch("recidiviz.utils.regions.get_region")
    def test_load_target_list_background_no_names_file(self, mock_region: Mock) -> None:
        mock_region.return_value.names_file = None
        docket.load_target_list(self.scrape_key_write)

        item = docket.get_new_docket_item(self.scrape_key_write)
        assert item is not None
        assert item.message.data.decode() == json.dumps("empty")

    @patch("recidiviz.utils.regions.get_region")
    def test_load_target_list_last_names(self, mock_region: Mock) -> None:
        mock_region.return_value.names_file = (
            "../recidiviz/tests/ingest/testdata/docket/names/last_only.csv"
        )

        docket.load_target_list(self.scrape_key_write)

        names = []
        for _ in range(12):
            item = docket.get_new_docket_item(self.scrape_key_write)
            assert item is not None
            name_serialized = item.message.data.decode()
            names.append(json.loads(name_serialized))
        assert names == [
            ["SMITH", ""],
            ["JOHNSON", ""],
            ["WILLIAMS", ""],
            ["BROWN", ""],
            ["JONES", ""],
            ["MILLER", ""],
            ["DAVIS", ""],
            ["GARCIA", ""],
            ["RODRIGUEZ", ""],
            ["WILSON", ""],
            ["MARTINEZ", ""],
            ["ANDERSON", ""],
        ]
        assert not docket.get_new_docket_item(self.scrape_key_write)

    @patch("recidiviz.utils.regions.get_region")
    def test_load_target_list_last_names_with_query(self, mock_region: Mock) -> None:
        mock_region.return_value.names_file = (
            "../recidiviz/tests/ingest/testdata/docket/names/last_only.csv"
        )
        docket.load_target_list(self.scrape_key_write, surname="WILSON")

        names = []
        for _ in range(3):
            item = docket.get_new_docket_item(self.scrape_key_write)
            assert item is not None
            name_serialized = item.message.data.decode()
            names.append(json.loads(name_serialized))
        assert names == [
            ["WILSON", ""],
            ["MARTINEZ", ""],
            ["ANDERSON", ""],
        ]
        assert not docket.get_new_docket_item(self.scrape_key_write)

    @patch("recidiviz.utils.regions.get_region")
    def test_load_target_list_last_names_with_bad_query(
        self, mock_region: Mock
    ) -> None:
        mock_region.return_value.names_file = (
            "../recidiviz/tests/ingest/testdata/docket/names/last_only.csv"
        )
        docket.load_target_list(self.scrape_key_write, surname="GARBAGE")

        item = docket.get_new_docket_item(self.scrape_key_write)
        assert item is not None
        assert item.message.data.decode() == json.dumps(("GARBAGE", ""))
        assert not docket.get_new_docket_item(self.scrape_key_write)

    @patch("recidiviz.utils.regions.get_region")
    def test_load_target_list_full_names(self, mock_region: Mock) -> None:
        mock_region.return_value.names_file = (
            "../recidiviz/tests/ingest/testdata/docket/names/last_and_first.csv"
        )

        docket.load_target_list(self.scrape_key_write)

        names = []
        for _ in range(8):
            item = docket.get_new_docket_item(self.scrape_key_write)
            assert item is not None
            name_serialized = item.message.data.decode()
            names.append(json.loads(name_serialized))
        assert names == [
            ["Smith", "James"],
            ["Smith", "Michael"],
            ["Smith", "Robert"],
            ["Smith", "David"],
            ["Johnson", "James"],
            ["Johnson", "Michael"],
            ["Smith", "William"],
            ["Williams", "James"],
        ]
        assert not docket.get_new_docket_item(self.scrape_key_write)

    def test_get_new_docket_item_no_matching_items(self) -> None:
        docket.add_to_query_docket(self.scrape_key_write, get_payload()).result()

        docket_item = docket.get_new_docket_item(
            self.scrape_key_read, return_immediately=True
        )
        assert not docket_item

    def test_get_new_docket_item_no_items_at_all(self) -> None:
        docket_item = docket.get_new_docket_item(
            ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND),
            return_immediately=True,
        )
        assert not docket_item

    def test_purge_query_docket(self) -> None:
        docket.add_to_query_docket(self.scrape_key_write, get_payload()).result()
        docket.add_to_query_docket(self.scrape_key_read, get_payload()).result()

        docket.purge_query_docket(self.scrape_key_write)
        assert not docket.get_new_docket_item(
            self.scrape_key_write, return_immediately=True
        )
        assert docket.get_new_docket_item(self.scrape_key_read, return_immediately=True)

    def test_purge_query_docket_nothing_matching(self) -> None:
        docket.add_to_query_docket(self.scrape_key_read, get_payload()).result()

        docket.purge_query_docket(self.scrape_key_write)
        assert not docket.get_new_docket_item(
            self.scrape_key_write, return_immediately=True
        )

    def test_purge_query_docket_already_empty(self) -> None:
        docket.purge_query_docket(self.scrape_key_write)
        assert not docket.get_new_docket_item(
            self.scrape_key_write, return_immediately=True
        )


def get_payload() -> List[Dict[str, str]]:
    return [{"name": "Jacoby, Mackenzie"}, {"name": "Jacoby, Clementine"}]
