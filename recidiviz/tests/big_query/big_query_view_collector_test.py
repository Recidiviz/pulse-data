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
"""Tests for BigQueryViewCollector."""

import os
import unittest
from typing import List

import attr
from mock import patch

import recidiviz
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestView,
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.tests.big_query import test_views
from recidiviz.tests.big_query.fake_big_query_view_builder import (
    FakeBigQueryViewBuilder,
)

VIEWS_DIR_RELATIVE_PATH = os.path.relpath(
    os.path.dirname(test_views.__file__), os.path.dirname(recidiviz.__file__)
)


class BigQueryViewCollectorTest(unittest.TestCase):
    """Tests for BigQueryViewCollector."""

    @attr.s(frozen=True)
    class DeflatedView:
        clazz: type[BigQueryView] = attr.ib()
        project: str = attr.ib()
        dataset_id: str = attr.ib()
        view_id: str = attr.ib()
        view_query: str = attr.ib()

    good_view_1: DeflatedView
    good_view_2: DeflatedView
    good_view_3: DeflatedView

    @classmethod
    def setUpClass(cls) -> None:
        cls.good_view_1 = cls.DeflatedView(
            clazz=BigQueryView,
            project="project-id",
            dataset_id="my_dataset",
            view_id="early_discharge_incarceration_sentence",
            view_query="SELECT * FROM table1",
        )
        cls.good_view_2 = cls.DeflatedView(
            clazz=DirectIngestPreProcessedIngestView,
            project="project-id",
            dataset_id="NO DATASET",
            view_id="ingest_view_name",
            view_query="WITH\n\nSELECT * FROM table1\nORDER BY some_col, another_col;",
        )
        cls.good_view_3 = cls.DeflatedView(
            clazz=BigQueryView,
            project="project-id",
            dataset_id="my_dataset_3",
            view_id="early_discharge_incarceration_sentence",
            view_query="SELECT * FROM table1",
        )

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "project-id"

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def _deflated(
        self, viewlist: List[BigQueryView]
    ) -> List["BigQueryViewCollectorTest.DeflatedView"]:
        return [
            BigQueryViewCollectorTest.DeflatedView(
                clazz=v.__class__,
                project=v.project,
                dataset_id=v.dataset_id,
                view_id=v.view_id,
                view_query=v.view_query,
            )
            for v in viewlist
        ]

    def test_collect_view_builders(self) -> None:
        builders = BigQueryViewCollector.collect_view_builders_in_dir(
            builder_type=FakeBigQueryViewBuilder,
            relative_dir_path=VIEWS_DIR_RELATIVE_PATH,
            view_file_prefix_filter="good_",
        )
        views: List[BigQueryView] = [builder.build() for builder in builders]
        self.assertCountEqual(
            [
                BigQueryViewCollectorTest.good_view_1,
                BigQueryViewCollectorTest.good_view_2,
            ],
            self._deflated(views),
        )

    def test_collect_view_builders_recursive(self) -> None:
        builders = BigQueryViewCollector.collect_view_builders_in_dir(
            builder_type=FakeBigQueryViewBuilder,
            relative_dir_path=VIEWS_DIR_RELATIVE_PATH,
            recurse=True,
            view_file_prefix_filter="good_",
        )
        views: List[BigQueryView] = [builder.build() for builder in builders]
        self.assertCountEqual(
            [
                BigQueryViewCollectorTest.good_view_1,
                BigQueryViewCollectorTest.good_view_2,
                BigQueryViewCollectorTest.good_view_3,
            ],
            self._deflated(views),
        )

    def test_collect_views_too_narrow_view_type(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Unexpected type \[FakeBigQueryViewBuilder\] for attribute \[VIEW_BUILDER\] "
            r"in file \[.*good_view_1.py\]. Expected type "
            r"\[DirectIngestPreProcessedIngestViewBuilder\].",
        ):
            # One of the views is only a BigQueryView, not a DirectIngestPreProcessedIngestView
            _ = BigQueryViewCollector.collect_view_builders_in_dir(
                builder_type=DirectIngestPreProcessedIngestViewBuilder,
                relative_dir_path=VIEWS_DIR_RELATIVE_PATH,
                view_file_prefix_filter="good_",
            )

    def test_collect_views_narrow_view_type_ok(self) -> None:
        builders = BigQueryViewCollector.collect_view_builders_in_dir(
            builder_type=FakeBigQueryViewBuilder,
            relative_dir_path=VIEWS_DIR_RELATIVE_PATH,
            view_file_prefix_filter="good_view_2",
        )

        self.assertCountEqual(
            [BigQueryViewCollectorTest.good_view_2],
            self._deflated([b.build() for b in builders]),
        )

    def test_file_no_builder_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"File \[.*bad_view_no_builder.py\] has no top-level attribute matching "
            r"\[VIEW_BUILDER\]",
        ):
            _ = BigQueryViewCollector.collect_view_builders_in_dir(
                builder_type=FakeBigQueryViewBuilder,
                relative_dir_path=VIEWS_DIR_RELATIVE_PATH,
                view_file_prefix_filter="bad_view_no_builder",
            )

    def test_file_builder_wrong_name_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"File \[.*view_builder_alternate_name.py\] has no top-level attribute "
            r"matching \[VIEW_BUILDER\]",
        ):
            _ = BigQueryViewCollector.collect_view_builders_in_dir(
                builder_type=FakeBigQueryViewBuilder,
                relative_dir_path=VIEWS_DIR_RELATIVE_PATH,
                view_file_prefix_filter="view_builder_alternate_name",
            )

    def test_collect_views_regex_name_match(self) -> None:
        builders = BigQueryViewCollector.collect_view_builders_in_dir(
            builder_type=FakeBigQueryViewBuilder,
            relative_dir_path=VIEWS_DIR_RELATIVE_PATH,
            view_file_prefix_filter="view_builder_alternate_name",
            view_builder_attribute_name_regex="[A-Z]{2}_VIEW_BUILDER",
        )

        self.assertCountEqual(
            [BigQueryViewCollectorTest.good_view_1],
            self._deflated([b.build() for b in builders]),
        )

    def test_file_builder_wrong_name_raises_regex(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"File \[.*good_view_1.py\] has no top-level attribute "
            r"matching \[VIEW_BUILDER_\.\*\]",
        ):
            _ = BigQueryViewCollector.collect_view_builders_in_dir(
                builder_type=FakeBigQueryViewBuilder,
                relative_dir_path=VIEWS_DIR_RELATIVE_PATH,
                view_file_prefix_filter="good_view_1",
                view_builder_attribute_name_regex=r"VIEW_BUILDER_.*",
            )

    def test_file_builder_wrong_name_raises_regex_strict(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"File \[.*good_view_1.py\] has no top-level attribute "
            r"matching \[VIEW]",
        ):
            _ = BigQueryViewCollector.collect_view_builders_in_dir(
                builder_type=FakeBigQueryViewBuilder,
                relative_dir_path=VIEWS_DIR_RELATIVE_PATH,
                view_file_prefix_filter="good_view_1",
                view_builder_attribute_name_regex=r"VIEW",
            )

    def test_file_builder_wrong_name_do_not_expect(self) -> None:
        builders = BigQueryViewCollector.collect_view_builders_in_dir(
            builder_type=FakeBigQueryViewBuilder,
            relative_dir_path=VIEWS_DIR_RELATIVE_PATH,
            view_file_prefix_filter="good_view_1",
            view_builder_attribute_name_regex=r"VIEW",
            expect_builders_in_all_files=False,
        )
        self.assertCountEqual([], [b.build() for b in builders])

    def test_file_builder_wrong_type_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Unexpected type \[str\] for attribute \[VIEW_BUILDER\] "
            r"in file \[.*bad_view_builder_wrong_type.py\]. Expected type "
            r"\[FakeBigQueryViewBuilder\].",
        ):
            _ = BigQueryViewCollector.collect_view_builders_in_dir(
                builder_type=FakeBigQueryViewBuilder,
                relative_dir_path=VIEWS_DIR_RELATIVE_PATH,
                view_file_prefix_filter="bad_view_builder_wrong_type",
            )
