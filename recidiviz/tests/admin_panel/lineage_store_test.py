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
"""Unit tests for lineage store"""
from unittest import TestCase
from unittest.mock import patch

from recidiviz.admin_panel.lineage_store import LineageStore
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.source_tables.source_table_repository import SourceTableRepository

# View builders for views forming a DAG shaped like an X:
# S1     S2
# |      |
#  1     2
#   \   /
#     3
#   /   \
#  4     5
X_SHAPED_DAG_VIEW_BUILDERS_LIST = [
    SimpleBigQueryViewBuilder(
        dataset_id="dataset_1",
        view_id="table_1",
        description="table_1 description",
        view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
    ),
    SimpleBigQueryViewBuilder(
        dataset_id="dataset_2",
        view_id="table_2",
        description="table_2 description",
        view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
    ),
    SimpleBigQueryViewBuilder(
        dataset_id="dataset_3",
        view_id="table_3",
        description="table_3 description",
        view_query_template="""
            SELECT * FROM `{project_id}.dataset_1.table_1`
            JOIN `{project_id}.dataset_2.table_2`
            USING (col)""",
    ),
    SimpleBigQueryViewBuilder(
        dataset_id="dataset_4",
        view_id="table_4",
        description="table_4 description",
        view_query_template="""
            SELECT * FROM `{project_id}.dataset_3.table_3`""",
    ),
    SimpleBigQueryViewBuilder(
        dataset_id="dataset_5",
        view_id="table_5",
        description="table_5 description",
        view_query_template="""
            SELECT * FROM `{project_id}.dataset_3.table_3`""",
    ),
]


class LineageStoreTest(TestCase):
    """Unit tests for lineage store"""

    def setUp(self) -> None:
        self.project_id_patcher = patch(
            "recidiviz.utils.metadata.project_id", return_value="recidiviz-456"
        )
        self.project_id_patcher.start()
        self.deployed_vbs_patch = patch(
            "recidiviz.admin_panel.lineage_store.deployed_view_builders",
            return_value=X_SHAPED_DAG_VIEW_BUILDERS_LIST,
        )
        self.deployed_vbs_patch.start()
        self.source_table_patch = patch(
            "recidiviz.admin_panel.lineage_store.build_source_table_repository_for_collected_schemata",
            return_value=SourceTableRepository(source_table_collections=[]),
        )
        self.source_table_patch.start()
        self.store = LineageStore()

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        self.deployed_vbs_patch.stop()
        self.source_table_patch.stop()

    def test_build_upstream_dep(self) -> None:
        # pylint: disable=protected-access
        address_to_dependency_str = self.store._build_upstream_dependencies()

        assert (
            address_to_dependency_str[
                BigQueryAddress(
                    dataset_id="source_dataset",
                    table_id="source_table",
                )
            ]
            == []
        )

        assert address_to_dependency_str[
            BigQueryAddress(
                dataset_id="dataset_1",
                table_id="table_1",
            )
        ] == ["source_dataset.source_table"]

        assert set(
            address_to_dependency_str[
                BigQueryAddress(
                    dataset_id="dataset_3",
                    table_id="table_3",
                )
            ]
        ) == {
            "dataset_1.table_1",
            "dataset_2.table_2",
            "source_dataset.source_table",
            "source_dataset.source_table_2",
        }

        assert set(
            address_to_dependency_str[
                BigQueryAddress(
                    dataset_id="dataset_4",
                    table_id="table_4",
                )
            ]
        ) == {
            "dataset_1.table_1",
            "dataset_2.table_2",
            "dataset_3.table_3",
            "source_dataset.source_table",
            "source_dataset.source_table_2",
        }

    def test_build_downstream_dep(self) -> None:
        # pylint: disable=protected-access
        address_to_dependency_str = self.store._build_downstream_dependencies()

        assert set(
            address_to_dependency_str[
                BigQueryAddress(
                    dataset_id="dataset_1",
                    table_id="table_1",
                )
            ]
        ) == {"dataset_3.table_3", "dataset_4.table_4", "dataset_5.table_5"}

        assert set(
            address_to_dependency_str[
                BigQueryAddress(
                    dataset_id="source_dataset",
                    table_id="source_table",
                )
            ]
        ) == {
            "dataset_1.table_1",
            "dataset_3.table_3",
            "dataset_4.table_4",
            "dataset_5.table_5",
        }
