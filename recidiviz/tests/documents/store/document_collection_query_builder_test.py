# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for document_collection_query_builder.py."""
import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    collect_document_collection_configs,
)
from recidiviz.documents.store.document_collection_query_builder import (
    DocumentCollectionDiffQueryBuilder,
)
from recidiviz.tests.big_query.sqlglot_helpers import check_query_selects_output_columns


class TestBuildDocumentGenerationQuery(unittest.TestCase):
    def test_generation_query_output_matches_temp_table_schema(self) -> None:
        for state_code in StateCode:
            configs = collect_document_collection_configs(state_code)
            for config in configs.values():
                expected_columns = {
                    field.name for field in config.build_bq_temp_table_schema()
                }
                query = DocumentCollectionDiffQueryBuilder(
                    project_id="test-project",
                ).build_document_generation_query(
                    config=config,
                )
                try:
                    check_query_selects_output_columns(
                        query=query,
                        expected_output_columns=expected_columns,
                    )
                except ValueError as e:
                    raise ValueError(
                        f"Query output column mismatch for "
                        f"[{state_code.value}/{config.name}]"
                    ) from e
