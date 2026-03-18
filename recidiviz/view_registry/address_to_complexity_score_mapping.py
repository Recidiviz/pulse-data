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
"""Returns a mapping that tells us the number of complexity score points that should
be awarded for any view query reference to a given table or view address.
"""
import enum

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.ingest.direct.views.direct_ingest_latest_view_collector import (
    DirectIngestRawDataTableLatestViewBuilder,
)
from recidiviz.source_tables.source_table_config import RawDataSourceTableLabel
from recidiviz.source_tables.source_table_repository import SourceTableRepository


class ReferenceType(enum.Enum):
    SOURCE_TABLE = "source_table"
    RAW_DATA = "raw_data"
    STATE_SPECIFIC_NON_RAW_DATA_VIEW = "state_specific_non_raw_data_view"
    OTHER = "other"


class ParentAddressComplexityScoreMapper:
    """Class that returns a mapping that tells us the number of complexity score points
    that should be awarded for any view query reference to a given table or view
    address.
    """

    def __init__(
        self,
        source_table_repository: SourceTableRepository,
        all_view_builders: list[BigQueryViewBuilder],
    ) -> None:
        self._all_view_builders_by_address = {
            vb.address: vb for vb in all_view_builders
        }
        self._reference_type_by_address = {
            **{
                table: ReferenceType.SOURCE_TABLE
                for table in source_table_repository.source_tables
            },
            **{
                config.address: ReferenceType.RAW_DATA
                for config in source_table_repository.tables_labeled_with(
                    RawDataSourceTableLabel
                )
            },
            **{
                vb.table_for_query: self._reference_type_for_view_builder(address, vb)
                for address, vb in self._all_view_builders_by_address.items()
            },
        }

    @staticmethod
    def _reference_type_for_view_builder(
        address: BigQueryAddress, vb: BigQueryViewBuilder
    ) -> ReferenceType:
        if not address.is_state_specific_address():
            return ReferenceType.OTHER
        if isinstance(vb, DirectIngestRawDataTableLatestViewBuilder):
            return ReferenceType.RAW_DATA
        return ReferenceType.STATE_SPECIFIC_NON_RAW_DATA_VIEW

    @staticmethod
    def _view_can_reference_raw_data_directly_2025(
        view_builder: BigQueryViewBuilder,
    ) -> bool:
        """Returns True if references to raw data tables / raw data latest views in the
        |view_builder| query are expected and should not be penalized excessively.

        NOTE: THE ALGORITHM FOR COMPUTING THIS SCORE IS LOCKED FOR 2025 - DO NOT MAKE
        ANY MODIFICATIONS TO THE LOGIC, OTHERWISE WE WON'T BE ABLE TO LEGITIMATELY
        COMPARE HOW COMPLEXITY CHANGES OVER THE COURSE OF THE YEAR.
        """
        return isinstance(view_builder, DirectIngestRawDataTableLatestViewBuilder)

    def _get_reference_scores_for_caller_2025(
        self, view_address: BigQueryAddress
    ) -> dict[ReferenceType, int]:
        """Returns the score for each kind of parent reference from this given view.
        Most references are given 1 point. State-specific tables that should not be
        referenced in our view graph are given higher point values.

        NOTE: THE ALGORITHM FOR COMPUTING THIS SCORE IS LOCKED FOR 2025 - DO NOT MAKE
        ANY MODIFICATIONS TO THE LOGIC, OTHERWISE WE WON'T BE ABLE TO LEGITIMATELY
        COMPARE HOW COMPLEXITY CHANGES OVER THE COURSE OF THE YEAR.
        """
        view_builder = self._all_view_builders_by_address[view_address]
        return {
            ReferenceType.SOURCE_TABLE: 1,
            ReferenceType.RAW_DATA: 1
            if self._view_can_reference_raw_data_directly_2025(view_builder)
            else 10,
            ReferenceType.STATE_SPECIFIC_NON_RAW_DATA_VIEW: 1
            if isinstance(view_builder, UnionAllBigQueryViewBuilder)
            else 5,
            ReferenceType.OTHER: 1,
        }

    def get_parent_complexity_for_view_2025(
        self, *, child: BigQueryAddress, parent: BigQueryAddress
    ) -> int:
        """Returns the number of complexity score points that should be awarded for a
        view query reference to the given parent table or view address.

        NOTE: THE ALGORITHM FOR COMPUTING THIS SCORE IS LOCKED FOR 2025 - DO NOT MAKE
        ANY MODIFICATIONS TO THE LOGIC, OTHERWISE WE WON'T BE ABLE TO LEGITIMATELY
        COMPARE HOW COMPLEXITY CHANGES OVER THE COURSE OF THE YEAR.
        """
        if parent not in self._reference_type_by_address:
            raise ValueError(f"No known reference type for table [{parent.to_str()}]")
        return self._get_reference_scores_for_caller_2025(child)[
            self._reference_type_by_address[parent]
        ]
