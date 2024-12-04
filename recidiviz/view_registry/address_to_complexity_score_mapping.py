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
from typing import Callable

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
from recidiviz.validation.views.state.raw_data.stable_historical_raw_data_counts_validation import (
    VIEW_ID_TEMPLATE as STABLE_HISTORICAL_RAW_DATA_COUNTS_VALIDATION_VIEW_ID_SUFFIX,
)
from recidiviz.validation.views.state.raw_data.stale_raw_data_validation import (
    VIEW_ID_TEMPLATE as STALE_RAW_DATA_VALIDATION_VIEW_ID_SUFFIX,
)

AddressComplexityScoreMappingFn = Callable[[BigQueryAddress], int]


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
        self._source_table_repository = source_table_repository
        self._all_view_builders_by_address = {
            vb.address: vb for vb in all_view_builders
        }
        self._default_scores_by_address = {
            # Default score of 1 for all possible source tables / views referenced
            # by any view
            **{a: 1 for a in self._source_table_repository.source_tables},
            **{
                vb.table_for_query: 1
                for vb in self._all_view_builders_by_address.values()
            },
        }
        self._raw_data_table_configs = (
            self._source_table_repository.tables_labeled_with(RawDataSourceTableLabel)
        )

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
        return (
            isinstance(view_builder, DirectIngestRawDataTableLatestViewBuilder)
            or view_builder.view_id.endswith(STALE_RAW_DATA_VALIDATION_VIEW_ID_SUFFIX)
            or view_builder.view_id.endswith(
                STABLE_HISTORICAL_RAW_DATA_COUNTS_VALIDATION_VIEW_ID_SUFFIX
            )
        )

    def get_parent_complexity_map_for_view_2025(
        self, view_address: BigQueryAddress
    ) -> dict[BigQueryAddress, int]:
        """Returns a mapping that tells us the number of complexity score points that
        should be awarded for any view query reference to a given table or view address.
        Most references are given 1 point. State-specific tables that should not be
        referenced in our view graph are given higher point values.

        NOTE: THE ALGORITHM FOR COMPUTING THIS SCORE IS LOCKED FOR 2025 - DO NOT MAKE
        ANY MODIFICATIONS TO THE LOGIC, OTHERWISE WE WON'T BE ABLE TO LEGITIMATELY
        COMPARE HOW COMPLEXITY CHANGES OVER THE COURSE OF THE YEAR.
        """
        view_builder = self._all_view_builders_by_address[view_address]
        state_specific_raw_data_reference_score = (
            1 if self._view_can_reference_raw_data_directly_2025(view_builder) else 10
        )

        state_specific_non_raw_data_reference_score = (
            1 if isinstance(view_builder, UnionAllBigQueryViewBuilder) else 5
        )

        state_specific_view_score_mapping = {
            vb.table_for_query: (
                state_specific_raw_data_reference_score
                if isinstance(vb, DirectIngestRawDataTableLatestViewBuilder)
                else state_specific_non_raw_data_reference_score
            )
            for address, vb in self._all_view_builders_by_address.items()
            if address.is_state_specific_address()
        }
        return {
            **self._default_scores_by_address,
            # Raw data tables are expensive
            **{
                config.address: state_specific_raw_data_reference_score
                for config in self._raw_data_table_configs
            },
            **state_specific_view_score_mapping,
        }
