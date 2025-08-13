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
"""A class that can be used to collect view builders of types
SegmentProductEventBigQueryViewBuilder.
"""
import itertools
from collections import defaultdict
from types import ModuleType
from typing import Callable

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import (
    BigQueryViewCollector,
    filename_matches_view_id_validator,
)
from recidiviz.segment import views as segment_views
from recidiviz.segment.product_type import ProductType
from recidiviz.segment.segment_product_event_big_query_view_builder import (
    SegmentProductEventBigQueryViewBuilder,
)
from recidiviz.utils.types import assert_type_list


class SegmentProductEventBigQueryViewCollector(
    BigQueryViewCollector[SegmentProductEventBigQueryViewBuilder]
):
    """A class that can be used to collect view builders of types
    SegmentProductEventBigQueryViewBuilder.
    """

    def collect_view_builders(
        self,
    ) -> list[SegmentProductEventBigQueryViewBuilder]:
        return list(
            itertools.chain.from_iterable(
                self.collect_segment_product_event_view_builders_by_product().values()
            )
        )

    def collect_segment_product_event_view_builders_by_product(
        self,
    ) -> dict[ProductType, list[SegmentProductEventBigQueryViewBuilder]]:

        builders_by_unit = {}
        for product_module in self.get_submodules(
            segment_views, submodule_name_prefix_filter=None
        ):
            product_type = self._product_type_from_module(product_module)
            builders = assert_type_list(
                self.collect_view_builders_in_module(
                    builder_type=SegmentProductEventBigQueryViewBuilder,
                    view_dir_module=product_module,
                    validate_builder_fn=self._get_view_builder_validator(product_type),
                ),
                SegmentProductEventBigQueryViewBuilder,
            )

            builders_by_unit[product_type] = list(builders)
        return builders_by_unit

    def collect_segment_product_event_view_builders_by_event_source_table_address(
        self,
    ) -> dict[BigQueryAddress, list[SegmentProductEventBigQueryViewBuilder]]:
        """Collects product types by segment table SQL source."""
        product_event_builders_by_event_source_table_address: dict[
            BigQueryAddress, list[SegmentProductEventBigQueryViewBuilder]
        ] = defaultdict(list)
        for builder in self.collect_view_builders():
            product_event_builders_by_event_source_table_address[
                builder.segment_table_sql_source
            ].append(builder)
        return product_event_builders_by_event_source_table_address

    def _product_type_from_module(self, product_module: ModuleType) -> ProductType:
        product_type_str = product_module.__name__.split(".")[-1]
        try:
            return ProductType(product_type_str.upper())
        except Exception as e:
            raise ValueError(
                f"No ProductType found for module " f"{product_module.__name__}"
            ) from e

    def _get_view_builder_validator(
        self, product_type: ProductType
    ) -> Callable[[BigQueryViewBuilder, ModuleType], None]:
        def _builder_validator(
            builder: BigQueryViewBuilder,
            view_module: ModuleType,
        ) -> None:
            filename_matches_view_id_validator(builder, view_module)

            if not isinstance(builder, SegmentProductEventBigQueryViewBuilder):
                raise ValueError(f"Unexpected builder type [{type(builder)}]")

            if not product_type == builder.product_type:
                raise ValueError(
                    f"Found view [{builder.address.to_str()}] in module "
                    f"[{view_module.__name__}] with product_type "
                    f"[{builder.product_type}] which does not match "
                    f"expected type for views in that module: "
                    f"[{product_type}]."
                )
            if not builder.view_id == builder.segment_event_name:
                raise ValueError(
                    f"View ID [{builder.view_id}] does not match "
                    f"segment event table source [{builder.segment_event_name}] for "
                    f"view [{builder.address.to_str()}]."
                )

        return _builder_validator


if __name__ == "__main__":
    # Collect and print the view builders
    collector = SegmentProductEventBigQueryViewCollector()
    collector.collect_segment_product_event_view_builders_by_product()
