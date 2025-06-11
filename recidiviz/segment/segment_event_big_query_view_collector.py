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
SegmentEventBigQueryViewBuilder.
"""
import itertools
from types import ModuleType
from typing import Callable

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import (
    BigQueryViewCollector,
    filename_matches_view_id_validator,
)
from recidiviz.segment import views as segment_views
from recidiviz.segment.product_type import ProductType
from recidiviz.segment.segment_event_big_query_view_builder import (
    SegmentEventBigQueryViewBuilder,
)
from recidiviz.utils.types import assert_type_list


class SegmentEventBigQueryViewCollector(
    BigQueryViewCollector[SegmentEventBigQueryViewBuilder]
):
    """A class that can be used to collect view builders of types
    SegmentEventBigQueryViewBuilder.
    """

    def collect_view_builders(
        self,
    ) -> list[SegmentEventBigQueryViewBuilder]:
        return list(
            itertools.chain.from_iterable(
                self.collect_segment_event_view_builders_by_product().values()
            )
        )

    def collect_segment_event_view_builders_by_product(
        self,
    ) -> dict[ProductType, list[SegmentEventBigQueryViewBuilder]]:

        builders_by_unit = {}
        for product_module in self.get_submodules(
            segment_views, submodule_name_prefix_filter=None
        ):
            product_type = self._product_type_from_module(product_module)
            builders = assert_type_list(
                self.collect_view_builders_in_module(
                    builder_type=SegmentEventBigQueryViewBuilder,
                    view_dir_module=product_module,
                    validate_builder_fn=self._get_view_builder_validator(product_type),
                ),
                SegmentEventBigQueryViewBuilder,
            )

            builders_by_unit[product_type] = list(builders)
        return builders_by_unit

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

            if not isinstance(builder, SegmentEventBigQueryViewBuilder):
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
    collector = SegmentEventBigQueryViewCollector()
    collector.collect_segment_event_view_builders_by_product()
