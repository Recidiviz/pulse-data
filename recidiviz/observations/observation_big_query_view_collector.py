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
"""A class that can be used to collect view builders of types
EventObservationBigQueryViewBuilder and SpanObservationBigQueryViewBuilder.
"""
import itertools
from types import ModuleType
from typing import Callable

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import (
    BigQueryViewCollector,
    filename_matches_view_id_validator,
)
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.views import events, spans
from recidiviz.utils.types import assert_type_list

ObservationBigQueryViewBuilder = (
    EventObservationBigQueryViewBuilder | SpanObservationBigQueryViewBuilder
)


class ObservationBigQueryViewCollector(
    BigQueryViewCollector[ObservationBigQueryViewBuilder]
):
    """A class that can be used to collect view builders of types
    EventObservationBigQueryViewBuilder and SpanObservationBigQueryViewBuilder.
    """

    def collect_view_builders(
        self,
    ) -> list[ObservationBigQueryViewBuilder]:
        return [*self.collect_span_builders(), *self.collect_event_builders()]

    def collect_span_builders(self) -> list[SpanObservationBigQueryViewBuilder]:
        return list(
            itertools.chain.from_iterable(
                self.collect_span_builders_by_unit_of_observation().values()
            )
        )

    def collect_event_builders(self) -> list[EventObservationBigQueryViewBuilder]:
        return list(
            itertools.chain.from_iterable(
                self.collect_event_builders_by_unit_of_observation().values()
            )
        )

    def _unit_of_observation_from_module(
        self, unit_of_observation_module: ModuleType
    ) -> MetricUnitOfObservationType:
        unit_of_observation_str = unit_of_observation_module.__name__.split(".")[-1]
        try:
            return MetricUnitOfObservationType(unit_of_observation_str.upper())
        except Exception as e:
            raise ValueError(
                f"No MetricUnitOfObservationType found for module "
                f"{unit_of_observation_module.__name__}"
            ) from e

    def _get_view_builder_validator(
        self, unit_of_observation_type: MetricUnitOfObservationType
    ) -> Callable[[BigQueryViewBuilder, ModuleType], None]:
        def _builder_validator(
            builder: BigQueryViewBuilder,
            view_module: ModuleType,
        ) -> None:
            filename_matches_view_id_validator(builder, view_module)

            if not isinstance(builder, ObservationBigQueryViewBuilder):
                raise ValueError(f"Unexpected builder type [{type(builder)}]")

            if not unit_of_observation_type == builder.unit_of_observation_type:
                raise ValueError(
                    f"Found view [{builder.address.to_str()}] in module "
                    f"[{view_module.__name__}] with unit_of_observation_type "
                    f"[{builder.unit_of_observation_type}] which does not match "
                    f"expected type for views in that module: "
                    f"[{unit_of_observation_type}]."
                )

        return _builder_validator

    def collect_span_builders_by_unit_of_observation(
        self,
    ) -> dict[MetricUnitOfObservationType, list[SpanObservationBigQueryViewBuilder]]:

        builders_by_unit = {}
        for unit_of_observation_module in self.get_submodules(
            spans, submodule_name_prefix_filter=None
        ):
            unit_of_observation_type = self._unit_of_observation_from_module(
                unit_of_observation_module
            )
            builders = assert_type_list(
                self.collect_view_builders_in_module(
                    builder_type=SpanObservationBigQueryViewBuilder,
                    view_dir_module=unit_of_observation_module,
                    validate_builder_fn=self._get_view_builder_validator(
                        unit_of_observation_type
                    ),
                ),
                SpanObservationBigQueryViewBuilder,
            )

            builders_by_unit[unit_of_observation_type] = list(builders)
        return builders_by_unit

    def collect_event_builders_by_unit_of_observation(
        self,
    ) -> dict[MetricUnitOfObservationType, list[EventObservationBigQueryViewBuilder]]:
        builders_by_unit = {}
        for unit_of_observation_module in self.get_submodules(
            events, submodule_name_prefix_filter=None
        ):
            unit_of_observation_type = self._unit_of_observation_from_module(
                unit_of_observation_module
            )
            builders = assert_type_list(
                self.collect_view_builders_in_module(
                    builder_type=EventObservationBigQueryViewBuilder,
                    view_dir_module=unit_of_observation_module,
                    validate_builder_fn=self._get_view_builder_validator(
                        unit_of_observation_type
                    ),
                ),
                EventObservationBigQueryViewBuilder,
            )

            builders_by_unit[unit_of_observation_type] = list(builders)
        return builders_by_unit
