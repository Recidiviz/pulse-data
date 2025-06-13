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
"""A script for building and writing a set of LookML views that allow access to
observations at all units of observation.

Run the following to write views to the specified directory DIR:
python -m recidiviz.tools.looker.top_level_generators.observations_lookml_generator [--looker-repo-root [DIR]]
"""

import os

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.common.str_field_utils import snake_to_title
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import (
    DimensionLookMLViewField,
    LookMLFieldParameter,
    TimeDimensionGroupLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import LookMLFieldType
from recidiviz.looker.lookml_view_source_table import LookMLViewSourceTable
from recidiviz.observations.dataset_config import EventType
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.observations.observation_big_query_view_collector import (
    ObservationBigQueryViewCollector,
)
from recidiviz.observations.observation_type_utils import (
    date_column_names_for_observation_type,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.tools.looker.script_helpers import (
    get_generated_views_path,
    parse_and_validate_output_dir_arg,
)
from recidiviz.tools.looker.top_level_generators.base_lookml_generator import (
    LookMLGenerator,
)


def build_single_observation_lookml_view(
    observation_type: SpanType | EventType,
) -> LookMLView:
    """Builds a single LookML view for a given observation type."""
    observation_name = observation_type.value.lower()

    # Get all attributes from the observation builder
    builder = ObservationBigQueryViewCollector().get_view_builder_for_observation_type(
        observation_type
    )
    all_attribute_dimensions = [
        DimensionLookMLViewField(
            field_name=f"{observation_name}__{field}",
            parameters=[
                LookMLFieldParameter.type(
                    LookMLFieldType.NUMBER
                    if field == "person_id"
                    else LookMLFieldType.STRING
                ),
                LookMLFieldParameter.label(
                    f"{snake_to_title(observation_name)}: {snake_to_title(field)}"
                ),
                LookMLFieldParameter.view_label("Observation Attributes"),
                LookMLFieldParameter.sql(f"${{TABLE}}.{field}"),
            ],
        )
        for field in builder.attribute_cols
    ]

    unit_of_observation_keys = sorted(
        list(
            MetricUnitOfObservation(
                type=observation_type.unit_of_observation_type
            ).primary_key_columns
        )
    )

    all_unit_of_observation_key_dimensions = [
        DimensionLookMLViewField(
            field_name=field,
            parameters=[
                LookMLFieldParameter.type(
                    LookMLFieldType.NUMBER
                    if field == "person_id"
                    else LookMLFieldType.STRING
                ),
                LookMLFieldParameter.view_label("Units of Analysis"),
                LookMLFieldParameter.sql(f"${{TABLE}}.{field}"),
            ],
        )
        for field in unit_of_observation_keys
    ]

    date_columns = date_column_names_for_observation_type(observation_type)

    all_date_dimensions = [
        TimeDimensionGroupLookMLViewField.for_date_column(col).extend(
            additional_parameters=[
                LookMLFieldParameter.view_label("Time"),
            ]
        )
        for col in date_columns
    ]

    date_columns_query_fragments = [
        f"IFNULL(${{TABLE}}.{col}, '9999-12-31')" for col in date_columns
    ]
    unit_of_observation_keys_query_fragments = [
        f"${{TABLE}}.{col}" for col in unit_of_observation_keys
    ]
    all_primary_key_columns_query_fragments = (
        date_columns_query_fragments + unit_of_observation_keys_query_fragments
    )

    primary_key_dimension = DimensionLookMLViewField(
        field_name="primary_key",
        parameters=[
            LookMLFieldParameter.type(LookMLFieldType.STRING),
            LookMLFieldParameter.view_label("Units of Analysis"),
            LookMLFieldParameter.sql(
                f"CONCAT({list_to_query_string(all_primary_key_columns_query_fragments)})"
            ),
        ],
    )

    return LookMLView(
        view_name=observation_name,
        table=LookMLViewSourceTable.derived_table(
            f"SELECT * FROM {builder.table_for_query.to_str()}"
        ),
        fields=[
            primary_key_dimension,
            *all_unit_of_observation_key_dimensions,
            *all_date_dimensions,
            *all_attribute_dimensions,
        ],
    )


class ObservationsLookMLGenerator(LookMLGenerator):
    """Generates LookML files for observations at all units of observation"""

    @staticmethod
    def generate_lookml(output_dir: str) -> None:
        """
        Write observation LookML views to the given directory,
        which should be a path to the local copy of the looker repo
        """
        output_subdir = get_generated_views_path(
            output_dir=output_dir, module_name="observations"
        )

        for unit_of_observation_type in MetricUnitOfObservationType:
            sub_dir = os.path.join(
                output_subdir,
                "event",
                unit_of_observation_type.value.lower(),
            )
            event_builders_dict = (
                ObservationBigQueryViewCollector().collect_event_builders_by_unit_of_observation()
            )
            if unit_of_observation_type in event_builders_dict:
                for event_builder in event_builders_dict[unit_of_observation_type]:
                    observations_view = build_single_observation_lookml_view(
                        event_builder.event_type
                    )
                    observations_view.write(sub_dir, source_script_path=__file__)

        for unit_of_observation_type in MetricUnitOfObservationType:
            sub_dir = os.path.join(
                output_subdir,
                "span",
                unit_of_observation_type.value.lower(),
            )
            span_builders_dict = (
                ObservationBigQueryViewCollector().collect_span_builders_by_unit_of_observation()
            )
            if unit_of_observation_type in span_builders_dict:
                for span_builder in span_builders_dict[unit_of_observation_type]:
                    observations_view = build_single_observation_lookml_view(
                        span_builder.span_type
                    )
                    observations_view.write(sub_dir, source_script_path=__file__)


if __name__ == "__main__":
    ObservationsLookMLGenerator.generate_lookml(
        output_dir=parse_and_validate_output_dir_arg()
    )
