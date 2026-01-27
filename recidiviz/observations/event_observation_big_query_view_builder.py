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
"""View builder that can be used to encode a collection of point-in-time observations of
a specified type.
"""
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_query_provider import BigQueryQueryProvider
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.observations.dataset_config import dataset_for_observation_type
from recidiviz.observations.event_type import EventType
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.utils.string_formatting import fix_indent
from recidiviz.utils.types import assert_type

# Attribute columns that we assume should be interpreted as integer values and serialized
# to JSON as such.
_INFERRED_INTEGER_ATTRIBUTE_COLS = ["person_id"]


class EventObservationBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """View builder that can be used to encode a collection of point-in-time
    observations of a specified type.

    This view is a building block for our aggregated metrics - each row is an
    observation about a unit of observation (e.g. person, supervision officer, etc.)
    which can later be associated with a unit of analysis (e.g. state_code, facility,
    etc.) in order to build a metric about that unit of analysis.
    """

    EVENT_DATE_OUTPUT_COL_NAME = "event_date"
    EVENT_ATTRIBUTES_OUTPUT_COL_NAME = "event_attributes"

    def __init__(
        self,
        # Type of event
        event_type: EventType,
        # Description of the event
        description: str,
        # Source for generating metric entity: requires either a standalone SQL query
        # string, or a BigQueryAddress if referencing an existing table
        sql_source: BigQueryAddress | str,
        # List of column names from source query to include in the attributes JSON blob
        attribute_cols: list[str],
        # Name of the column from source table that should be used as the event date
        event_date_col: str,
    ) -> None:
        self.sql_source = sql_source
        self.event_type = event_type
        self.event_date_col = event_date_col
        self.attribute_cols = attribute_cols

        address = self.view_address_for_event(event_type)
        super().__init__(
            dataset_id=address.dataset_id,
            view_id=address.table_id,
            description=description,
            view_query_template=self._build_query_template(
                event_type=event_type,
                sql_source=sql_source,
                attribute_cols=attribute_cols,
                event_date_col=event_date_col,
            ),
            should_materialize=True,
            clustering_fields=MetricUnitOfObservation(
                type=self.unit_of_observation_type
            ).primary_key_columns_ordered,
        )

    @property
    def observation_name(self) -> str:
        return self.event_type.value.lower()

    @property
    def unit_of_observation_type(self) -> MetricUnitOfObservationType:
        return self.event_type.unit_of_observation_type

    @classmethod
    def view_address_for_event(cls, event_type: EventType) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=dataset_for_observation_type(event_type),
            table_id=event_type.value.lower(),
        )

    @classmethod
    def materialized_view_address_for_event(
        cls, event_type: EventType
    ) -> BigQueryAddress:
        view_address = cls.view_address_for_event(event_type)
        return assert_type(
            cls._build_materialized_address(
                dataset_id=view_address.dataset_id,
                view_id=view_address.table_id,
                should_materialize=True,
                materialized_address_override=None,
            ),
            BigQueryAddress,
        )

    @classmethod
    def _build_query_template(
        cls,
        event_type: EventType,
        sql_source: BigQueryAddress | str,
        attribute_cols: list[str],
        event_date_col: str,
    ) -> str:
        """Builds and returns the query template for this event view."""

        if isinstance(sql_source, BigQueryAddress):
            source_query_fragment = (
                f"`{sql_source.format_address_for_query_template()}`"
            )
        else:
            source_query_fragment = f"""(
{BigQueryQueryProvider.strip_semicolon(fix_indent(sql_source, indent_level=4))}
)"""

        unit_of_observation = MetricUnitOfObservation(
            type=event_type.unit_of_observation_type
        )

        column_clauses = [
            *unit_of_observation.primary_key_columns_ordered,
            f"DATE({event_date_col}) AS {cls.EVENT_DATE_OUTPUT_COL_NAME}",
            *[
                (
                    f"CAST({col} AS INT64) AS {col}"
                    if col in _INFERRED_INTEGER_ATTRIBUTE_COLS
                    else f"CAST({col} AS STRING) AS {col}"
                )
                for col in attribute_cols
            ],
        ]
        columns_str = ",\n".join(column_clauses)

        return f"""
SELECT DISTINCT
{fix_indent(columns_str, indent_level=4)}
FROM {source_query_fragment}
"""

    def required_sql_source_input_columns(self) -> set[str]:
        unit_of_observation = MetricUnitOfObservation(
            type=self.unit_of_observation_type
        )
        return (
            set(unit_of_observation.primary_key_columns)
            | {self.event_date_col}
            | set(self.attribute_cols)
        )

    @classmethod
    def non_attribute_output_columns(
        cls, unit_of_observation_type: MetricUnitOfObservationType
    ) -> list[str]:
        unit_of_observation = MetricUnitOfObservation(type=unit_of_observation_type)
        return unit_of_observation.primary_key_columns_ordered + [
            cls.EVENT_DATE_OUTPUT_COL_NAME,
        ]
