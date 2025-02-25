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
"""Tests for ObservationBigQueryViewCollector."""
import unittest

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.observations.observation_big_query_view_collector import (
    ObservationBigQueryViewCollector,
)
from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.observations.views.events.person.us_ar_incentives import (
    VIEW_BUILDER as US_AR_INCENTIVES_VIEW_BUILDER,
)
from recidiviz.observations.views.events.person.us_ar_ovg_tranche_changes import (
    VIEW_BUILDER as US_AR_OVG_TRANCHE_CHANGES_VIEW_BUILDER,
)
from recidiviz.observations.views.spans.person.us_ar_ovg_sessions import (
    VIEW_BUILDER as US_AR_OVG_SESSIONS_VIEW_BUILDER,
)
from recidiviz.tests.big_query.sqlglot_helpers import (
    check_query_selects_output_columns,
    check_view_has_no_state_specific_logic,
)
from recidiviz.utils.metadata import local_project_id_override


class ObservationBigQueryViewCollectorTest(unittest.TestCase):
    """Tests for ObservationBigQueryViewCollector."""

    def test_collect_observation_builders(self) -> None:
        builders = ObservationBigQueryViewCollector().collect_view_builders()
        if not builders:
            raise ValueError(
                "Expected to find observation view builders but found none."
            )

        types = {type(b) for b in builders}

        self.assertEqual(
            {EventObservationBigQueryViewBuilder, SpanObservationBigQueryViewBuilder},
            types,
            "Expected to find observation builders of both builder types",
        )

        seen_addresses = set()
        for builder in builders:
            if builder.address in seen_addresses:
                raise ValueError(
                    f"Found two observation views defined with the same address "
                    f"[{builder.address.to_str()}]"
                )
            seen_addresses.add(builder.address)

    def test_all_observation_types_covered(self) -> None:
        builders = ObservationBigQueryViewCollector().collect_view_builders()
        seen_types: set[EventType | SpanType] = set()
        for builder in builders:
            observation_type: EventType | SpanType
            if isinstance(builder, EventObservationBigQueryViewBuilder):
                observation_type = builder.event_type
            elif isinstance(builder, SpanObservationBigQueryViewBuilder):
                observation_type = builder.span_type
            else:
                raise ValueError(
                    f"Unexpected observation builder type [{type(builder)}]"
                )

            if observation_type in seen_types:
                raise ValueError(
                    f"Found multiple views defined with observation type "
                    f"[{observation_type}]. Each SpanType and EventType should only "
                    f"have exactly one corresponding view defined."
                )
            seen_types.add(observation_type)

        expected_types = {*SpanType, *EventType}
        if missing_types := expected_types - seen_types:
            raise ValueError(
                f"No view defined for the following observation types: {missing_types}"
            )

    def test_observation_views_explicitly_select_correct_columns(self) -> None:
        for view_builder in ObservationBigQueryViewCollector().collect_view_builders():
            sql_source = view_builder.sql_source
            if isinstance(sql_source, BigQueryAddress):
                continue

            expected_output_columns = view_builder.required_sql_source_input_columns()

            try:
                check_query_selects_output_columns(
                    query=sql_source, expected_output_columns=expected_output_columns
                )
            except Exception as e:
                raise ValueError(
                    f"Invalid query format for observation query "
                    f"[{view_builder.address.to_str()}]"
                ) from e

    def test_observation_views_have_no_state_specific_logic(self) -> None:
        exempt_views = {
            US_AR_OVG_SESSIONS_VIEW_BUILDER.address,
            US_AR_OVG_TRANCHE_CHANGES_VIEW_BUILDER.address,
            US_AR_INCENTIVES_VIEW_BUILDER.address,
        }

        with local_project_id_override("recidiviz-456"):
            for (
                view_builder
            ) in ObservationBigQueryViewCollector().collect_view_builders():
                if view_builder.address in exempt_views:
                    continue
                check_view_has_no_state_specific_logic(view_builder)
