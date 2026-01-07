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
"""Collector for SegmentEventBigQueryViewBuilder instances from event view files."""

from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.segment import views as segment_views
from recidiviz.segment.segment_event_big_query_view_builder import (
    SegmentEventBigQueryViewBuilder,
)
from recidiviz.utils.types import assert_type_list


class SegmentEventBigQueryViewCollector(
    BigQueryViewCollector[SegmentEventBigQueryViewBuilder]
):
    """Collects SegmentEventBigQueryViewBuilder instances from event view files."""

    def collect_view_builders(self) -> list[SegmentEventBigQueryViewBuilder]:
        """Collect all event view builders from /recidiviz/segment/views/ directory.

        Returns:
            List of SegmentEventBigQueryViewBuilder instances
        """
        return list(
            assert_type_list(
                self.collect_view_builders_in_module(
                    builder_type=SegmentEventBigQueryViewBuilder,
                    view_dir_module=segment_views,
                    validate_builder_fn=None,
                ),
                SegmentEventBigQueryViewBuilder,
            )
        )


if __name__ == "__main__":
    # Collect and print the view builders
    collector = SegmentEventBigQueryViewCollector()
    builders = collector.collect_view_builders()
    print(f"Collected {len(builders)} event view builders:")
    for builder in builders:
        print(f"  - {builder.segment_event_name}")
