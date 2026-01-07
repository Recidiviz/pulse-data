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
"""View builder representing all pageview events recorded via Segment `pages` table,
along with the product type associated with each page."""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.segment.product_type import ProductType
from recidiviz.segment.segment_event_utils import (
    SEGMENT_DATASETS,
    build_segment_event_view_query_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_ID = "all_segment_pages"


def _get_pages_query_template(dataset: str) -> str:
    # Pages events are relevant to all product types
    return build_segment_event_view_query_template(
        segment_table_sql_source=BigQueryAddress(dataset_id=dataset, table_id="pages"),
        segment_table_jii_pseudonymized_id_columns=[],
        additional_attribute_cols=[],
        relevant_product_types=list(ProductType),
    )


# TODO(#46788): Deprecate this view and generalize `pages` to use standard
# Segment event infra once views are no longer configured by event X product builders
ALL_SEGMENT_PAGES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id="segment_events",
    view_query_template="\nUNION ALL\n".join(
        _get_pages_query_template(dataset) for dataset in SEGMENT_DATASETS
    ),
    view_id=_VIEW_ID,
    description=__doc__,
    should_materialize=True,
    clustering_fields=[],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ALL_SEGMENT_PAGES_VIEW_BUILDER.build_and_print()
