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
from recidiviz.big_query.big_query_view_column import String
from recidiviz.calculator.query.state.dataset_config import (
    CASE_PLANNING_PRODUCTION_DATASET,
    JII_FRONTEND_PROD_SEGMENT_DATASET,
    PULSE_DASHBOARD_SEGMENT_DATASET,
)
from recidiviz.segment.product_type import ProductType
from recidiviz.segment.segment_event_utils import (
    build_segment_event_view_query_template,
    segment_event_schema,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_ID = "all_segment_pages"

_UTM_COLS = ["utm_source", "utm_medium", "utm_campaign", "utm_term"]

_NULL_UTM_COLS_SNIPPET = ", ".join(
    f"CAST(NULL AS STRING) AS {col}" for col in _UTM_COLS
)

_DATASETS_WITH_UTM_COLS = [PULSE_DASHBOARD_SEGMENT_DATASET]

# TODO(OBT-39139): Include public pathways dashboard views here once we have proper handling for them in segment_event_big_query_view_builder.py
_STAFF_PAGES_DATASETS = [
    PULSE_DASHBOARD_SEGMENT_DATASET,
    CASE_PLANNING_PRODUCTION_DATASET,
]

_JII_PAGES_DATASETS = [
    JII_FRONTEND_PROD_SEGMENT_DATASET,
]

event_schema = {col.name: col for col in segment_event_schema()}
_SCHEMA = [
    event_schema["state_code"],
    event_schema["user_id"],
    event_schema["email_address"],
    event_schema["event_ts"],
    event_schema["person_id"],
    event_schema["context_page_path"],
    event_schema["context_page_url"],
    event_schema["product_type"],
] + [
    String(
        name="utm_source",
        description="The utm_source query parameter in the URL that led to this pageview.",
        mode="NULLABLE",
    ),
    String(
        name="utm_medium",
        description="The utm_medium query parameter in the URL that led to this pageview.",
        mode="NULLABLE",
    ),
    String(
        name="utm_campaign",
        description="The utm_campaign query parameter in the URL that led to this pageview.",
        mode="NULLABLE",
    ),
    String(
        name="utm_term",
        description="The utm_term query parameter in the URL that led to this pageview.",
        mode="NULLABLE",
    ),
]


def _get_pages_query_template(
    dataset: str, additional_cols: list[str], user_is_jii: bool = False
) -> str:
    return build_segment_event_view_query_template(
        segment_table_sql_source=BigQueryAddress(dataset_id=dataset, table_id="pages"),
        segment_table_jii_pseudonymized_id_columns=[],
        additional_attribute_cols=additional_cols,
        relevant_product_types=list(ProductType),
        user_is_jii=user_is_jii,
    )


def _get_staff_pages_query(dataset: str) -> str:
    # Only pulse_dashboard_segment_metrics.pages has UTM parameter columns,
    # not case_planning_production.pages
    if dataset in _DATASETS_WITH_UTM_COLS:
        return _get_pages_query_template(dataset, _UTM_COLS)
    return f"SELECT *, {_NULL_UTM_COLS_SNIPPET} FROM ({_get_pages_query_template(dataset, [])})"


def _get_jii_pages_query(dataset: str) -> str:
    # Scope to the JII opportunities app *hosts* (across all vendor domains):
    # CPA reentry-assessment surface embedded in the opportunities app is retained here
    return (
        f"SELECT *, {_NULL_UTM_COLS_SNIPPET} FROM "
        f"({_get_pages_query_template(dataset, [], user_is_jii=True)}) "
        f"WHERE {ProductType.JII_OPPORTUNITIES_APP.url_base_filter()}"
    )


# TODO(#46788): Deprecate this view and generalize `pages` to use standard
# Segment event infra once views are no longer configured by event X product builders
ALL_SEGMENT_PAGES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id="segment_events",
    view_query_template="\nUNION ALL\n".join(
        [_get_staff_pages_query(dataset) for dataset in _STAFF_PAGES_DATASETS]
        + [_get_jii_pages_query(dataset) for dataset in _JII_PAGES_DATASETS]
    ),
    view_id=_VIEW_ID,
    description=__doc__,
    should_materialize=True,
    schema=_SCHEMA,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ALL_SEGMENT_PAGES_VIEW_BUILDER.build_and_print()
