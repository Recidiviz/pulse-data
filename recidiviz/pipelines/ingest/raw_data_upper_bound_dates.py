# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Validates and backfills the `raw_data_upper_bound_dates` input shared by
the activity and identity ingest pipelines."""
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.gating import RAW_DATA_TABLES_ALLOWED_EMPTY_BY_INGEST_VIEW
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)


def validate_and_backfill_raw_data_upper_bound_dates(
    *,
    state_code: StateCode,
    region: DirectIngestRegion,
    view_collector: DirectIngestViewQueryBuilderCollector,
    view_names: list[str],
    raw_data_upper_bound_dates: dict[str, str],
) -> dict[str, str]:
    """Returns a validated and backfilled copy of `raw_data_upper_bound_dates`.

    Validates that every file tag in `raw_data_upper_bound_dates` is a real
    raw file tag for the region. For each view in `view_names`, checks that
    every raw data dependency has an upper bound date; dependencies that are
    missing data must be listed in
    `RAW_DATA_TABLES_ALLOWED_EMPTY_BY_INGEST_VIEW[state_code][view_name]`, in
    which case they are backfilled with the max upper bound of the view's
    populated peer dependencies (the exact value doesn't matter because the
    table is empty).
    """
    all_raw_file_tags = DirectIngestRegionRawFileConfig(
        state_code.value, region_module=region.region_module
    ).raw_file_tags

    if unexpected_file_tags := set(raw_data_upper_bound_dates) - all_raw_file_tags:
        raise ValueError(
            f"Found unexpected file tags in raw_data_upper_bound_dates. These are "
            f"not valid raw file tags for [{state_code.value}]: "
            f"[{unexpected_file_tags}]. "
        )

    result = dict(raw_data_upper_bound_dates)
    for view_name in view_names:
        view_query_builder = view_collector.get_query_builder_by_view_name(view_name)
        dependencies_missing_data = (
            view_query_builder.raw_data_table_dependency_file_tags - set(result)
        )
        if not dependencies_missing_data:
            continue
        allowed_empty = RAW_DATA_TABLES_ALLOWED_EMPTY_BY_INGEST_VIEW.get(
            state_code, {}
        ).get(view_name, set())
        if still_missing := dependencies_missing_data - allowed_empty:
            raise ValueError(
                f"Found dependency table(s) of ingest view [{view_name}] with no "
                f"data: {still_missing}"
            )
        populated_upper_bounds = [
            result[ft]
            for ft in view_query_builder.raw_data_table_dependency_file_tags
            if result.get(ft)
        ]
        if not populated_upper_bounds:
            raise ValueError(
                f"At least one raw data dependency of view [{view_name}] "
                f"must be hydrated."
            )
        fill_upper_bound = max(populated_upper_bounds)
        for file_tag in dependencies_missing_data:
            result[file_tag] = fill_upper_bound
    return result
