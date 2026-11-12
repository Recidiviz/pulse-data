# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Find views that reference raw data tables/views directly."""

from collections import defaultdict
from typing import Dict, Set

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import MATERIALIZED_SUFFIX, BigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    raw_data_views_dataset_for_region,
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_all_view_collector import (
    RAW_DATA_ALL_VIEW_ID_SUFFIX,
)
from recidiviz.ingest.direct.views.direct_ingest_latest_view_collector import (
    RAW_DATA_LATEST_VIEW_ID_SUFFIX,
)
from recidiviz.tools.raw_data_reference_reasons_yaml_loader import (
    RawDataReferenceVariant,
)

_VARIANT_SUFFIXES: Dict[RawDataReferenceVariant, str] = {
    RawDataReferenceVariant.LATEST: RAW_DATA_LATEST_VIEW_ID_SUFFIX,
    RawDataReferenceVariant.ALL: RAW_DATA_ALL_VIEW_ID_SUFFIX,
}


def _raw_dataset_state_and_variant() -> (
    Dict[str, tuple[StateCode, RawDataReferenceVariant]]
):
    """Returns, for every raw data table/view dataset across all states, the state it
    belongs to and the variant (`_all`, `_latest`, raw table) its tables represent.
    """
    datasets: Dict[str, tuple[StateCode, RawDataReferenceVariant]] = {}
    for state_code in get_existing_direct_ingest_states():
        for instance in DirectIngestInstance:
            datasets[raw_tables_dataset_for_region(state_code, instance)] = (
                state_code,
                RawDataReferenceVariant.TABLE,
            )
            datasets[raw_latest_views_dataset_for_region(state_code, instance)] = (
                state_code,
                RawDataReferenceVariant.LATEST,
            )
            datasets[raw_data_views_dataset_for_region(state_code, instance)] = (
                state_code,
                RawDataReferenceVariant.ALL,
            )
    return datasets


def _file_tag_for_table(table_id: str, variant: RawDataReferenceVariant) -> str:
    """Returns the file tag for a raw data table id read via the given variant.

    Strips the materialization suffix, then the variant's view suffix. Raises if a
    `_latest`/`_all` view table is missing its expected suffix, rather than silently
    misclassifying the reference.
    """
    table_id = table_id.removesuffix(MATERIALIZED_SUFFIX)
    if variant is RawDataReferenceVariant.TABLE:
        return table_id
    suffix = _VARIANT_SUFFIXES[variant]
    if not table_id.endswith(suffix):
        raise ValueError(
            f"Expected table [{table_id}] read via variant [{variant.value}] to end "
            f"with suffix [{suffix}]"
        )
    return table_id.removesuffix(suffix)


def find_direct_raw_data_references(
    view_builders: list[BigQueryViewBuilder],
) -> Dict[StateCode, Dict[str, Dict[BigQueryAddress, Set[RawDataReferenceVariant]]]]:
    """
    Finds direct references to raw data tables in the given list of BigQuery view builders.

    A direct reference is identified when a view queries the raw data we receive from a state, rather than querying the state-agnostic format.

    The returned dictionary has the following structure:
    {
        StateCode: {
            FileTag: {
                BigQueryAddress: Set[RawDataReferenceVariant]
            }
        }
    }

    - StateCode: Represents the state code of the source raw dataset.
    - FileTag: Represents the table id of the raw data table, with any `_latest`/`_all`
               view suffix stripped. Ex, myFileTag_latest and myFileTag_all are both
               represented as myFileTag; which variant was read is captured separately.
    - BigQueryAddress: Represents the address of the BigQuery view that references the raw data table.
    - RawDataReferenceVariant: Which flavor of the raw data (`_all` view, `_latest` view, or raw table) the view reads.

    For example, a returned dictionary entry might look like this:
    {
        StateCode.US_MO: {
            'LBAKRDTA_TAK033': {
                BigQueryAddress(dataset_id='analyst_data', table_id='us_mo_restrictive_housing_record'):
                    {RawDataReferenceVariant.LATEST}
            }
        }
    }
    Note that the same view address may be included multiple times if it references multiple raw data tables.
    """
    raw_data_references: Dict[
        StateCode, Dict[str, Dict[BigQueryAddress, Set[RawDataReferenceVariant]]]
    ] = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    views = [builder.build(sandbox_context=None) for builder in view_builders]
    raw_datasets = _raw_dataset_state_and_variant()
    for view in views:
        for parent_table in view.parent_tables:
            # We don't count raw data references that are *in* raw data datasets
            if view.dataset_id in raw_datasets:
                continue

            if parent_table.dataset_id in raw_datasets:
                state_code, variant = raw_datasets[parent_table.dataset_id]
                file_tag = _file_tag_for_table(parent_table.table_id, variant)
                raw_data_references[state_code][file_tag][view.address].add(variant)
    return raw_data_references
