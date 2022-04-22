# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""A script that:
a) Pulls the results of PII scans of our raw data datasets
b) Compares those results to our raw data configs
c) Complains if any PII columns found in the scan are not marked as PII in the raw file
  configs.

Usage:
   python -m recidiviz.tools.ingest.one_offs.find_unlabeled_raw_data_pii
"""
import logging
import re
import sys
from collections import defaultdict
from typing import Dict, Set

from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import StrictStringFormatter

PII_SCAN_DATASET_ID = "phenggeler_dlp"

QUERY_TEMPLATE = f"""
SELECT
  location.container.root_path AS dataset_id,
  location.container.relative_path AS table_id,
  content_location.record_location.field_id.name AS column_name,
  STRING_AGG(DISTINCT info_type.name, ',') AS pii_types
FROM
  `{{project_id}}.{PII_SCAN_DATASET_ID}.{{region_pii_scan_table}}`,
  UNNEST(location.content_locations) as content_location
WHERE info_type.name NOT IN (
  -- These types of information are not PII on their own when not associated with other
  -- PII like names, IDs, etc. 
  'ETHNIC_GROUP', 'US_STATE', 'AGE', 'GENDER'
)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
"""

# Add more fields here as we discover exemptions
EXEMPT_FIELDS_BY_STATE: Dict[StateCode, Set[str]] = {
    StateCode.US_MI: {"name_suffix"},
}


def check_for_marked_pii(
    bq_client: BigQueryClient, state_code: StateCode, region_pii_scan_table: str
) -> bool:
    """For the given DLP PII scan table prints out all PII columns that are not marked
    as is_pii=True in the raw data docs. Returns True if any unmarked PII columns were
    found.
    """
    logging.info("*" * 80)
    logging.info("*" * 80)
    logging.info("Finding PII cols for state [%s] ...", state_code.value)
    query_job = bq_client.run_query_async(
        StrictStringFormatter().format(
            QUERY_TEMPLATE,
            project_id=metadata.project_id(),
            region_pii_scan_table=region_pii_scan_table,
        )
    )

    pii_cols_by_file_tag: Dict[str, Set[str]] = defaultdict(set)
    for row in query_job:
        column_name = row["column_name"]
        if (
            state_code in EXEMPT_FIELDS_BY_STATE
            and column_name in EXEMPT_FIELDS_BY_STATE[state_code]
        ):
            # This isn't a PII column
            continue
        pii_cols_by_file_tag[row["table_id"]].add(row["column_name"])

    total_found = sum(len(cols) for cols in pii_cols_by_file_tag)
    logging.info(
        "Found [%s] PII columns for [%s] across [%s] tables.",
        total_found,
        state_code.value,
        len(pii_cols_by_file_tag),
    )

    found_missing_pii_labels = False
    region_raw_file_config = DirectIngestRegionRawFileConfig(state_code.value)
    for file_tag, columns_with_pii in pii_cols_by_file_tag.items():
        if file_tag not in region_raw_file_config.raw_file_configs:
            logging.warning("SKIPPING FILE [%s] WITH NO RAW DATA CONFIG", file_tag)
            continue

        columns = region_raw_file_config.raw_file_configs[file_tag].columns
        columns_marked_as_pii = {c.name for c in columns if c.is_pii}
        missing = columns_with_pii - columns_marked_as_pii
        if missing:
            logging.error(
                "Found colums in [%s] not marked as PII: %s", file_tag, missing
            )
            found_missing_pii_labels = True
    return found_missing_pii_labels


def main() -> None:
    bq_client = BigQueryClientImpl()
    tables = bq_client.list_tables(PII_SCAN_DATASET_ID)
    found_missing_pii_labels = False
    for table in tables:
        match = re.match(r"(us_[a-z][a-z])_raw_data", table.table_id)
        if match:
            found_missing_pii_labels |= check_for_marked_pii(
                bq_client, StateCode(match.group(1).upper()), table.table_id
            )

    if found_missing_pii_labels:
        logging.error(
            "****** FOUND ONE OR MORE COLUMNS MISSING AN [is_pii] LABEL ******"
        )
        sys.exit(0)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    with local_project_id_override(GCP_PROJECT_STAGING):
        main()
