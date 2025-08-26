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
"""Common constants for BigQuery infrastructure."""

# When creating temporary datasets with prefixed names, set the default table expiration to 24 hours
TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS = 24 * 60 * 60 * 1000

# Maximum length for any column name
BQ_TABLE_COLUMN_NAME_MAX_LENGTH = 300

# Maximum length for any column description on any column.
BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH = 1024

# Maximum length for any view query, including comments and whitespace. See
# https://cloud.google.com/bigquery/quotas#standard_view_limits.
BQ_VIEW_QUERY_MAX_LENGTH = 256000

# MOST external tables have the _FILE_NAME psudeocolumn available for query, see:
# https://cloud.google.com/bigquery/docs/query-cloud-storage-data#query_the_file_name_pseudo-column
EXTERNAL_DATA_FILE_NAME_PSEUDOCOLUMN = "_FILE_NAME"

# However, some external tables do not (e.g. google sheets) do not have the _FILE_NAME
# pseudocolumn.
EXTERNAL_DATA_SOURCE_FORMATS_WITHOUT_FILE_NAME_PSUEDOCOLUMN = {
    "GOOGLE_SHEETS",
}

# if time partitioning does not specify a field name, it means it uses ingest-time
# partitioning; in these cases, there are additional psuedocolumns that are available.
# see https://cloud.google.com/bigquery/docs/querying-partitioned-tables#query_an_ingestion-time_partitioned_table
# for more info
# _PARTITIONTIME is available for all time partitioning types
PARTITION_TIME_PSEUDOCOLUMN = "_PARTITIONTIME"
# _PARTITIONDATE is ONLY available for DAY partitioned tables
PARTITION_DATE_PSEUDOCOLUMN = "_PARTITIONDATE"

REQUIRE_PARTITION_FILTER_FIELD_NAME = "require_partition_filter"
