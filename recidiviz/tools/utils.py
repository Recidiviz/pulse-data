# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Utils methods for all scripts in the tools directory."""

import datetime
import logging
import re
from enum import Enum, auto
from typing import Optional, TypeVar, Tuple

import attr
from google.cloud import bigquery
from google.cloud.bigquery import Table
from more_itertools import one

INGESTED_FILE_REGEX = re.compile(
    r"^(processed_|unprocessed_|un)?(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}:\d{6}(raw|ingest_view)?.*)"
)
"""Regex for files going through direct ingest"""

DATE_SUBDIR_REGEX = re.compile(r"\d{4}/\d{2}/\d{2}")
"""Regex for parsing YYYY/MM/DD from file paths"""


def is_date_str(potential_date_str: str) -> bool:
    """Returns True if the string is an ISO-formatted date, (e.g. '2019-09-25'), False otherwise."""
    try:
        datetime.datetime.strptime(potential_date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def is_between_date_strs_inclusive(
    *,
    upper_bound_date: Optional[str],
    lower_bound_date: Optional[str],
    date_of_interest: str,
) -> bool:
    """Returns true if the provided |date_of_interest| is between the provided |upper_bound_date| and
    |lower_bound_date|.
    """

    if (lower_bound_date is None or date_of_interest >= lower_bound_date) and (
        upper_bound_date is None or date_of_interest <= upper_bound_date
    ):
        return True
    return False


def to_datetime(date_str: str) -> datetime.datetime:
    """Parses a string in the format 'YYYY-MM-DD HH:MM' into a datetime."""
    return datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M")


def _get_table(
    *, project_id: str, client: bigquery.Client, dataset: str, table_name: str
) -> Optional[Table]:
    """Returns a Table object found in BQ from the provided |project_id|, |dataset|, and |table_name|."""
    table_id = f"{project_id}.{dataset}.{table_name}"
    table = Table.from_string(table_id)
    return client.get_table(table)


# TODO(#3020): Move metadata methods into IngestMetadataManager class


class MetadataType(Enum):
    INGEST = auto()
    RAW = auto()


_MetadataRowType = TypeVar("_MetadataRowType", "_RawMetadataRow", "_IngestMetadataRow")


@attr.s
class _RawMetadataRow:
    region_code: str = attr.ib()
    file_id: int = attr.ib()
    file_tag: str = attr.ib()
    normalized_file_name: str = attr.ib()
    import_time: Optional[datetime.datetime] = attr.ib()
    processed_time: Optional[datetime.datetime] = attr.ib()
    datetimes_contained_lower_bound_inclusive: Optional[datetime.datetime] = attr.ib()
    datetimes_contained_upper_bound_inclusive: Optional[datetime.datetime] = attr.ib()


@attr.s
class _IngestMetadataRow:
    region_code: str = attr.ib()
    file_id: int = attr.ib()
    file_tag: str = attr.ib()
    normalized_file_name: str = attr.ib()
    export_time: Optional[datetime.datetime] = attr.ib()
    processed_time: Optional[datetime.datetime] = attr.ib()
    is_invalidated: Optional[bool] = attr.ib()
    datetimes_contained_lower_bound_exclusive: Optional[datetime.datetime] = attr.ib()
    datetimes_contained_upper_bound_inclusive: Optional[datetime.datetime] = attr.ib()


def get_table_name_for_type(metadata_type: MetadataType) -> str:
    if metadata_type == MetadataType.RAW:
        return "raw_file_metadata"
    if metadata_type == MetadataType.INGEST:
        return "ingest_file_metadata"
    raise ValueError(f"Unexpected metadata type {metadata_type}")


def _add_row_to_metadata(
    *,
    row: _MetadataRowType,
    metadata_type: MetadataType,
    client: bigquery.Client,
    project_id: str,
    dry_run: bool,
    file_tag: str,
) -> None:
    """Adds the given row to the raw_file_metadata table."""
    table_name = get_table_name_for_type(metadata_type=metadata_type)

    table = _get_table(
        project_id=project_id,
        client=client,
        dataset="direct_ingest_processing_metadata",
        table_name=table_name,
    )
    row_dict = row.__dict__

    if dry_run:
        logging.info(
            "[DRY RUN] would have appended rows to table %s:\n%s\n",
            file_tag,
            [row_dict],
        )
        return

    errors = client.insert_rows(table, [row_dict])
    if errors:
        raise ValueError(f"Encountered errors: {errors}")
    logging.info("Appended rows to table %s:\n%s\n", file_tag, [row])


def add_row_to_raw_metadata(
    *,
    client: bigquery.Client,
    project_id: str,
    region_code: str,
    import_time: datetime.datetime,
    dry_run: bool,
    file_id: int,
    file_tag: str,
    normalized_file_name: str,
    processed_time: Optional[datetime.datetime] = None,
    datetimes_contained_lower_bound_inclusive: Optional[datetime.datetime] = None,
    datetimes_contained_upper_bound_inclusive: Optional[datetime.datetime] = None,
) -> None:
    """Adds a row to the raw_file_metadata table with the input args."""
    row = _RawMetadataRow(
        region_code=region_code,
        file_id=file_id,
        file_tag=file_tag,
        normalized_file_name=normalized_file_name,
        import_time=import_time,
        processed_time=processed_time,
        datetimes_contained_lower_bound_inclusive=datetimes_contained_lower_bound_inclusive,
        datetimes_contained_upper_bound_inclusive=datetimes_contained_upper_bound_inclusive,
    )
    _add_row_to_metadata(
        row=row,
        client=client,
        project_id=project_id,
        dry_run=dry_run,
        file_tag=file_tag,
        metadata_type=MetadataType.RAW,
    )


def add_row_to_ingest_metadata(
    *,
    client: bigquery.Client,
    project_id: str,
    region_code: str,
    export_time: datetime.datetime,
    dry_run: bool,
    file_id: int,
    file_tag: str,
    normalized_file_name: str,
    processed_time: Optional[datetime.datetime] = None,
    is_invalidated: Optional[bool] = None,
    datetimes_contained_lower_bound_exclusive: Optional[datetime.datetime] = None,
    datetimes_contained_upper_bound_inclusive: Optional[datetime.datetime] = None,
) -> None:
    row = _IngestMetadataRow(
        region_code=region_code,
        file_id=file_id,
        file_tag=file_tag,
        normalized_file_name=normalized_file_name,
        export_time=export_time,
        is_invalidated=is_invalidated,
        processed_time=processed_time,
        datetimes_contained_lower_bound_exclusive=datetimes_contained_lower_bound_exclusive,
        datetimes_contained_upper_bound_inclusive=datetimes_contained_upper_bound_inclusive,
    )
    _add_row_to_metadata(
        row=row,
        client=client,
        project_id=project_id,
        dry_run=dry_run,
        file_tag=file_tag,
        metadata_type=MetadataType.INGEST,
    )


def mark_existing_metadata_row_as_processed(
    *,
    metadata_type: MetadataType,
    project_id: str,
    dry_run: bool,
    client: bigquery.Client,
    file_id: int,
    processed_time: datetime.datetime,
) -> None:
    table_name = get_table_name_for_type(metadata_type)
    _mark_existing_metadata_row_as_processed_helper(
        table_name=table_name,
        project_id=project_id,
        dry_run=dry_run,
        client=client,
        file_id=file_id,
        processed_time=processed_time,
    )


def _mark_existing_metadata_row_as_processed_helper(
    *,
    table_name: str,
    project_id: str,
    dry_run: bool,
    client: bigquery.Client,
    file_id: int,
    processed_time: datetime.datetime,
) -> None:
    query = f"""
        UPDATE
            `{project_id}.direct_ingest_processing_metadata.{table_name}`
        SET
            processed_time = DATETIME "{processed_time}"
        WHERE
            file_id = {file_id}
    """
    if dry_run:
        logging.info("[DRY RUN] Would have run query to mark as processed: %s", query)
        return

    query_job = client.query(query)
    query_job.result()
    logging.info("Ran query to mark as processed: %s", query)


def get_file_id_and_processed_status_for_file(
    *,
    metadata_type: MetadataType,
    project_id: str,
    region_code: str,
    client: bigquery.Client,
    normalized_file_name: str,
) -> Tuple[Optional[int], bool]:
    """Checks to see if the provided |normalized_file_name| has been registered in the raw_data_metadata table. If
    it has, it returns the file's file_id and whether or not the file has already been processed. If it has not,
    returns None, False
    """
    table_name = get_table_name_for_type(metadata_type=metadata_type)
    table_id = f"{project_id}.direct_ingest_processing_metadata.{table_name}"
    query = f"""SELECT file_id, processed_time FROM `{table_id}`
             WHERE region_code = '{region_code}' AND normalized_file_name = '{normalized_file_name}'"""
    query_job = client.query(query)
    rows = query_job.result()

    if rows.total_rows > 1:
        raise ValueError(
            f"Expected there to only be one row per combination of {region_code} and {normalized_file_name}"
        )

    if not rows.total_rows:
        # TODO(#3020): Once metadata tables are in postgres (and we don't have any limits on UPDATE queries), insert
        #  a row here that will have the processed_time filled in later.
        logging.info(
            "\nNo found row for %s and %s in %s.",
            normalized_file_name,
            region_code,
            table_id,
        )
        return None, False

    row = one(rows)
    file_id = row.get("file_id")
    processed_time = row.get("processed_time")
    logging.info(
        "Found row for %s and %s with values file_id: %s and processed_time: %s",
        normalized_file_name,
        region_code,
        file_id,
        processed_time,
    )
    return file_id, processed_time


def get_next_available_file_id(
    metadata_type: MetadataType, project_id: str, client: bigquery.Client
) -> int:
    """Retrieves the next available file_id in the raw_file_metadata table."""
    table_name = get_table_name_for_type(metadata_type)
    query = f"""SELECT MAX(file_id) AS max_file_id
                FROM `{project_id}.direct_ingest_processing_metadata.{table_name}`"""
    query_job = client.query(query)
    rows = query_job.result()
    max_file_id = one(rows).get("max_file_id")
    if max_file_id is None:
        return 1
    return max_file_id + 1
