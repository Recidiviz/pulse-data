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
"""Builds, precomputes, and serves the bulk individual-level Public Pathways
export: a zip containing one CSV per month over the trailing BULK_EXPORT_MONTHS
months, each built the same way as the single-snapshot export in
individual_level_export.py. The zip is precomputed at data-import time (see
application_data_import/server.py) rather than per-request, since building it
requires resolving and querying BULK_EXPORT_MONTHS separate snapshots; the serving
route here only ever needs to look up the precomputed GCS object.
"""
import io
import tempfile
import zipfile
from datetime import date

from flask import Response
from sqlalchemy.orm import sessionmaker
from werkzeug.exceptions import NotFound

from recidiviz.case_triage.shared_pathways.pathways_database_manager import (
    PathwaysDatabaseManager,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.public_pathways.individual_level_export import (
    build_csv_for_date,
    build_label_maps,
    get_metric_metadata,
    individual_level_export_columns,
    individual_level_export_filename,
    resolve_snapshot_date_for_month,
)
from recidiviz.utils import metadata as gcp_metadata

# Number of trailing months of individual-level history bundled into the bulk
# export.
BULK_EXPORT_MONTHS = 60

_BULK_EXPORT_ZIP_CONTENT_TYPE = "application/zip"

# GCS custom metadata key the precomputed zip's last_updated date is stored under,
# so the serving route can stamp the download filename without re-querying the DB.
_LAST_UPDATED_METADATA_KEY = "last_updated"


def bulk_export_gcs_path(*, state_code: StateCode) -> GcsfsFilePath:
    """Returns the GCS path the precomputed bulk export lives at for state_code.
    There is only ever one live object per state: a new precompute overwrites the
    previous one in place rather than accumulating historical artifacts.
    """
    return GcsfsFilePath(
        bucket_name=f"{gcp_metadata.project_id()}-public-pathways-data",
        blob_name=f"individual_level_bulk_exports/{state_code.value.lower()}.zip",
    )


def bulk_export_filename(*, state_code: StateCode, last_updated: date) -> str:
    """Returns the filename for the bulk individual-level export zip, stamped with
    the date of the most recently received data for the given state.
    """
    return (
        f"{state_code.value.lower()}_individual_level_data_last_5_years_"
        f"{last_updated.isoformat()}.zip"
    )


def _trailing_months(*, end_date: date, num_months: int) -> list[tuple[int, int]]:
    """Returns num_months (year, month) pairs in chronological order, ending with
    the month containing end_date.
    """
    months: list[tuple[int, int]] = []
    year, month = end_date.year, end_date.month
    for _ in range(num_months):
        months.append((year, month))
        month -= 1
        if month == 0:
            year, month = year - 1, 12
    months.reverse()
    return months


def build_bulk_individual_level_export_zip(
    *,
    state_code: StateCode,
    session_factory: sessionmaker,
    label_maps: dict[str, dict[str, str]],
    as_of: date,
) -> bytes:
    """Returns a zip archive (as bytes) containing one CSV per month over the
    BULK_EXPORT_MONTHS months ending with as_of's month. A month with no snapshot
    resolved by resolve_snapshot_date_for_month() within its window is skipped
    rather than erroring, so states with less than BULK_EXPORT_MONTHS months of
    history still get a zip of whatever is available.
    """
    columns = individual_level_export_columns()

    with session_factory() as session:
        snapshot_dates = sorted(
            {
                snapshot_date
                for snapshot_date in (
                    resolve_snapshot_date_for_month(session, year=year, month=month)
                    for year, month in _trailing_months(
                        end_date=as_of, num_months=BULK_EXPORT_MONTHS
                    )
                )
                if snapshot_date is not None
            }
        )

    buffer = io.BytesIO()
    with zipfile.ZipFile(
        buffer, mode="w", compression=zipfile.ZIP_DEFLATED
    ) as zip_file:
        for snapshot_date in snapshot_dates:
            csv_data = build_csv_for_date(
                session_factory=session_factory,
                columns=columns,
                label_maps=label_maps,
                snapshot_date=snapshot_date,
            )
            zip_file.writestr(
                individual_level_export_filename(
                    state_code=state_code, last_updated=snapshot_date
                ),
                csv_data,
            )

    return buffer.getvalue()


def build_and_upload_bulk_individual_level_export(
    *, state_code: StateCode, enabled_states: list[str]
) -> None:
    """Builds the bulk individual-level export for state_code and uploads it to
    GCS, overwriting any previous artifact for that state. Intended to run as a
    post-import step whenever new PublicPrisonPopulationOverTime data lands (see
    application_data_import/server.py), so the serving route below always has a
    precomputed zip to return instead of building one per request.
    """
    database_manager = PathwaysDatabaseManager(
        enabled_states, SchemaType.PUBLIC_PATHWAYS
    )
    session_factory = database_manager.get_pathways_session(state_code)

    with session_factory() as session:
        metric_metadata = get_metric_metadata(session)
        last_updated = metric_metadata.last_updated
        dynamic_filter_options = metric_metadata.dynamic_filter_options
        if dynamic_filter_options is None:
            raise ValueError(
                f"No dynamic_filter_options found for PublicPrisonPopulationOverTime "
                f"in state [{state_code.value}]; cannot translate the bulk "
                f"individual-level export's columns to display labels."
            )
        label_maps = build_label_maps(dynamic_filter_options)

    zip_bytes = build_bulk_individual_level_export_zip(
        state_code=state_code,
        session_factory=session_factory,
        label_maps=label_maps,
        as_of=last_updated,
    )

    with tempfile.NamedTemporaryFile(suffix=".zip") as temp_file:
        temp_file.write(zip_bytes)
        temp_file.flush()
        GcsfsFactory.build().upload_from_contents_handle_stream(
            path=bulk_export_gcs_path(state_code=state_code),
            contents_handle=LocalFileContentsHandle(
                local_file_path=temp_file.name, cleanup_file=False
            ),
            content_type=_BULK_EXPORT_ZIP_CONTENT_TYPE,
            metadata={_LAST_UPDATED_METADATA_KEY: last_updated.isoformat()},
        )


def build_bulk_individual_level_export_response(*, state_code: StateCode) -> Response:
    """Returns the precomputed bulk individual-level export zip for state_code from
    GCS. Raises NotFound if no precomputed export exists yet for this state (it
    should always exist after the first successful import; see
    build_and_upload_bulk_individual_level_export).
    """
    gcsfs = GcsfsFactory.build()
    gcs_path = bulk_export_gcs_path(state_code=state_code)
    if not gcsfs.exists(gcs_path):
        raise NotFound(
            f"No bulk individual-level export found for state [{state_code.value}]"
        )

    object_metadata = gcsfs.get_metadata(gcs_path) or {}
    last_updated_str = object_metadata.get(_LAST_UPDATED_METADATA_KEY)
    if last_updated_str is None:
        raise ValueError(
            f"Bulk individual-level export for state [{state_code.value}] is "
            f"missing its [{_LAST_UPDATED_METADATA_KEY}] GCS metadata."
        )

    zip_bytes = gcsfs.download_as_bytes(gcs_path)
    filename = bulk_export_filename(
        state_code=state_code, last_updated=date.fromisoformat(last_updated_str)
    )

    response = Response(zip_bytes, mimetype=_BULK_EXPORT_ZIP_CONTENT_TYPE)
    response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response
