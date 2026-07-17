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
"""Builds the cached CSV export of individual-level Public Pathways data."""
import csv
import io
from datetime import date

from flask import Response
from sqlalchemy import Column, func
from sqlalchemy.orm import Query, Session, sessionmaker

from recidiviz.case_triage.shared_pathways.pathways_database_manager import (
    PathwaysDatabaseManager,
)
from recidiviz.case_triage.util import get_public_pathways_metric_redis
from recidiviz.cloud_memorystore import utils as cloud_memorystore_utils
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.public_pathways.schema import (
    MetricMetadata,
    PublicPrisonPopulationOverTime,
)
from recidiviz.persistence.database.schema_type import SchemaType

# The only columns ever surfaced in the individual-level export. A new dimension
# column added to PublicPrisonPopulationOverTime is excluded from the export by
# default until explicitly added here.
# This constant is never to contain PII columns. person_id is deliberately
# excluded even though it is not PII: it is our internal identifier, and this
# export should never give an external consumer a way to key off of it.
INCLUDED_INDIVIDUAL_LEVEL_COLUMNS = frozenset(
    {
        "state_code",
        "date_in_population",
        "time_period",
        "age_group",
        "facility",
        "race",
        "ethnicity",
        "sentence_length_min",
        "sentence_length_max",
        "charge_county_code",
        "offense_type",
        "charge_description",
        "admission_reason",
    }
)

# Number of rows buffered into the SQL cursor per fetch while building the CSV.
_ROWS_PER_CHUNK = 1000

# Bounds Redis growth from old dated cache keys (see _cache_key): a new import
# always produces a fresh key, so this is not required for cache correctness.
_CACHE_TTL_SECONDS = 60 * 60 * 24 * 7


def individual_level_export_columns() -> list[Column]:
    """Returns the PublicPrisonPopulationOverTime columns included in the
    individual-level export: INCLUDED_INDIVIDUAL_LEVEL_COLUMNS, in the table's
    column order.
    """
    return [
        getattr(PublicPrisonPopulationOverTime, column.name)
        for column in PublicPrisonPopulationOverTime.__table__.columns
        if column.name in INCLUDED_INDIVIDUAL_LEVEL_COLUMNS
    ]


def individual_level_export_filename(
    *, state_code: StateCode, last_updated: date
) -> str:
    """Returns the filename for the individual-level export, stamped with the date
    of the most recently received data for the given state.
    """
    return (
        f"{state_code.value.lower()}_individual_level_data_"
        f"{last_updated.isoformat()}.csv"
    )


def _get_last_updated(session: Session) -> date:
    """Returns the last_updated date recorded for PublicPrisonPopulationOverTime."""
    metadata_row = (
        Query(MetricMetadata)
        .filter(MetricMetadata.metric == PublicPrisonPopulationOverTime.__name__)
        .with_session(session)
        .one()
    )
    return metadata_row.last_updated


def _build_latest_snapshot_query(columns: list[Column]) -> Query:
    """Returns a query for the given columns, filtered to rows for the most recent
    date_in_population. This is the only filter applied: the individual-level export
    is always unfiltered and always reflects the latest snapshot.
    """
    max_date_subquery = Query(
        func.max(PublicPrisonPopulationOverTime.date_in_population)
    ).scalar_subquery()
    return Query(columns).filter(
        PublicPrisonPopulationOverTime.date_in_population == max_date_subquery
    )


def _cache_key(*, state_code: StateCode, last_updated: date) -> str:
    """Returns the individual-level export cache key for state_code, scoped to
    last_updated so a new data import always produces a fresh key rather than
    serving a stale cached export.
    """
    return f"{state_code.value} individual_level_export {last_updated.isoformat()}"


def _build_csv(*, session_factory: sessionmaker, columns: list[Column]) -> str:
    """Returns the individual-level export as a single CSV-encoded string: a
    header row followed by one row per record in the latest snapshot, fetched
    from the database in batches of _ROWS_PER_CHUNK.
    """
    column_names = [column.name for column in columns]
    with session_factory() as session:
        query = _build_latest_snapshot_query(columns).with_session(session)

        buffer = io.StringIO()
        writer = csv.writer(buffer)

        writer.writerow(column_names)
        for row in query.yield_per(_ROWS_PER_CHUNK):
            writer.writerow(row)

        return buffer.getvalue()


def build_individual_level_export_response(
    *, state_code: StateCode, enabled_states: list[str]
) -> Response:
    """Returns a CSV response of the most recent individual-level Public Pathways
    data for the given state, limited to INCLUDED_INDIVIDUAL_LEVEL_COLUMNS. The
    export itself is cached in Redis per state/last_updated date, so repeat
    requests for the same snapshot skip the database query.
    """
    database_manager = PathwaysDatabaseManager(
        enabled_states, SchemaType.PUBLIC_PATHWAYS
    )
    session_factory = database_manager.get_pathways_session(state_code)

    with session_factory() as session:
        last_updated = _get_last_updated(session)

    columns = individual_level_export_columns()
    filename = individual_level_export_filename(
        state_code=state_code, last_updated=last_updated
    )

    csv_data = cloud_memorystore_utils.get_or_set_str(
        get_public_pathways_metric_redis(),
        _cache_key(state_code=state_code, last_updated=last_updated),
        lambda: _build_csv(session_factory=session_factory, columns=columns),
        ex=_CACHE_TTL_SECONDS,
    )

    response = Response(csv_data, mimetype="text/csv")
    response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response
