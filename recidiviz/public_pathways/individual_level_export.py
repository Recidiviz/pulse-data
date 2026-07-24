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
import json
from datetime import date
from typing import Iterable

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
        "sex",
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

# Raw sentinel values that Pathways' BigQuery id-to-label maps (see PATHWAYS_UNKNOWNS
# in recidiviz.calculator.query.state.state_specific_query_strings) normalize to the
# "UNKNOWN" value before assigning a label. A None column value is treated the same way.
_UNKNOWN_RAW_VALUES = frozenset(
    {"EXTERNAL_UNKNOWN", "INTERNAL_UNKNOWN", "PRESENT_WITHOUT_INFO", ""}
)

# time_period (e.g. "months_0_6") has no server-side id/name map: the dashboard
# formats it client-side via shared-pathways/src/utils.ts's timePeriodMap composed
# with timePeriod.ts's formatTimePeriodLabel. Mirrored here for the CSV export.
_TIME_PERIOD_LABELS: dict[str, str] = {
    "months_0_6": "6 months",
    "months_7_12": "1 year",
    "months_13_24": "2 years",
    "months_25_60": "5 years",
}

# Maps each translatable export column to its key within the JSON object stored in
# MetricMetadata.dynamic_filter_options (see get_pathways_dynamic_filter_options() in
# recidiviz.calculator.query.state.state_specific_query_strings). Columns not listed
# here (state_code, date_in_population, age_group) are already display-ready.
_DYNAMIC_FILTER_OPTION_KEYS_BY_COLUMN: dict[str, str] = {
    "facility": "facility_id_name_map",
    "race": "race_id_name_map",
    "ethnicity": "ethnicity_id_name_map",
    "sentence_length_min": "sentence_length_min_id_name_map",
    "sentence_length_max": "sentence_length_max_id_name_map",
    "charge_county_code": "charge_county_id_name_map",
    "offense_type": "offense_type_id_name_map",
    "charge_description": "charge_description_id_name_map",
    "admission_reason": "admission_reason_id_name_map",
}

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


def _get_metric_metadata(session: Session) -> MetricMetadata:
    """Returns the MetricMetadata row for PublicPrisonPopulationOverTime, which
    carries both the last_updated date and the dynamic_filter_options id-to-label
    maps used to translate the columns in the individual-level export.
    """
    return (
        Query(MetricMetadata)
        .filter(MetricMetadata.metric == PublicPrisonPopulationOverTime.__name__)
        .with_session(session)
        .one()
    )


def _build_label_maps(
    dynamic_filter_options: str | dict[str, str | None]
) -> dict[str, dict[str, str]]:
    """Returns a value->label dict per translatable export column: time_period from
    the static _TIME_PERIOD_LABELS, and the rest parsed out of dynamic_filter_options
    (a MetricMetadata.dynamic_filter_options value, itself a JSON object whose values
    are JSON-encoded arrays of {value, label} pairs; see
    get_pathways_dynamic_filter_options() in state_specific_query_strings.py).

    dynamic_filter_options may come back from SQLAlchemy already parsed into a dict
    (local dev fixtures are written via tools/shared_pathways/load_fixtures.py's raw
    SQL INSERT, which lets Postgres parse the JSON text directly into a jsonb object)
    or as a JSON-encoded string (real state data is written via
    application_data_import/server.py's SQLAlchemy ORM merge, whose JSONB bind
    processor re-serializes the already-string value); both shapes are handled here.
    A key may also be missing entirely rather than present with a null value, e.g. in
    older or hand-written fixture data predating a since-added dimension.
    """
    label_maps: dict[str, dict[str, str]] = {"time_period": _TIME_PERIOD_LABELS}
    parsed_dynamic_filter_options: dict[str, str | None] = (
        json.loads(dynamic_filter_options)
        if isinstance(dynamic_filter_options, str)
        else dynamic_filter_options
    )
    for column, key in _DYNAMIC_FILTER_OPTION_KEYS_BY_COLUMN.items():
        options_json = parsed_dynamic_filter_options.get(key)
        if options_json is None:
            continue
        options = json.loads(options_json)
        label_maps[column] = {option["value"]: option["label"] for option in options}
    return label_maps


def _translate_value(raw_value: str | None, label_map: dict[str, str]) -> str | None:
    """Returns the display label for raw_value per label_map, treating None and the
    Pathways "unknown" sentinel values (_UNKNOWN_RAW_VALUES) as the map's "UNKNOWN"
    entry. Falls back to the raw value itself if it has no entry in the map (e.g. new
    data ingested since the map was last generated) rather than dropping the row.
    """
    if raw_value is None or raw_value in _UNKNOWN_RAW_VALUES:
        return label_map.get("UNKNOWN", raw_value)
    return label_map.get(raw_value, raw_value)


def _translate_row(
    row: Iterable[str | None],
    *,
    column_names: list[str],
    label_maps: dict[str, dict[str, str]],
) -> list[str | None]:
    """Returns row with each column translated per label_maps, leaving columns with
    no entry in label_maps (state_code, date_in_population, age_group, sex) unchanged.
    """
    return [
        _translate_value(value, label_maps[column_name])
        if column_name in label_maps
        else value
        for column_name, value in zip(column_names, row)
    ]


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


def _build_csv(
    *,
    session_factory: sessionmaker,
    columns: list[Column],
    label_maps: dict[str, dict[str, str]],
) -> str:
    """Returns the individual-level export as a single CSV-encoded string: a
    header row followed by one row per record in the latest snapshot, fetched
    from the database in batches of _ROWS_PER_CHUNK. Columns present in label_maps
    are translated from their raw internal codes to display labels, matching what
    the dashboard UI shows for the same values.
    """
    column_names = [column.name for column in columns]
    with session_factory() as session:
        query = _build_latest_snapshot_query(columns).with_session(session)

        buffer = io.StringIO()
        writer = csv.writer(buffer)

        writer.writerow(column_names)
        for row in query.yield_per(_ROWS_PER_CHUNK):
            writer.writerow(
                _translate_row(row, column_names=column_names, label_maps=label_maps)
            )

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
        metric_metadata = _get_metric_metadata(session)
        last_updated = metric_metadata.last_updated
        dynamic_filter_options = metric_metadata.dynamic_filter_options
        if dynamic_filter_options is None:
            raise ValueError(
                f"No dynamic_filter_options found for PublicPrisonPopulationOverTime "
                f"in state [{state_code.value}]; cannot translate the individual-level "
                f"export's columns to display labels."
            )
        label_maps = _build_label_maps(dynamic_filter_options)

    columns = individual_level_export_columns()
    filename = individual_level_export_filename(
        state_code=state_code, last_updated=last_updated
    )

    csv_data = cloud_memorystore_utils.get_or_set_str(
        get_public_pathways_metric_redis(),
        _cache_key(state_code=state_code, last_updated=last_updated),
        lambda: _build_csv(
            session_factory=session_factory, columns=columns, label_maps=label_maps
        ),
        ex=_CACHE_TTL_SECONDS,
    )

    response = Response(csv_data, mimetype="text/csv")
    response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response
