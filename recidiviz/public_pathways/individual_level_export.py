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
from datetime import date, timedelta
from typing import Callable, Iterable

from flask import Response
from sqlalchemy import Column, func
from sqlalchemy.orm import Query, Session, sessionmaker
from werkzeug.exceptions import BadRequest

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
from recidiviz.utils.environment import CloudRunEnvironment, in_cloud_run

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
        "age_group",
        "facility",
        "sex",
        "race",
        "ethnicity",
        "months_at_facility",
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

# Maps each translatable export column to its key within the JSON object stored in
# MetricMetadata.dynamic_filter_options (see get_pathways_dynamic_filter_options() in
# recidiviz.calculator.query.state.state_specific_query_strings). Columns not listed
# here (state_code, date_in_population, age_group) are already display-ready.
_DYNAMIC_FILTER_OPTION_KEYS_BY_COLUMN: dict[str, str] = {
    "facility": "facility_id_name_map",
    "race": "race_id_name_map",
    "ethnicity": "ethnicity_id_name_map",
    "months_at_facility": "months_at_facility_id_name_map",
    "sentence_length_min": "sentence_length_min_id_name_map",
    "sentence_length_max": "sentence_length_max_id_name_map",
    "charge_county_code": "charge_county_id_name_map",
    "offense_type": "offense_type_id_name_map",
    "charge_description": "charge_description_id_name_map",
    "admission_reason": "admission_reason_id_name_map",
}

# sentence_length_min/max's translated labels (e.g. "12-17") are bare numeric
# ranges. Excel and Sheets auto-parse a bare range like "12-17" as a date (it
# displays as "17-Dec"), so the individual-level export appends " months" to
# these labels. The shared BigQuery label maps that feed the live dashboard's
# filter dropdowns (get_pathways_sentence_length_min/max_id_name_map in
# state_specific_query_strings.py) are left unchanged.
_SENTENCE_LENGTH_COLUMNS = frozenset({"sentence_length_min", "sentence_length_max"})

# Translated sentence-length labels that describe a sentence type rather than a
# span of months; these are left as-is instead of getting a " months" suffix.
_SENTENCE_LENGTH_NON_DURATION_LABELS = frozenset(
    {"Life", "Life, no parole", "Not Coded"}
)

# Renames the sentence-length columns' CSV headers to state their unit
# explicitly (DOCCS found the bare "sentence_length_min"/"_max" ambiguous).
_EXPORT_COLUMN_HEADER_OVERRIDES: dict[str, str] = {
    "sentence_length_min": "sentence_length_min_in_months",
    "sentence_length_max": "sentence_length_max_in_months",
}

# Number of rows buffered into the SQL cursor per fetch while building the CSV.
_ROWS_PER_CHUNK = 1000

# _cache_key()'s revision component outside Cloud Run (local dev, tests, CI),
# where there is no Cloud Run revision to key off of.
_NON_CLOUD_RUN_CACHE_REVISION = "local"

# Bounds Redis growth from old dated cache keys (see _cache_key): a new import or
# a new deploy always produces a fresh key, so this is not required for cache
# correctness.
_CACHE_TTL_SECONDS = 60 * 60 * 24 * 7

# How far a resolved month snapshot's date_in_population may drift from the 1st of
# the requested month. Most months land exactly on the 1st, but ETL delays have
# pushed it a few days late in the past; this tolerates that without matching an
# unrelated month's snapshot.
_MONTH_SNAPSHOT_WINDOW_DAYS = 15


def individual_level_export_columns() -> list[Column]:
    """Returns the INCLUDED_INDIVIDUAL_LEVEL_COLUMNS columns of
    PublicPrisonPopulationOverTime, in table column order.
    """
    return [
        getattr(PublicPrisonPopulationOverTime, column.name)
        for column in PublicPrisonPopulationOverTime.__table__.columns
        if column.name in INCLUDED_INDIVIDUAL_LEVEL_COLUMNS
    ]


def individual_level_export_filename(
    *, state_code: StateCode, last_updated: date
) -> str:
    """Returns the individual-level export's filename, stamped with state_code and
    the export's snapshot date.
    """
    return (
        f"{state_code.value.lower()}_individual_level_data_"
        f"{last_updated.isoformat()}.csv"
    )


def get_metric_metadata(session: Session) -> MetricMetadata:
    """Returns the MetricMetadata row for PublicPrisonPopulationOverTime: its
    last_updated date and dynamic_filter_options label maps.
    """
    return (
        Query(MetricMetadata)
        .filter(MetricMetadata.metric == PublicPrisonPopulationOverTime.__name__)
        .with_session(session)
        .one()
    )


def build_label_maps(
    dynamic_filter_options: str | dict[str, str | None]
) -> dict[str, dict[str, str]]:
    """Returns a value->label dict per translatable export column, parsed out of
    dynamic_filter_options (see get_pathways_dynamic_filter_options() in
    state_specific_query_strings.py). Accepts dynamic_filter_options as either a
    JSON string or an already-parsed dict, and tolerates missing keys.
    """
    label_maps: dict[str, dict[str, str]] = {}
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


def _label_map_lookup_key(raw_value: str | None) -> str:
    """Returns the label-map lookup key for raw_value: "UNKNOWN" for
    None/_UNKNOWN_RAW_VALUES, otherwise raw_value itself.
    """
    if raw_value is None or raw_value in _UNKNOWN_RAW_VALUES:
        return "UNKNOWN"
    return raw_value


def _translate_value(raw_value: str | None, label_map: dict[str, str]) -> str | None:
    """Returns the display label for raw_value per label_map, falling back to
    raw_value if it has no entry in the map.
    """
    return label_map.get(_label_map_lookup_key(raw_value), raw_value)


def _append_sentence_length_months_suffix(value: str | None) -> str | None:
    """Returns value with " months" appended, unless None or in
    _SENTENCE_LENGTH_NON_DURATION_LABELS.
    """
    if value is None or value in _SENTENCE_LENGTH_NON_DURATION_LABELS:
        return value
    return f"{value} months"


def _translate_row(
    row: Iterable[str | None],
    *,
    column_names: list[str],
    label_maps: dict[str, dict[str, str]],
) -> list[str | None]:
    """Returns row with each column translated per label_maps; columns absent from
    label_maps are left unchanged. sentence_length_min/max also get a " months"
    suffix, but only when the value was actually resolved via label_maps, to avoid
    double-labeling an untranslated fallback (e.g. "48-71 MONTHS").
    """
    translated_row = []
    for column_name, value in zip(column_names, row):
        label_map = label_maps.get(column_name)
        if label_map is None:
            translated_row.append(value)
            continue
        translated_value = _translate_value(value, label_map)
        if (
            column_name in _SENTENCE_LENGTH_COLUMNS
            and _label_map_lookup_key(value) in label_map
        ):
            translated_value = _append_sentence_length_months_suffix(translated_value)
        translated_row.append(translated_value)
    return translated_row


def _build_latest_snapshot_query(columns: list[Column]) -> Query:
    """Returns a query for the given columns, filtered to rows for the most recent
    date_in_population.
    """
    max_date_subquery = Query(
        func.max(PublicPrisonPopulationOverTime.date_in_population)
    ).scalar_subquery()
    return Query(columns).filter(
        PublicPrisonPopulationOverTime.date_in_population == max_date_subquery
    )


def build_snapshot_query_for_date(columns: list[Column], snapshot_date: date) -> Query:
    """Returns a query for the given columns, filtered to rows for exactly the
    given date_in_population.
    """
    return Query(columns).filter(
        PublicPrisonPopulationOverTime.date_in_population == snapshot_date
    )


def resolve_snapshot_date_for_month(
    session: Session, *, year: int, month: int
) -> date | None:
    """Returns the date_in_population closest to the 1st of the given month, among
    dates within _MONTH_SNAPSHOT_WINDOW_DAYS of it, or None if none qualify.
    """
    month_start = date(year, month, 1)
    window_start = month_start - timedelta(days=_MONTH_SNAPSHOT_WINDOW_DAYS)
    window_end = month_start + timedelta(days=_MONTH_SNAPSHOT_WINDOW_DAYS)

    candidate_dates = (
        Query(PublicPrisonPopulationOverTime.date_in_population)
        .filter(
            PublicPrisonPopulationOverTime.date_in_population >= window_start,
            PublicPrisonPopulationOverTime.date_in_population <= window_end,
        )
        .distinct()
        .with_session(session)
        .all()
    )
    if not candidate_dates:
        return None
    return min(
        (candidate_date for (candidate_date,) in candidate_dates),
        key=lambda candidate_date: abs((candidate_date - month_start).days),
    )


def _cache_key_revision() -> str:
    """Returns the Cloud Run revision name, or _NON_CLOUD_RUN_CACHE_REVISION
    outside Cloud Run. Included in _cache_key() so a new deploy (which always
    gets a new Cloud Run revision) invalidates previously cached exports even
    when neither the data nor the requested snapshot has changed.
    """
    if in_cloud_run():
        return CloudRunEnvironment.get_revision_name()
    return _NON_CLOUD_RUN_CACHE_REVISION


def _cache_key(
    *,
    state_code: StateCode,
    last_updated: date,
    year_month: tuple[int, int] | None = None,
) -> str:
    """Returns the individual-level export cache key, scoped to state_code,
    last_updated, year_month (if given), and the current Cloud Run revision (see
    _cache_key_revision()) so each snapshot caches independently and a new
    deploy always produces a fresh key.
    """
    month_suffix = f" {year_month[0]}-{year_month[1]:02d}" if year_month else ""
    return (
        f"{state_code.value} individual_level_export "
        f"{last_updated.isoformat()}{month_suffix} {_cache_key_revision()}"
    )


def _build_csv(
    *,
    session_factory: sessionmaker,
    columns: list[Column],
    label_maps: dict[str, dict[str, str]],
    build_query: Callable[[], Query],
) -> str:
    """Returns the individual-level export as a CSV string: a header row (column
    names, overridden per _EXPORT_COLUMN_HEADER_OVERRIDES) followed by rows from
    build_query(), translated per label_maps.
    """
    column_names = [column.name for column in columns]
    with session_factory() as session:
        query = build_query().with_session(session)

        buffer = io.StringIO()
        writer = csv.writer(buffer)

        writer.writerow(
            [
                _EXPORT_COLUMN_HEADER_OVERRIDES.get(column_name, column_name)
                for column_name in column_names
            ]
        )
        for row in query.yield_per(_ROWS_PER_CHUNK):
            writer.writerow(
                _translate_row(row, column_names=column_names, label_maps=label_maps)
            )

        return buffer.getvalue()


def build_csv_for_date(
    *,
    session_factory: sessionmaker,
    columns: list[Column],
    label_maps: dict[str, dict[str, str]],
    snapshot_date: date,
) -> str:
    """Returns the individual-level export as a CSV string for exactly
    snapshot_date. Used by the bulk multi-month export.
    """
    return _build_csv(
        session_factory=session_factory,
        columns=columns,
        label_maps=label_maps,
        build_query=lambda: build_snapshot_query_for_date(columns, snapshot_date),
    )


def build_individual_level_export_response(
    *,
    state_code: StateCode,
    enabled_states: list[str],
    year_month: tuple[int, int] | None = None,
) -> Response:
    """Returns a CSV response of individual-level Public Pathways data for
    state_code: the most recent snapshot, or the one resolved for year_month if
    given (raising BadRequest if none resolves). Cached in Redis per snapshot.
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
                f"in state [{state_code.value}]; cannot translate the individual-level "
                f"export's columns to display labels."
            )
        label_maps = build_label_maps(dynamic_filter_options)

        snapshot_date: date | None = None
        if year_month is not None:
            year, month = year_month
            snapshot_date = resolve_snapshot_date_for_month(
                session, year=year, month=month
            )
            if snapshot_date is None:
                raise BadRequest(
                    f"No individual-level snapshot found within "
                    f"{_MONTH_SNAPSHOT_WINDOW_DAYS} days of {year}-{month:02d} for "
                    f"state [{state_code.value}]"
                )

    columns = individual_level_export_columns()
    filename = individual_level_export_filename(
        state_code=state_code, last_updated=snapshot_date or last_updated
    )

    build_query = (
        (lambda: build_snapshot_query_for_date(columns, snapshot_date))
        if snapshot_date is not None
        else (lambda: _build_latest_snapshot_query(columns))
    )

    csv_data = cloud_memorystore_utils.get_or_set_str(
        get_public_pathways_metric_redis(),
        _cache_key(
            state_code=state_code, last_updated=last_updated, year_month=year_month
        ),
        lambda: _build_csv(
            session_factory=session_factory,
            columns=columns,
            label_maps=label_maps,
            build_query=build_query,
        ),
        ex=_CACHE_TTL_SECONDS,
    )

    response = Response(csv_data, mimetype="text/csv")
    response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response
