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
"""Contains enums for our instruments that are registered in `monitoring_instruments.yaml`"""
import enum


class InstrumentEnum(enum.Enum):
    pass


class ObservableGaugeInstrumentKey(InstrumentEnum):
    """Instruments registered here will be created as opentelemetry.sdk.metrics.ObservableGauge
    and use the `LastValueAggregation` by default
    See: https://opentelemetry-python.readthedocs.io/en/latest/sdk/metrics.view.html"""

    EXPORT_FILE_AGE = "bigquery.metric_view_export_manager.export_file_age"


class HistogramInstrumentKey(InstrumentEnum):
    """Instruments registered here will be created as a opentelemetry.sdk.metrics.Histogram and
    use the `ExplicitBucketHistogramAggregation` by default
    See: https://opentelemetry-python.readthedocs.io/en/latest/sdk/metrics.view.html"""

    FUNCTION_DURATION = "function_duration"


class CounterInstrumentKey(InstrumentEnum):
    """Instruments registered here will be created as opentelemetry.sdk.metrics.Counter
    and use the `SumAggregation` by default
    See: https://opentelemetry-python.readthedocs.io/en/latest/sdk/metrics.view.html"""

    VIEW_UPDATE_FAILURE = "bigquery.view_update_manager.num_view_update_failure"
    VIEW_EXPORT_VALIDATION_FAILURE = (
        "bigquery.metric_view_export_manager.num_metric_view_export_validation_failure"
    )
    VIEW_EXPORT_JOB_FAILURE = (
        "bigquery.metric_view_export_manager.num_metric_view_export_job_failure"
    )
    VALIDATION_FAILURE_TO_RUN = "validation.num_fail_to_run"
    VALIDATION_FAILURE = "validation.num_failure"

    ENGINE_INITIALIZATION_FAILURE = (
        "persistence.database.sqlalchemy_engine_initialization_failure"
    )


class AttributeKey:
    """Scope to hold key constants for attributes added to our traces / metrics"""

    REGION = "region"

    # Ingest related attributes
    INGEST_INSTANCE = "ingest_instance"
    INGEST_VIEW_MATERIALIZATION_TAG = "ingest_view_materialization_tag"

    # Bigquery related attributes
    VALIDATION_CHECK_TYPE = "validation_check_type"
    VALIDATION_VIEW_ID = "validation_view_id"
    CREATE_UPDATE_VIEWS_NAMESPACE = "create_update_views_namespace"
    METRIC_VIEW_EXPORT_NAME = "metric_view_export_name"

    # Postgres related attributes
    SCHEMA_TYPE = "schema_type"
    DATABASE_NAME = "database_name"

    # Code related attributes
    FUNCTION = "function"
    MODULE = "module"
    ARGS = "function_args"
    KWARGS = "function_kwargs"
    RECURSION_DEPTH = "recursion_depth"

    # Export related attributes
    EXPORT_FILE = "export_file"


def build_instrument_key(instrument_name: str) -> InstrumentEnum:
    for Key in InstrumentEnum.__subclasses__():
        try:
            return Key(instrument_name)
        except ValueError:
            pass

    raise ValueError(
        f"Unknown instrument: {instrument_name}; is it registered in a recidiviz.monitoring.keys enum?"
    )
