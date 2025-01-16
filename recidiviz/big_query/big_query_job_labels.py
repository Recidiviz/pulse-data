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
"""Utils for working with BigQuery labels"""
from enum import Enum
from typing import List, Optional

import attrs

from recidiviz.common import attr_validators, big_query_attr_validators
from recidiviz.common.constants import platform_logging_strings
from recidiviz.utils import environment


@attrs.define(kw_only=True)
class BigQueryJobLabel:
    key: str = attrs.field(validator=big_query_attr_validators.is_valid_bq_label_key)
    value: str = attrs.field(
        validator=big_query_attr_validators.is_valid_bq_label_value
    )
    parents: Optional[List["BigQueryJobLabel"]] = attrs.field(
        default=None, validator=attr_validators.is_opt_list
    )

    def flatten_to_dictionary(self) -> dict[str, str]:
        parents = (
            coalesce_job_labels(*self.parents, should_throw_on_conflict=True)
            if self.parents
            else {}
        )
        return {
            **parents,
            self.key: self.value,
        }


@attrs.define(kw_only=True)
class BigQueryDatasetIdJobLabel(BigQueryJobLabel):
    key: str = attrs.field(default=platform_logging_strings.DATASET_ID)


@attrs.define(kw_only=True)
class BigQueryAddressJobLabel(BigQueryJobLabel):
    key: str = attrs.field(default=platform_logging_strings.BIG_QUERY_ADDRESS)


@attrs.define(kw_only=True)
class BigQueryTableIdJobLabel(BigQueryJobLabel):
    key: str = attrs.field(default=platform_logging_strings.TABLE_ID)


@attrs.define(kw_only=True)
class BigQueryStateCodeJobLabel(BigQueryJobLabel):
    key: str = attrs.field(default=platform_logging_strings.STATE_CODE)


@attrs.define(kw_only=True)
class BigQueryIngestInstanceJobLabel(BigQueryJobLabel):
    key: str = attrs.field(default=platform_logging_strings.INGEST_INSTANCE)


@attrs.define(kw_only=True)
class BigQueryStateAgnosticJobLabel(BigQueryJobLabel):
    key: str = attrs.field(default=platform_logging_strings.STATE_CODE)
    value: str = attrs.field(default=platform_logging_strings.STATE_AGNOSTIC)


class PlatformEnvironmentBQLabel(Enum):
    """Enum of BQ job labels with the "platform_environment" key. This is meant to
    represent the execution environment where the BQ job launched.
    """

    AIRFLOW = BigQueryJobLabel(
        key=platform_logging_strings.PLATFORM_ENVIRONMENT,
        value=platform_logging_strings.AIRFLOW,
    )
    DATAFLOW = BigQueryJobLabel(
        key=platform_logging_strings.PLATFORM_ENVIRONMENT,
        value=platform_logging_strings.DATAFLOW,
    )
    KUBERNETES = BigQueryJobLabel(
        key=platform_logging_strings.PLATFORM_ENVIRONMENT,
        value=platform_logging_strings.KUBERNETES,
    )
    CLOUD_RUN = BigQueryJobLabel(
        key=platform_logging_strings.PLATFORM_ENVIRONMENT,
        value=platform_logging_strings.CLOUD_RUN,
    )
    APP_ENGINE = BigQueryJobLabel(
        key=platform_logging_strings.PLATFORM_ENVIRONMENT,
        value=platform_logging_strings.APP_ENGINE,
    )
    LOCAL_MACHINE = BigQueryJobLabel(
        key=platform_logging_strings.PLATFORM_ENVIRONMENT,
        value=platform_logging_strings.LOCAL_MACHINE,
    )

    @classmethod
    def for_current_env(cls) -> BigQueryJobLabel:
        """Derives the platform_environment label from the current env, returning
        "unknown" if we cannot determine the value.
        """
        if environment.in_cloud_run():
            return cls.CLOUD_RUN.value

        if environment.in_dataflow_worker():
            return cls.DATAFLOW.value

        if environment.in_airflow_kubernetes_pod():
            return cls.KUBERNETES.value

        if environment.in_airflow():
            return cls.AIRFLOW.value

        if environment.in_app_engine():
            return cls.APP_ENGINE.value

        if not environment.in_gcp():
            return cls.LOCAL_MACHINE.value

        return BigQueryJobLabel(
            key=platform_logging_strings.PLATFORM_ENVIRONMENT,
            value=platform_logging_strings.UNKNOWN,
        )


class PlatformOrchestrationBQLabel(Enum):
    """Enum of BQ job labels with the "platform_orchestration_mechanism" key. This is
    meant to represent the process responsible for scheduling and running the process
    that launched the BQ job.
    """

    RAW_DATA_IMPORT_DAG = BigQueryJobLabel(
        key=platform_logging_strings.PLATFORM_ORCHESTRATION_MECHANISM,
        value=platform_logging_strings.RAW_DATA_IMPORT_DAG,
    )
    CALCULATION_DAG = BigQueryJobLabel(
        key=platform_logging_strings.PLATFORM_ORCHESTRATION_MECHANISM,
        value=platform_logging_strings.CALCULATION_DAG,
    )
    LOCAL_SCRIPT = BigQueryJobLabel(
        key=platform_logging_strings.PLATFORM_ORCHESTRATION_MECHANISM,
        value=platform_logging_strings.LOCAL_SCRIPT,
    )


class RawDataImportStepBQLabel(Enum):
    """Enum of BQ job labels with the "raw_data_import_step" key. This is meant to represent
    conceptual phases during the raw data import process.
    """

    RAW_DATA_TEMP_LOAD = BigQueryJobLabel(
        key=platform_logging_strings.RAW_DATA_IMPORT_STEP,
        value=platform_logging_strings.RAW_DATA_TEMP_LOAD,
    )
    RAW_DATA_PRE_IMPORT_TRANSFORMATIONS = BigQueryJobLabel(
        key=platform_logging_strings.RAW_DATA_IMPORT_STEP,
        value=platform_logging_strings.RAW_DATA_PRE_IMPORT_TRANSFORMATIONS,
    )
    RAW_DATA_MIGRATIONS = BigQueryJobLabel(
        key=platform_logging_strings.RAW_DATA_IMPORT_STEP,
        value=platform_logging_strings.RAW_DATA_MIGRATIONS,
    )
    RAW_DATA_PRE_IMPORT_VALIDATIONS = BigQueryJobLabel(
        key=platform_logging_strings.RAW_DATA_IMPORT_STEP,
        value=platform_logging_strings.RAW_DATA_PRE_IMPORT_VALIDATIONS,
    )
    RAW_DATA_PRUNING = BigQueryJobLabel(
        key=platform_logging_strings.RAW_DATA_IMPORT_STEP,
        value=platform_logging_strings.RAW_DATA_PRUNING,
    )
    RAW_DATA_TABLE_APPEND = BigQueryJobLabel(
        key=platform_logging_strings.RAW_DATA_IMPORT_STEP,
        value=platform_logging_strings.RAW_DATA_TABLE_APPEND,
    )


class AirflowDAGPhaseBQLabel(Enum):
    """Enum of BQ job labels with the "airflow_dag_phase" key. This is meant to represent
    conceptual phases that are orchestrated by an airflow dag.
    """

    BQ_REFRESH = BigQueryJobLabel(
        key=platform_logging_strings.AIRFLOW_DAG_PHASE,
        value=platform_logging_strings.BQ_REFRESH,
        parents=[PlatformOrchestrationBQLabel.CALCULATION_DAG.value],
    )
    VIEW_UPDATE = BigQueryJobLabel(
        key=platform_logging_strings.AIRFLOW_DAG_PHASE,
        value=platform_logging_strings.VIEW_UPDATE,
        parents=[PlatformOrchestrationBQLabel.CALCULATION_DAG.value],
    )
    VALIDATIONS = BigQueryJobLabel(
        key=platform_logging_strings.AIRFLOW_DAG_PHASE,
        value=platform_logging_strings.VALIDATIONS,
        parents=[PlatformOrchestrationBQLabel.CALCULATION_DAG.value],
    )
    METRIC_EXPORTS = BigQueryJobLabel(
        key=platform_logging_strings.AIRFLOW_DAG_PHASE,
        value=platform_logging_strings.METRIC_EXPORTS,
        parents=[PlatformOrchestrationBQLabel.CALCULATION_DAG.value],
    )


def coalesce_job_labels(
    *labels: BigQueryJobLabel, should_throw_on_conflict: bool
) -> dict[str, str]:
    """Given a sequence of |labels|, returns the union of all BigQuery job labels as a
    dictionary, optionally throwing on conflict, otherwise selecting the value that
    comes first in |labels|.
    """
    job_labels: dict[str, str] = {}
    for label in labels:
        for key, value in label.flatten_to_dictionary().items():
            if key in job_labels and value != job_labels[key]:
                if should_throw_on_conflict:
                    raise ValueError(
                        f"Found conflicting labels for key [{key}]: [{value}] and [{job_labels[key]}]"
                    )
                continue

            job_labels[key] = value

    return job_labels
