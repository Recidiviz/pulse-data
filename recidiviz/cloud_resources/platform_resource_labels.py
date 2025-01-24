# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Common resource labels used across our data platform."""

from enum import Enum

import attrs

from recidiviz.cloud_resources.resource_label import ResourceLabel
from recidiviz.common.constants import platform_logging_strings
from recidiviz.utils import environment


class PlatformEnvironmentResourceLabel(Enum):
    """Enum of resource labels with the "platform_environment" key. This is meant to
    represent the execution environment where the resource launched.
    """

    AIRFLOW = ResourceLabel(
        key=platform_logging_strings.PLATFORM_ENVIRONMENT,
        value=platform_logging_strings.AIRFLOW,
    )
    DATAFLOW = ResourceLabel(
        key=platform_logging_strings.PLATFORM_ENVIRONMENT,
        value=platform_logging_strings.DATAFLOW,
    )
    KUBERNETES = ResourceLabel(
        key=platform_logging_strings.PLATFORM_ENVIRONMENT,
        value=platform_logging_strings.KUBERNETES,
    )
    CLOUD_RUN = ResourceLabel(
        key=platform_logging_strings.PLATFORM_ENVIRONMENT,
        value=platform_logging_strings.CLOUD_RUN,
    )
    APP_ENGINE = ResourceLabel(
        key=platform_logging_strings.PLATFORM_ENVIRONMENT,
        value=platform_logging_strings.APP_ENGINE,
    )
    LOCAL_MACHINE = ResourceLabel(
        key=platform_logging_strings.PLATFORM_ENVIRONMENT,
        value=platform_logging_strings.LOCAL_MACHINE,
    )

    @classmethod
    def for_current_env(cls) -> ResourceLabel:
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

        return ResourceLabel(
            key=platform_logging_strings.PLATFORM_ENVIRONMENT,
            value=platform_logging_strings.UNKNOWN,
        )


class PlatformOrchestrationResourceLabel(Enum):
    """Enum of resource labels with the "platform_orchestration_mechanism" key. This is
    meant to represent the process responsible for scheduling and running the process
    that launched the resource.
    """

    RAW_DATA_IMPORT_DAG = ResourceLabel(
        key=platform_logging_strings.PLATFORM_ORCHESTRATION_MECHANISM,
        value=platform_logging_strings.RAW_DATA_IMPORT_DAG,
    )
    CALCULATION_DAG = ResourceLabel(
        key=platform_logging_strings.PLATFORM_ORCHESTRATION_MECHANISM,
        value=platform_logging_strings.CALCULATION_DAG,
    )
    LOCAL_SCRIPT = ResourceLabel(
        key=platform_logging_strings.PLATFORM_ORCHESTRATION_MECHANISM,
        value=platform_logging_strings.LOCAL_SCRIPT,
    )


class RawDataImportStepResourceLabel(Enum):
    """Enum of resource job labels with the "raw_data_import_step" key. This is meant to
    represent conceptual phases during the raw data import process.
    """

    RAW_DATA_TEMP_LOAD = ResourceLabel(
        key=platform_logging_strings.RAW_DATA_IMPORT_STEP,
        value=platform_logging_strings.RAW_DATA_TEMP_LOAD,
    )
    RAW_DATA_PRE_IMPORT_TRANSFORMATIONS = ResourceLabel(
        key=platform_logging_strings.RAW_DATA_IMPORT_STEP,
        value=platform_logging_strings.RAW_DATA_PRE_IMPORT_TRANSFORMATIONS,
    )
    RAW_DATA_MIGRATIONS = ResourceLabel(
        key=platform_logging_strings.RAW_DATA_IMPORT_STEP,
        value=platform_logging_strings.RAW_DATA_MIGRATIONS,
    )
    RAW_DATA_PRE_IMPORT_VALIDATIONS = ResourceLabel(
        key=platform_logging_strings.RAW_DATA_IMPORT_STEP,
        value=platform_logging_strings.RAW_DATA_PRE_IMPORT_VALIDATIONS,
    )
    RAW_DATA_PRUNING = ResourceLabel(
        key=platform_logging_strings.RAW_DATA_IMPORT_STEP,
        value=platform_logging_strings.RAW_DATA_PRUNING,
    )
    RAW_DATA_TABLE_APPEND = ResourceLabel(
        key=platform_logging_strings.RAW_DATA_IMPORT_STEP,
        value=platform_logging_strings.RAW_DATA_TABLE_APPEND,
    )


class AirflowDAGPhaseResourceLabel(Enum):
    """Enum of resource job labels with the "airflow_dag_phase" key. This is meant to
    represent conceptual phases that are orchestrated by an airflow dag.
    """

    BQ_REFRESH = ResourceLabel(
        key=platform_logging_strings.AIRFLOW_DAG_PHASE,
        value=platform_logging_strings.BQ_REFRESH,
        parents=[PlatformOrchestrationResourceLabel.CALCULATION_DAG.value],
    )
    VIEW_UPDATE = ResourceLabel(
        key=platform_logging_strings.AIRFLOW_DAG_PHASE,
        value=platform_logging_strings.VIEW_UPDATE,
        parents=[PlatformOrchestrationResourceLabel.CALCULATION_DAG.value],
    )
    VALIDATIONS = ResourceLabel(
        key=platform_logging_strings.AIRFLOW_DAG_PHASE,
        value=platform_logging_strings.VALIDATIONS,
        parents=[PlatformOrchestrationResourceLabel.CALCULATION_DAG.value],
    )
    METRIC_EXPORTS = ResourceLabel(
        key=platform_logging_strings.AIRFLOW_DAG_PHASE,
        value=platform_logging_strings.METRIC_EXPORTS,
        parents=[PlatformOrchestrationResourceLabel.CALCULATION_DAG.value],
    )


@attrs.define(kw_only=True)
class StateCodeResourceLabel(ResourceLabel):
    key: str = attrs.field(default=platform_logging_strings.STATE_CODE)


@attrs.define(kw_only=True)
class StateAgnosticResourceLabel(ResourceLabel):
    key: str = attrs.field(default=platform_logging_strings.STATE_CODE)
    value: str = attrs.field(default=platform_logging_strings.STATE_AGNOSTIC)


@attrs.define(kw_only=True)
class IngestInstanceResourceLabel(ResourceLabel):
    key: str = attrs.field(default=platform_logging_strings.INGEST_INSTANCE)
