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
"""Airflow environment age timeliness monitoring"""
import logging
from typing import Iterable

from google.cloud.orchestration.airflow import service_v1
from opentelemetry.metrics import CallbackOptions, Observation
from proto.datetime_helpers import DatetimeWithNanoseconds

from recidiviz.monitoring.instruments import get_monitoring_instrument
from recidiviz.monitoring.keys import AttributeKey, ObservableGaugeInstrumentKey
from recidiviz.utils import metadata
from recidiviz.utils.environment import gcp_only
from recidiviz.utils.types import assert_type


def get_airflow_environment_ages(
    callback_options: CallbackOptions,
) -> Iterable[Observation]:
    """Collects Composer environment age and creates measurements;
    referenced by AIRFLOW_ENVIRONMENT_AGE in monitoring_instruments.yaml"""
    logging.info("Getting Airflow environment age with options: %s", callback_options)

    client = service_v1.EnvironmentsClient()

    for environment in client.list_environments(
        request=service_v1.ListEnvironmentsRequest(
            parent=f"projects/{metadata.project_id()}/locations/us-central1"
        )
    ):
        name, age = environment.name, round(
            assert_type(environment.create_time, DatetimeWithNanoseconds).timestamp()
        )
        logging.info("Airflow instance age: %s %s", name, age)

        attributes = {
            AttributeKey.AIRFLOW_ENVIRONMENT_NAME: name,
        }

        yield Observation(value=age, attributes=attributes)


@gcp_only
def report_airflow_environment_age_metrics() -> None:
    """Collects Airflow environment age; used for alerting on unused experiment environments"""
    # Load the instrument and run the callback configured in `monitoring_instruments.yaml`
    get_monitoring_instrument(ObservableGaugeInstrumentKey.AIRFLOW_ENVIRONMENT_AGE)
