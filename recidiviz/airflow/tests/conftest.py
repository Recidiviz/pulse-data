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
"""Pytest hooks for airflow tests"""

import os
import shutil
import tempfile

from recidiviz.airflow.tests.test_airflow_config import (
    AIRFLOW_INTEGRATION_TEST_CONFIG_TEMPLATE,
)
from recidiviz.utils.string import StrictStringFormatter

TEST_AIRFLOW_HOME_DIRECTORY: str


def create_airflow_config(directory: str) -> None:
    with open(
        os.path.join(directory, "airflow.cfg"),
        "w",
        encoding="utf-8",
    ) as config_file:
        airflow_config = StrictStringFormatter().format(
            AIRFLOW_INTEGRATION_TEST_CONFIG_TEMPLATE,
            data_dir=directory,
        )
        config_file.write(airflow_config)


def pytest_configure() -> None:
    global TEST_AIRFLOW_HOME_DIRECTORY
    os.environ["_AIRFLOW__AS_LIBRARY"] = "True"
    # Create a temporary directory to store config, logs, etc
    TEST_AIRFLOW_HOME_DIRECTORY = tempfile.mkdtemp(prefix="airflow_")
    os.makedirs(TEST_AIRFLOW_HOME_DIRECTORY, exist_ok=True)
    # Configure environment to point to that directory
    os.environ["AIRFLOW_HOME"] = TEST_AIRFLOW_HOME_DIRECTORY
    create_airflow_config(TEST_AIRFLOW_HOME_DIRECTORY)


def pytest_sessionfinish() -> None:
    shutil.rmtree(TEST_AIRFLOW_HOME_DIRECTORY)
