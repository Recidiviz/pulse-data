# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Helper functions for configuration of pytest tests"""
import os

BQ_EMULATOR_PROJECT_ID = "recidiviz-bq-emulator-project"


def get_pytest_worker_id() -> int:
    """Retrieves the worker number from the appropriate environment variable
    https://github.com/pytest-dev/pytest-xdist#identifying-the-worker-process-during-a-test
    """
    return int(os.environ.get("PYTEST_XDIST_WORKER", "gw0")[2:])


def get_pytest_retry_number() -> int:
    return int()


def get_bq_emulator_port() -> int:
    """Returns the port for the BigQuery emulator that the current environment should
    use. If we are running outside the context of pytest, we assume there is only one
    emulator running at the default port. Otherwise, the port number is variable to
    avoid interference between tests running in parallel.
    """
    # This port is arbitrarily chosen to avoid conflicts services in the RunsOn runner.
    port = 60050
    if not (pytest_worker_id := get_pytest_worker_id()):
        return port

    return port + pytest_worker_id


def get_bq_emulator_grpc_port() -> int:
    """Returns the port for the BigQuery emulator that the current environment should
    use. If we are running outside the context of pytest, we assume there is only one
    emulator running at the default port. Otherwise, the port number is variable to
    avoid interference between tests running in parallel.
    """
    # This port is arbitrarily chosen to avoid conflicts services in the RunsOn runner.
    port = 61050
    if not (pytest_worker_id := get_pytest_worker_id()):
        return port

    return port + pytest_worker_id
