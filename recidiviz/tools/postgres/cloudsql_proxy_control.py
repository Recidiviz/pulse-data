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
""" Python bindings for cloudsql_proxy_control.sh, which allows us to start / stop the Cloud SQL Proxy """
import atexit
import contextlib
import logging
import os
from typing import Generator, Optional

import attr

from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    CLOUDSQL_PROXY_MIGRATION_PORT,
    SQLAlchemyEngineManager,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation, run_command

script_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "cloudsql_proxy_control.sh")
)


@attr.s(auto_attribs=True)
class CloudSQLProxyControl:
    """Bindings for `cloudsql_proxy_control.sh`. Exposes context manager under `.connection()`"""

    port: int

    def _start_proxy(
        self,
        *,
        schema_type: SchemaType,
        prompt: Optional[bool] = True,
        secret_prefix_override: Optional[str],
    ) -> str:
        connection_string = SQLAlchemyEngineManager.get_full_cloudsql_instance_id(
            schema_type=schema_type, secret_prefix_override=secret_prefix_override
        )

        if prompt:
            prompt_for_confirmation(
                f"This will start the Cloud SQL proxy for {connection_string}.",
                connection_string,
            )

        connection_args = ["-c", connection_string]

        # Verify that the proxy can be run
        self._run_command([*connection_args, "-d"])

        # Run the proxy
        return self._run_command(connection_args)

    def _quit_proxy(self) -> str:
        return self._run_command(["-q"])

    def _run_command(self, additional_flags: list[str]) -> str:
        output = run_command(
            f"{script_path} -p {self.port} {' '.join(additional_flags)}"
        )

        if output:
            for line in output.splitlines():
                logging.info(line.rstrip())

        return output

    @contextlib.contextmanager
    def connection(
        self,
        *,
        schema_type: SchemaType,
        prompt: Optional[bool] = None,
        secret_prefix_override: Optional[str] = None,
    ) -> Generator[None, None, None]:
        atexit.register(self._quit_proxy)
        self._start_proxy(
            schema_type=schema_type,
            prompt=prompt,
            secret_prefix_override=secret_prefix_override,
        )

        try:
            yield
        finally:
            atexit.unregister(self._quit_proxy)
            self._quit_proxy()

    @contextlib.contextmanager
    def connection_for_instance(
        self,
        *,
        connection_string: str,
        prompt: Optional[bool] = None,
    ) -> Generator[None, None, None]:
        """Like connection(), but accepts a Cloud SQL instance connection string
        directly (e.g. 'project:region:instance') instead of resolving one from
        a SchemaType and Secret Manager."""
        if prompt:
            prompt_for_confirmation(
                f"This will start the Cloud SQL proxy for {connection_string}.",
                connection_string,
            )

        connection_args = ["-c", connection_string]

        # Verify that the proxy can be run
        self._run_command([*connection_args, "-d"])

        atexit.register(self._quit_proxy)
        # Run the proxy
        self._run_command(connection_args)

        try:
            yield
        finally:
            atexit.unregister(self._quit_proxy)
            self._quit_proxy()


cloudsql_proxy_control = CloudSQLProxyControl(port=CLOUDSQL_PROXY_MIGRATION_PORT)
