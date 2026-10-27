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
""" Python automation for Docker, which allows us to start / stop the BigQuery Emulator.

 To start the emulator:
 python -m recidiviz.tests.utils.big_query_emulator_control start

 To stop all running emulators:
 python -m recidiviz.tests.utils.big_query_emulator_control prune_all

 To download the latest image:
 python -m recidiviz.tests.utils.big_query_emulator_control pull_image
 """
import argparse
import logging
import os
import time
from functools import cached_property
from typing import Callable

import attr
from docker import DockerClient, errors
from docker.models.containers import Container

from recidiviz.tests.test_setup_utils import (
    BQ_EMULATOR_PROJECT_ID,
    get_bq_emulator_grpc_port,
    get_bq_emulator_port,
)


@attr.s(auto_attribs=True, frozen=True)
class EmulatorImage:
    repository: str
    version: str

    @cached_property
    def name(self) -> str:
        return f"{self.repository}:{self.version}"


GO_EMULATOR = EmulatorImage(
    repository="ghcr.io/recidiviz/bigquery-emulator",
    version="0.6.6-recidiviz.2.1",
)

ZIGQUERY = EmulatorImage(
    repository="ghcr.io/recidiviz/zigquery",
    version="v0.0.6",
)

# Every emulator we drive announces readiness with this line. The address it
# reports differs between builds — 0.6.6 prints `0.0.0.0:<port>` and 0.8.1
# prints `[::]:<port>` — so match only the part they share.
EMULATOR_READY_LOG_LINE = "REST server listening at"

# Owner read/write, world read, so the container's non-root user can read files
# mounted into it.
_MOUNTED_FILE_MODE = 0o644

# How many trailing log lines to include when the emulator exits during boot.
_EXITED_LOG_TAIL_LINES = 50


class EmulatorExitedError(RuntimeError):
    """Raised when the emulator container exits before it is ready."""


def _exponential_backoff(
    success_condition: Callable[[], bool],
    delay_ms: int,
    exponent: float,
    max_tries: int,
) -> None:
    tries = 0
    while True:
        tries += 1
        if success_condition():
            break

        if tries == max_tries:
            raise TimeoutError

        time.sleep((delay_ms * (exponent**tries)) / 1000)


@attr.s(auto_attribs=True)
class BigQueryEmulatorControl:
    """Tooling for starting the BigQuery emulator via the Docker API"""

    docker_client: DockerClient = attr.ib()
    image: EmulatorImage = attr.ib()
    container: Container | None = attr.ib(default=None)
    name: str = attr.ib(factory=lambda: f"recidiviz-bq-emulator-{os.urandom(15).hex()}")
    port: int = attr.ib(factory=get_bq_emulator_port)
    grpc_port: int = attr.ib(factory=get_bq_emulator_grpc_port)

    def pull_image(self) -> None:
        try:
            self.docker_client.images.get(self.image.name)
        except errors.APIError:
            self.docker_client.images.pull(self.image.repository, self.image.version)

    def start_emulator(self, input_schema_json_path: str | None = None) -> None:
        """Starts the emulator container. Optionally mounts source tables volume"""
        if self.container is not None:
            raise ValueError(
                f"Emulator container [{self.container.name}] is already running"
            )

        base_command = [
            f"--project={BQ_EMULATOR_PROJECT_ID}",
            "--log-level=info",
            "--database=:memory:",
            f"--port={self.port}",
            f"--grpc-port={self.grpc_port}",
        ]

        if self.image == GO_EMULATOR:
            base_command.insert(0, "/bin/bigquery-emulator")

        volumes: dict[str, dict[str, str]] = {}

        if input_schema_json_path:
            # The zigquery image runs as a non-root user, and on Linux a bind
            # mount keeps host permissions, so a file only its host owner can read
            # (e.g. one from tempfile, which creates files 0600) fails to open
            # with AccessDenied.
            os.chmod(input_schema_json_path, _MOUNTED_FILE_MODE)
            file_name = os.path.basename(input_schema_json_path)
            base_command.append(f"--data-from-json=/fixtures/{file_name}")
            volumes = {
                os.path.dirname(input_schema_json_path): {
                    "bind": "/fixtures",
                    "mode": "rw",
                }
            }

        try:
            self.container = self.docker_client.containers.run(
                self.image.name,
                command=base_command,
                name=self.name,
                ports={self.port: self.port, self.grpc_port: self.grpc_port},
                detach=True,
                volumes=volumes,
            )
        except errors.APIError:
            # Docker creates the container before starting it, so a start failure
            # (e.g. the port is already allocated) leaves a created container
            # holding this name, which would block every later start.
            try:
                self.docker_client.containers.get(self.name).remove(force=True)
            except errors.NotFound:
                pass
            raise

        start_time = time.perf_counter()

        try:
            # Exponential backoff, starting at 100ms, ending at 2.3s for a total of ~65 seconds
            _exponential_backoff(
                success_condition=self._is_ready,
                delay_ms=100,
                max_tries=44,
                exponent=1.1,
            )
        except TimeoutError as e:
            self.stop_emulator()
            raise TimeoutError(
                "Expected emulator to start in less than 60 seconds"
            ) from e
        except EmulatorExitedError:
            self.stop_emulator()
            raise

        print(f"Emulator started  in {time.perf_counter() - start_time}s")

    def _is_ready(self) -> bool:
        """Returns whether the emulator has announced readiness. Raises if the
        container exited before doing so, so a crash during boot fails fast with
        its exit details instead of waiting out the full backoff."""
        container = self._get_container()
        logs = self.get_logs()
        if EMULATOR_READY_LOG_LINE in logs:
            return True
        container.reload()
        if container.status != "exited":
            return False
        state = container.attrs["State"]
        raise EmulatorExitedError(
            f"Emulator container [{container.name}] exited during boot with exit "
            f"code [{state['ExitCode']}] (OOMKilled: [{state['OOMKilled']}]). "
            f"Last {_EXITED_LOG_TAIL_LINES} log lines:\n"
            + "\n".join(logs.splitlines()[-_EXITED_LOG_TAIL_LINES:])
        )

    def _get_container(self) -> Container:
        if self.container is None:
            raise ValueError("Expected a running emulator container")
        return self.docker_client.containers.get(self.container.id)

    def get_logs(self) -> str:
        logs = self._get_container().logs()

        if isinstance(logs, bytes):
            return logs.decode("utf-8")
        if isinstance(logs, str):
            return logs

        raise ValueError("expected str or bytes type from Docker logs")

    def is_running(self) -> bool:
        """Returns whether the emulator container is started and has not exited."""
        if self.container is None:
            return False
        container = self._get_container()
        container.reload()
        return container.status == "running"

    def stop_emulator(self) -> None:
        """Kills and removes the emulator container. Containers are not
        auto-removed, so that a container that crashes keeps its logs and exit
        state around for _is_ready to report."""
        if self.container is None:
            return
        try:
            self.docker_client.containers.get(self.container.id).remove(force=True)
        except errors.NotFound:
            # Already removed, e.g. by prune_all or a manual `docker rm`.
            pass
        self.container = None

    def prune_all(self) -> None:
        for container in self.docker_client.containers.list(
            all=True,
            filters={"ancestor": self.image.name},
        ):
            logging.info("Removing container: %s", container.name)
            container.remove(force=True)

    @classmethod
    def build(cls, use_zigquery: bool = True) -> "BigQueryEmulatorControl":
        image = ZIGQUERY if use_zigquery else GO_EMULATOR
        logging.info("Emulator implementation [docker]: %s", image.name)
        return BigQueryEmulatorControl(
            image=image, docker_client=DockerClient.from_env()
        )


ACTION_START = "start"
ACTION_PRUNE = "prune_all"
ACTION_PULL_IMAGE = "pull_image"


def _cli_action(action: str) -> None:
    control = BigQueryEmulatorControl.build()
    if action == ACTION_START:
        control.start_emulator()
    elif action == ACTION_PRUNE:
        control.prune_all()
    elif action == ACTION_PULL_IMAGE:
        control.pull_image()
        logging.info("Pulled image: %s", control.image.name)
    else:
        raise ValueError(f"Unknown action {action}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "action",
        choices=[ACTION_START, ACTION_PULL_IMAGE, ACTION_PRUNE],
    )
    known_args, _ = parser.parse_known_args()
    _cli_action(action=known_args.action)
