# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Utils for Case Triage"""
import logging
import os
import socket
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from redis import Redis
from redis.backoff import ConstantBackoff
from redis.retry import Retry

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.utils.environment import in_development
from recidiviz.utils.secrets import get_secret

local_path = os.path.join(
    os.path.realpath(os.path.dirname(os.path.realpath(__file__))), "local"
)

CASE_TRIAGE_STATES = {"US_ID"}


def get_local_secret(secret_name: str) -> Optional[str]:
    """
    Helper function for supporting local development flows.
    When in development environments, we fetch file contents from `recidiviz/case_triage/local/gsm`
    In Google Cloud environments, we delegate to Secrets Manager
    """
    if in_development():
        try:
            text = Path(os.path.join(local_path, "gsm", secret_name)).read_text("utf-8")
            return text.strip()
        except OSError:
            logging.error("Couldn't locate secret %s", secret_name)
            return None

    return get_secret(secret_name)


def get_local_file(file_path: GcsfsFilePath) -> str:
    """
    Helper function for supporting local development flows.
    When in development environments, we fetch file contents from `recidiviz/case_triage/local/gcs`
    In Google Cloud environments, we delegate to Cloud Storage
    """

    if in_development():
        return Path(os.path.join(local_path, "gcs", file_path.abs_path())).read_text(
            "utf-8"
        )

    gcs_fs = GcsfsFactory.build()
    return gcs_fs.download_as_string(file_path)


def get_rate_limit_storage_uri() -> str:
    """Reads the rate limit redis secrets; returns an in-memory store if they do not exist"""
    host = get_local_secret("case_triage_rate_limiter_redis_host")
    port = get_local_secret("case_triage_rate_limiter_redis_port")

    if host and port:
        return f"redis://{host}:{port}"

    return "memory://"


REDIS_RETRY_MAX_RETRIES = 3

REDIS_RETRY_BACKOFF_SECONDS = 0.01

REDIS_RETRY_SUPPORTED_ERRORS = (
    ConnectionError,
    ConnectionResetError,
    TimeoutError,
    socket.timeout,
)


def get_redis_connection_options() -> Dict[Any, Any]:
    """
    Returns options to keep the Redis connection alive during prolonged periods of inactivity
    Based off of https://github.com/redis/redis-py/issues/1186#issuecomment-921417396
    https://man7.org/linux/man-pages/man7/tcp.7.html
    """
    if sys.platform == "darwin":
        socket_keepalive_options = {
            socket.TCP_KEEPCNT: 5,
            socket.TCP_KEEPINTVL: 5,
        }
    elif sys.platform == "linux":
        socket_keepalive_options = {
            socket.TCP_KEEPIDLE: 10,
            socket.TCP_KEEPINTVL: 5,
            socket.TCP_KEEPCNT: 5,
        }
    else:
        socket_keepalive_options = {}

    return {
        "health_check_interval": 30,
        "socket_keepalive": True,
        "socket_keepalive_options": socket_keepalive_options,
        "retry_on_error": REDIS_RETRY_SUPPORTED_ERRORS,
        "retry": Retry(
            backoff=ConstantBackoff(backoff=REDIS_RETRY_BACKOFF_SECONDS),
            retries=REDIS_RETRY_MAX_RETRIES,
            supported_errors=REDIS_RETRY_SUPPORTED_ERRORS,
        ),
    }


def get_sessions_redis() -> Optional[Redis]:
    host = get_local_secret("case_triage_sessions_redis_host")
    port = get_local_secret("case_triage_sessions_redis_port")

    if host is None or port is None:
        return None

    return Redis(host=host, port=int(port), **get_redis_connection_options())
