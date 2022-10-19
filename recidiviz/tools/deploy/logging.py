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
"""Helpers for writing logs from deploy scripts"""

import logging
import os
from contextlib import contextmanager
from typing import Generator


def get_deploy_logs_dir() -> str:
    return os.path.join(os.path.dirname(__file__), "logs")


@contextmanager
def redirect_logging_to_file(log_path: str) -> Generator[None, None, None]:
    """Redirects logs from stdout/stderr to a file while the contextmanager is active

    If an exception is raised within this context, the logging will stop being
    redirected and the exception will be raised to the caller.
    """
    folder = os.path.dirname(log_path)
    if not os.path.isdir(folder):
        os.mkdir(folder)

    logger = logging.getLogger()
    prior_handlers = []
    for handler in logger.handlers:
        logger.removeHandler(handler)
        prior_handlers.append(handler)
    file_handler = logging.FileHandler(filename=log_path)
    logger.addHandler(file_handler)

    try:
        yield
    finally:
        logger.removeHandler(file_handler)
        for handler in prior_handlers:
            logger.addHandler(handler)
