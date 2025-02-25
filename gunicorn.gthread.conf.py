# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Configures gunicorn"""
import logging
import multiprocessing

from gunicorn.workers.base import Worker

# Note: if we adjust the number of gunicorn workers per cpu upwards,
# we may have to adjust the number of max connections in our postgres instances.
# See the dicussion in #5497 for more context, and see the docs:
# https://cloud.google.com/sql/docs/quotas#postgresql for more.

# To avoid consuming too much instance memory, limit the number of workers.
# http://docs.gunicorn.org/en/stable/design.html#how-many-workers
workers = (2 * multiprocessing.cpu_count()) + 1
# Use a threaded worker
worker_class = "gthread"
timeout = 3600  # 60 min timeout
loglevel = "debug"
accesslog = "gunicorn-access.log"
errorlog = "gunicorn-error.log"
keepalive = 650


def post_worker_init(worker: Worker) -> None:
    logging.info("Running post_worker_init for worker %s", worker)
    flask_app = worker.app.callable
    # attribute is expected to be defined in `server.py`
    if hasattr(flask_app, "initialize_worker_process"):
        flask_app.initialize_worker_process()
