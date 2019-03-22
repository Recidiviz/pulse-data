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
import multiprocessing
from gevent_grpc_worker import GeventGrpcWorker

# http://docs.gunicorn.org/en/stable/design.html#how-many-workers
workers = multiprocessing.cpu_count() * 2 + 1
worker_connections = 10000
# Use an asynchronous worker as most of the work is waiting for websites to load
worker_class = '.'.join([GeventGrpcWorker.__module__,
                         GeventGrpcWorker.__name__])
timeout = 600
loglevel = 'debug'
accesslog = 'gunicorn-access.log'
errorlog = 'gunicorn-error.log'
keepalive = 650
