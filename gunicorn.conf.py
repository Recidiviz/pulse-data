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
import gevent_config

# Note: if we adjust the number of gunicorn workers per cpu upwards,
# we may have to adjust the number of max connections in our postgres instances.
# See the dicussion in #5497 for more context, and see the docs:
# https://cloud.google.com/sql/docs/quotas#postgresql for more.

# To avoid consuming too much instance memory, limit the number of workers.
# http://docs.gunicorn.org/en/stable/design.html#how-many-workers
workers = multiprocessing.cpu_count() + 1
worker_connections = 10000
# Use an asynchronous worker as most of the work is waiting for websites to load
worker_class = '.'.join([gevent_config.CustomGeventWorker.__module__,
                         gevent_config.CustomGeventWorker.__name__])
timeout = 3600  # 60 min timeout
loglevel = 'debug'
accesslog = 'gunicorn-access.log'
keepalive = 650
