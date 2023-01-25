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

# https://docs.gunicorn.org/en/stable/design.html#choosing-a-worker-type
# http://docs.gunicorn.org/en/stable/design.html#how-many-workers
# Using the default synchronous worker because:
# 1) Asynchronous workers are difficult to configure correctly, and our usage
#    is low enough that the risks out weight the benefits
# 2) We want to use Sentry to profile the API and profiling breaks with greenlets
#    (https://greenlet.readthedocs.io/en/latest/tracing.html)
# Using the recommended (2 x $num_cores) + 1 number of workers
workers = 2 * multiprocessing.cpu_count() + 1
timeout = 3600  # 60 min timeout
loglevel = "debug"
accesslog = "gunicorn-access.log"
keepalive = 650
