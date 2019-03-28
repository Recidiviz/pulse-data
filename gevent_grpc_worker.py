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
"""Provides a gevent worker that also patches grpc to be asynchronous.

Note: This must be a the top level and called before any recidiviz code so that
nothing is imported prior to being patched. If it is placed inside of the
recidiviz directory, then the __init__.py file will be called first.
"""
from gunicorn.workers.ggevent import GeventWorker
from grpc.experimental import gevent


class GeventGrpcWorker(GeventWorker):
    def patch(self):
        super(GeventGrpcWorker, self).patch()
        gevent.init_gevent()
        self.log.info('patched grpc')
