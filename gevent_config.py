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
"""Provides configuration for gevent and a custom worker that patches grpc to be asynchronous.

Note: This must be at the top level and called before any recidiviz code so that
nothing is imported prior to being patched. If it is placed inside of the
recidiviz directory, then the __init__.py file will be called first.
"""

import os
import signal

from gevent import config
config.monitor_thread = True
config.max_blocking_time = 1  # 1 second
config.max_memory_usage = 4 * 1024 * 1024 * 1024  # 4 GiB
config.memory_monitor_period = 60  # 1 minute

# TODO(#1467): remove
os.environ['GRPC_DNS_RESOLVER'] = 'native'

MEMORY_DEBUG = os.environ.get('MEMORY_DEBUG', False)

# pylint: disable=wrong-import-position
from gunicorn.workers.ggevent import GeventWorker
from grpc.experimental import gevent


class GeventGrpcWorker(GeventWorker):
    """Custom gevent worker with extra patching and debugging utilities"""
    def patch(self):
        super(GeventGrpcWorker, self).patch()
        gevent.init_gevent()
        self.log.info('patched grpc')

    def init_process(self):
        # pylint: disable=import-outside-toplevel
        if MEMORY_DEBUG:
            import tracemalloc
            tracemalloc.start(25)

        # Starts run loop.
        super(GeventGrpcWorker, self).init_process()

    def init_signals(self):
        # Leave all signals defined by the superclass in place.
        super(GeventGrpcWorker, self).init_signals()

        # Use SIGUSR2 to dump memory usage information.
        signal.signal(signal.SIGUSR2, self.dump_memory_usage)

        # Don't let it interrupt system calls.
        signal.siginterrupt(signal.SIGUSR2, False)

    def dump_memory_usage(self, _sig, _frame):
        """Dumps memory to files in /tmp/
        Inspiration: https://github.com/mozilla-services/mozservices/blob/master/mozsvc/gunicorn_worker.py

        Only will run if MEMORY_DEBUG is enabled.

        Trigger with `kill -USR2 <pid>` from within the docker container.
        """
        # pylint: disable=import-outside-toplevel
        self.log.error('received signal, dumping memory')
        from datetime import datetime
        import tracemalloc
        if not tracemalloc.is_tracing():
            self.log.error('tracemalloc not enabled, will not dump memory')
            return

        filename = "/tmp/memdump.tracemalloc.%d.%s" % (os.getpid(), datetime.now().strftime("%d-%m-%Y_%I-%M-%S_%p"))
        try:
            snapshot = tracemalloc.take_snapshot()
            snapshot.dump(filename)
            self.log.error("tracemalloc dump to %s", filename)
        except Exception as e:
            self.log.error("tracemalloc failed: %s", str(e))
