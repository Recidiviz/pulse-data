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
# Turn this off by default as it may block the cpu itself: gevent/gevent#1665
config.monitor_thread = os.environ.get('MONITOR_THREAD', False)
config.max_blocking_time = 5  # 5 seconds
config.max_memory_usage = 4 * 1024 * 1024 * 1024  # 4 GiB
config.memory_monitor_period = 60  # 1 minute

# TODO(#1467): remove
os.environ['GRPC_DNS_RESOLVER'] = 'native'

MEMORY_DEBUG = os.environ.get('MEMORY_DEBUG', False)

# pylint: disable=wrong-import-position
from gunicorn.workers.ggevent import GeventWorker


class CustomGeventWorker(GeventWorker):
    """Custom gevent worker with extra patching and debugging utilities"""
    def patch(self):
        """Override `patch` so that we can patch some extra libraries.

        Gevent patches base libraries that perform I/O for us already (e.g. `os` and `sockets). We need to patch any
        additional libraries that perform I/O outside of the base libraries in order to make them cooperative.

        See https://docs.gunicorn.org/en/stable/design.html#async-workers and
        https://docs.gunicorn.org/en/stable/design.html#async-workers.

        For a longer discussion of why this is important, see description in #3850.
        """
        # pylint: disable=import-outside-toplevel
        super().patch()

        # patch grpc
        from grpc.experimental import gevent as grpc_gevent
        grpc_gevent.init_gevent()
        self.log.info('patched grpc')

        # patch psycopg2
        from psycogreen.gevent import patch_psycopg
        patch_psycopg()
        self.log.info('patched psycopg2')

    def init_process(self):
        # pylint: disable=import-outside-toplevel
        if MEMORY_DEBUG:
            import tracemalloc
            tracemalloc.start(25)

        # Starts run loop.
        super().init_process()

    def init_signals(self):
        # Leave all signals defined by the superclass in place.
        super().init_signals()

        # Use SIGUSR2 to dump threads and memory usage information.
        signal.signal(signal.SIGUSR2, self.dump_profiles)

        # Don't let it interrupt system calls.
        signal.siginterrupt(signal.SIGUSR2, False)

    def dump_profiles(self, _sig, _frame):
        """Dumps profiles to files in /tmp/

        Trigger with `kill -USR2 <pid>` from within the docker container.
        """
        self._dump_threads()
        self._dump_memory_usage()

    def _dump_threads(self):
        """Dumps run info, including stacks of greenlets and threads, to a file."""
        # pylint: disable=import-outside-toplevel
        self.log.error('received signal, dumping threads')
        from datetime import datetime
        import gevent
        filename = "/tmp/threads.%d.%s" % (os.getpid(), datetime.now().strftime("%d-%m-%Y_%I-%M-%S_%p"))
        try:
            with open(filename, 'w') as f:
                gevent.util.print_run_info(file=f)
            self.log.error("threads dump to %s", filename)
        except Exception as e:
            self.log.error("thread dump failed: %s", str(e))

    def _dump_memory_usage(self):
        """Dumps memory to files in /tmp/
        Inspiration: https://github.com/mozilla-services/mozservices/blob/master/mozsvc/gunicorn_worker.py

        Only will run if MEMORY_DEBUG is enabled.
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
