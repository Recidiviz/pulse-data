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
"""Configures logging setup."""

import logging
import threading
from contextlib import contextmanager
from functools import partial, wraps

from google.cloud.logging import Client, handlers

from recidiviz.utils import environment, monitoring

_thread_local = threading.local()

@contextmanager
def push_trace_id():
    # pylint: disable=protected-access
    setattr(_thread_local, 'trace_id',
            handlers._helpers.get_trace_id_from_flask())
    try:
        yield
    finally:
        setattr(_thread_local, 'trace_id', None)


def copy_trace_id_to_thread(func):
    @wraps(func)
    def add_trace_id_and_call(*args, **kwargs):
        with push_trace_id():
            return func(*args, **kwargs)

    return add_trace_id_and_call


def get_trace_id_from_thread():
    return getattr(_thread_local, 'trace_id', None)


def region_record_factory(default_record_factory, *args, **kwargs):
    record = default_record_factory(*args, **kwargs)

    tags = monitoring.thread_local_tags()
    record.region = tags.map.get(monitoring.TagKey.REGION)

    return record


class StructuredLogFormatter(logging.Formatter):
    # Stackdriver log entries have a max size of 100 KiB. If we log more than
    # that in a single entry it will be dropped, so we use 80 KiB to be
    # conservative.
    _MAX_BYTES = 80 * 1024  # 80 KiB
    _SUFFIX = '...truncated'

    def format(self, record):
        text = super(StructuredLogFormatter, self).format(record)
        if len(text) > self._MAX_BYTES:
            logging.warning('Truncated log message')
            text = text[:self._MAX_BYTES - len(self._SUFFIX)] + self._SUFFIX
        labels = {
            'region': record.region,
            'funcName': record.funcName,
            'module': record.module,
            'thread': record.thread,
            'threadName': record.threadName,
        }

        trace_id = get_trace_id_from_thread()
        if trace_id is not None:
            # pylint: disable=protected-access
            labels[handlers.app_enginge._TRACE_ID_LABEL] = trace_id

        return {
            'text': text,
            'labels': {k: str(v) for k, v in labels.items()}
        }


class StructuredAppEngineHandler(handlers.AppEngineHandler):
    def emit(self, record):
        """Overrides the emit method for AppEngineHandler.

        Allows us to put custom information in labels instead of
        jsonPayload.message
        """
        message = super(StructuredAppEngineHandler, self).format(record)
        labels = {**message['labels'], **self.get_gae_labels()}
        # pylint: disable=protected-access
        trace_id = (
            "projects/%s/traces/%s" % (
                self.project_id, labels[handlers.app_engine._TRACE_ID_LABEL])
            if handlers.app_engine._TRACE_ID_LABEL in labels else None
        )
        self.transport.send(
            record, message['text'],
            resource=self.resource, labels=labels, trace=trace_id
        )


def setup():
    logger = logging.getLogger()

    # Set the region on log records.
    default_factory = logging.getLogRecordFactory()
    logging.setLogRecordFactory(partial(region_record_factory, default_factory))

    # Send logs directly via the logging client if possible. This ensures trace
    # ids are propogated and allows us to send structured messages.
    if environment.in_gae():
        client = Client()
        handler = StructuredAppEngineHandler(client)
        handlers.setup_logging(handler, log_level=logging.INFO)
        for handler in logger.handlers:
            if not isinstance(handler, StructuredAppEngineHandler):
                logger.removeHandler(handler)
    else:
        logging.basicConfig()

    for handler in logger.handlers:
        # If writing directly to Stackdriver, send a structured message.
        if isinstance(handler, StructuredAppEngineHandler):
            handler.setFormatter(StructuredLogFormatter())
        # Otherwise, the default stream handler requires a string.
        else:
            handler.setFormatter(logging.Formatter(
                "(%(region)s) %(module)s/%(funcName)s : %(message)s"
            ))
