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
from functools import partial

from google.cloud.logging import Client, handlers

from recidiviz.utils import environment, monitoring


def region_record_factory(default_record_factory, *args, **kwargs):
    record = default_record_factory(*args, **kwargs)

    tags = monitoring.thread_local_tags()
    record.region = tags.map.get(monitoring.TagKey.REGION)

    return record


class StructuredLogFormatter(logging.Formatter):
    def format(self, record):
        labels = {
            'region': record.region,
            'funcName': record.funcName,
            'module': record.module,
            'thread': record.thread,
            'threadName': record.threadName,
        }
        return {
            'text': super(StructuredLogFormatter, self).format(record),
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
