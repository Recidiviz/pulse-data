# Recidiviz - a platform for tracking granular recidivism metrics in real time
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
        return {
            'text': super(StructuredLogFormatter, self).format(record),
            'region': record.region,

            'funcName': record.funcName,
            'module': record.module,
            'thread': record.thread,
            'threadName': record.threadName,
        }


CLOUD_HANDLERS = (handlers.AppEngineHandler,
                  handlers.CloudLoggingHandler,
                  handlers.ContainerEngineHandler)
def setup():
    logger = logging.getLogger()

    # Set the region on log records.
    default_factory = logging.getLogRecordFactory()
    logging.setLogRecordFactory(partial(region_record_factory, default_factory))

    # Send logs directly via the logging client if possible. This ensures trace
    # ids are propogated and allows us to send structured messages.
    if environment.in_gae():
        client = Client()
        client.setup_logging(log_level=logging.INFO)
        for handler in logger.handlers:
            if not isinstance(handler, CLOUD_HANDLERS):
                logger.removeHandler(handler)
    else:
        logging.basicConfig()

    for handler in logger.handlers:
        # If writing directly to Stackdriver, send a structured message.
        if isinstance(handler, CLOUD_HANDLERS):
            handler.setFormatter(StructuredLogFormatter())
        # Otherwise, the default stream handler requires a string.
        else:
            handler.setFormatter(logging.Formatter(
                "(%(region)s) %(module)s/%(funcName)s : %(message)s"
            ))
