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
import sys

from google.cloud.logging import Client, handlers
from opencensus.common.runtime_context import RuntimeContext
from opencensus.trace import execution_context

from recidiviz.utils import environment, monitoring


# TODO(#3043): Once census-instrumentation/opencensus-python#442 is fixed we can
# use OpenCensus threading intregration which will copy this for us. Until then
# we can just copy it manually.
with_context = RuntimeContext.with_current_context


def region_record_factory(default_record_factory, *args, **kwargs):
    record = default_record_factory(*args, **kwargs)

    tags = monitoring.context_tags()
    record.region = tags.map.get(monitoring.TagKey.REGION)

    context = execution_context.get_opencensus_tracer().span_context
    record.traceId = context.trace_id

    return record


class StructuredLogFormatter(logging.Formatter):
    # Stackdriver log entries have a max size of 100 KiB. If we log more than
    # that in a single entry it will be dropped, so we use 80 KiB to be
    # conservative.
    _MAX_BYTES = 80 * 1024  # 80 KiB
    _SUFFIX = "...truncated"

    def format(self, record):
        text = super().format(record)
        if len(text) > self._MAX_BYTES:
            logging.warning("Truncated log message")
            text = text[: self._MAX_BYTES - len(self._SUFFIX)] + self._SUFFIX
        labels = {
            "region": record.region,
            "funcName": record.funcName,
            "module": record.module,
            "thread": record.thread,
            "threadName": record.threadName,
        }

        if record.traceId is not None:
            # pylint: disable=protected-access
            labels[handlers.app_engine._TRACE_ID_LABEL] = record.traceId

        return {"text": text, "labels": {k: str(v) for k, v in labels.items()}}


class StructuredAppEngineHandler(handlers.AppEngineHandler):
    def emit(self, record):
        """Overrides the emit method for AppEngineHandler.

        Allows us to put custom information in labels instead of
        jsonPayload.message
        """
        message = super().format(record)
        labels = {**message.get("labels", {}), **self.get_gae_labels()}
        # pylint: disable=protected-access
        trace_id = (
            "projects/%s/traces/%s"
            % (self.project_id, labels[handlers.app_engine._TRACE_ID_LABEL])
            if handlers.app_engine._TRACE_ID_LABEL in labels
            else None
        )
        self.transport.send(
            record,
            message["text"],
            resource=self.resource,
            labels=labels,
            trace=trace_id,
        )


def setup():
    """Setup logging"""
    # Set the region on log records.
    default_factory = logging.getLogRecordFactory()
    logging.setLogRecordFactory(partial(region_record_factory, default_factory))

    logger = logging.getLogger()

    # Send logs directly via the logging client if possible. This ensures trace
    # ids are propogated and allows us to send structured messages.
    if environment.in_gcp():
        client = Client()
        handler = StructuredAppEngineHandler(client)
        handlers.setup_logging(handler, log_level=logging.INFO)

        # Streams unstructured logs to stdout - these logs will still show up
        # under the appengine.googleapis.com/stdout Stackdriver logs bucket,
        # even if other logs are stalled on the global interpreter lock or some
        # other issue.
        stdout_handler = logging.StreamHandler(sys.stdout)
        handlers.setup_logging(stdout_handler, log_level=logging.INFO)
        for handler in logger.handlers:
            if not isinstance(
                handler, (StructuredAppEngineHandler, logging.StreamHandler)
            ):
                logger.removeHandler(handler)
    else:
        logging.basicConfig()

    for handler in logger.handlers:
        # If writing directly to Stackdriver, send a structured message.
        if isinstance(handler, StructuredAppEngineHandler):
            handler.setFormatter(StructuredLogFormatter())
        # Otherwise, the default stream handler requires a string.
        else:
            handler.setFormatter(
                logging.Formatter("(%(region)s) %(module)s/%(funcName)s : %(message)s")
            )

    # Export gunicorn errors using the same handlers as other logs, so that they
    # go to Stackdriver in production.
    gunicorn_logger = logging.getLogger("gunicorn.error")
    gunicorn_logger.handlers = logger.handlers
