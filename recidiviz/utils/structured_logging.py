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
import sys
from types import TracebackType
from typing import Any, Dict, Optional, Tuple, Union

from google.cloud.logging import Client, handlers
from opencensus.common.runtime_context import RuntimeContext
from opencensus.trace import execution_context

from recidiviz.utils import environment, metadata, monitoring

# TODO(#3043): Once census-instrumentation/opencensus-python#442 is fixed we can
# use OpenCensus threading intregration which will copy this for us. Until then
# we can just copy it manually.
with_context = RuntimeContext.with_current_context


class ContextualLogRecord(logging.LogRecord):
    """Fetches context from when the record was produced and adds it to the record.

    This must happen when the record is produced, not during formatting or emitting
    as those may happen asynchronously on a separate thread with different
    context.
    """

    def __init__(
        self,
        name: str,
        level: int,
        pathname: str,
        lineno: int,
        msg: str,
        args: Tuple[Any, ...],
        exc_info: Union[
            Tuple[type, BaseException, Optional[TracebackType]],
            Tuple[None, None, None],
            None,
        ],
        func: str = None,
        sinfo: str = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            name,
            level,
            pathname,
            lineno,
            msg,
            args,
            exc_info,
            func=func,
            sinfo=sinfo,
            # Skip kwargs, they are unused and mypy complains.
        )

        tags = monitoring.context_tags()
        self.region = tags.map.get(monitoring.TagKey.REGION)
        self.ingest_instance = tags.map.get(monitoring.TagKey.INGEST_INSTANCE)

        context = execution_context.get_opencensus_tracer().span_context
        self.traceId = context.trace_id


# Stackdriver log entries have a max size of 100 KiB. If we log more than
# that in a single entry it will be dropped, so we use 80 KiB to be
# conservative.
_MAX_BYTES = 80 * 1024  # 80 KiB
_SUFFIX = "...truncated"
BEFORE_REQUEST_LOG = "before_request_log"


def _truncate_if_needed(text: str) -> str:
    if len(text) > _MAX_BYTES:
        logging.warning("Truncated log message")
        text = text[: _MAX_BYTES - len(_SUFFIX)] + _SUFFIX
    return text


def _labels_for_record(record: logging.LogRecord) -> Dict[str, str]:
    labels = {
        "func_name": record.funcName,
        "module": record.module,
        "thread": record.thread,
        "thread_name": record.threadName,
        "process_id": record.process,
        "process_name": record.processName,
    }

    if isinstance(record, ContextualLogRecord):
        labels["region"] = record.region
        labels["ingest_instance"] = record.ingest_instance
        # pylint: disable=protected-access
        labels[handlers.app_engine._TRACE_ID_LABEL] = record.traceId

    return {k: str(v) for k, v in labels.items()}


_GAE_DOMAIN = "appengine.googleapis.com"
_GCE_DOMAIN = "compute.googleapis.com"


class StructuredAppEngineHandler(handlers.AppEngineHandler):
    """
    Customized AppEngineHandler to allow for additional information in jsonPayload.message
    """

    def __init__(self, client: Client, name: Optional[str] = None) -> None:
        if name:
            super().__init__(client, name=name)
        else:
            super().__init__(client)

    def emit(self, record: logging.LogRecord) -> None:
        """Overrides the emit method for AppEngineHandler.

        Allows us to put custom information in labels instead of
        jsonPayload.message
        """
        labels = _labels_for_record(record)
        instance_name = metadata.instance_name()
        if instance_name:
            labels.update({"/".join([_GAE_DOMAIN, "instance_name"]): instance_name})
        instance_id = metadata.instance_id()
        if instance_id:
            labels.update({"/".join([_GCE_DOMAIN, "resource_id"]): instance_id})
        zone = metadata.zone()
        if zone:
            labels.update({"/".join([_GCE_DOMAIN, "zone"]): zone})
        labels.update(self.get_gae_labels())

        # pylint: disable=protected-access
        trace_id = "projects/%s/traces/%s" % (
            self.project_id,
            labels.get(handlers.app_engine._TRACE_ID_LABEL, None),
        )
        self.transport.send(
            record,
            _truncate_if_needed(super().format(record)),
            resource=self.resource,
            labels=labels,
            trace=trace_id,
        )


def setup() -> None:
    """Setup logging"""
    # Set the region on log records.
    logging.setLogRecordFactory(ContextualLogRecord)
    logger = logging.getLogger()

    # Send logs directly via the logging client if possible. This ensures trace
    # ids are propagated and allows us to send structured messages.
    if environment.in_gcp():
        client = Client()
        structured_handler = StructuredAppEngineHandler(client)
        handlers.setup_logging(structured_handler, log_level=logging.INFO)

        before_request_handler = StructuredAppEngineHandler(
            client, name=BEFORE_REQUEST_LOG
        )
        logging.getLogger(BEFORE_REQUEST_LOG).addHandler(before_request_handler)

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
        # If we aren't writing directly to Stackdriver, prefix the log with important
        # context that would be in the labels.
        if not isinstance(handler, StructuredAppEngineHandler):
            handler.setFormatter(
                logging.Formatter(
                    "[pid: %(process)d] (%(region)s) %(module)s/%(funcName)s : %(message)s"
                )
            )

    # Export gunicorn errors using the same handlers as other logs, so that they
    # go to Stackdriver in production.
    gunicorn_logger = logging.getLogger("gunicorn.error")
    gunicorn_logger.handlers = logger.handlers
