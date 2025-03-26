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
from functools import wraps
from types import TracebackType
from typing import Any, Callable, Dict, Optional, Tuple, Type, Union

from google.cloud.logging import Client, Resource, handlers
from google.cloud.logging_v2.handlers._monitored_resources import detect_resource
from opentelemetry import context
from opentelemetry.baggage import get_baggage

from recidiviz.monitoring.context import get_current_trace_id
from recidiviz.monitoring.keys import AttributeKey
from recidiviz.utils import environment


def with_context(func: Callable) -> Callable:
    current_context = context.get_current()

    @wraps(func)
    def _wrapper(*args: Any, **kwargs: Any) -> Any:
        token = context.attach(current_context)
        result = func(*args, **kwargs)
        context.detach(token)
        return result

    return _wrapper


class ContextualLogRecord(logging.LogRecord):
    """Fetches context from when the record was produced and adds it to the record.

    This must happen when the record is produced, not during formatting or emitting
    as those may happen asynchronously on a separate thread with different
    context.
    """

    # pylint: disable=too-many-positional-arguments
    def __init__(
        self,
        name: str,
        level: int,
        pathname: str,
        lineno: int,
        msg: str,
        args: Tuple[Any, ...],
        exc_info: Union[
            Tuple[Type[BaseException], BaseException, Optional[TracebackType]],
            Tuple[None, None, None],
            None,
        ],
        func: Optional[str] = None,
        sinfo: Optional[str] = None,
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

        self.region = str(get_baggage(AttributeKey.REGION))
        self.ingest_instance = str(get_baggage(AttributeKey.INGEST_INSTANCE))

        try:
            self.traceId = get_current_trace_id()
        except KeyError:
            self.traceId = ""


RECIDIVIZ_BEFORE_REQUEST_LOG = "before_request_log"


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
    return {k: str(v) for k, v in labels.items()}


_GCE_DOMAIN = "compute.googleapis.com"


def setup() -> None:
    """Setup logging"""
    # Set the region on log records.
    logging.setLogRecordFactory(ContextualLogRecord)
    logger = logging.getLogger()

    # Send logs directly via the logging client if possible. This ensures trace
    # ids are propagated and allows us to send structured messages.
    if environment.in_gcp():
        client = Client()

        label_filter: Optional[logging.Filter] = None
        if environment.in_cloud_run():
            resource = detect_resource()
        else:
            resource = Resource(type="global", labels={})

        structured_handler = handlers.CloudLoggingHandler(
            client,
            resource=resource,
        )

        handlers.setup_logging(structured_handler, log_level=logging.INFO)

        # The request logs are emitted after a request completes, so that they have the response status and latency.
        # If a request is OOMing a machine you won’t  be able to see that request because it will never be logged.
        # If you are wondering if a request has started and is just taking a really long time you can’t see that either
        # For these reasons, we emit a log at the beginning of the request lifecycle
        before_request_log = logging.getLogger(RECIDIVIZ_BEFORE_REQUEST_LOG)
        before_request_handler = handlers.CloudLoggingHandler(
            client, name=RECIDIVIZ_BEFORE_REQUEST_LOG
        )
        before_request_log.addHandler(before_request_handler)

        if label_filter:
            structured_handler.addFilter(label_filter)
            before_request_handler.addFilter(label_filter)

        # Streams unstructured logs to stdout - these logs will still show up
        # under the stdout Stackdriver logs bucket,
        # even if other logs are stalled on the global interpreter lock or some
        # other issue.
        stdout_handler = logging.StreamHandler(sys.stdout)
        logger.addHandler(stdout_handler)
        logger.setLevel(logging.INFO)

        for handler in logger.handlers:
            if not isinstance(
                handler,
                (
                    logging.StreamHandler,
                    handlers.CloudLoggingHandler,
                ),
            ):
                logger.removeHandler(handler)
    else:
        logging.basicConfig()

    for handler in logger.handlers:
        # If we aren't writing directly to Stackdriver, prefix the log with important
        # context that would be in the labels.
        if not isinstance(handler, handlers.CloudLoggingHandler):
            handler.setFormatter(
                logging.Formatter(
                    "[pid: %(process)d] (%(region)s) %(module)s/%(funcName)s : %(message)s"
                )
            )

    # Export gunicorn errors using the same handlers as other logs, so that they
    # go to Stackdriver in production.
    gunicorn_logger = logging.getLogger("gunicorn.error")
    gunicorn_logger.handlers = logger.handlers
