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
from functools import cached_property
from types import TracebackType
from typing import Any, Dict, Optional, Tuple, Type, Union

from flask import request
from google.cloud.logging import Client, handlers
from google.cloud.logging.resource import Resource
from opencensus.common.runtime_context import RuntimeContext
from opencensus.trace import execution_context

from recidiviz.utils import environment, metadata, monitoring
from recidiviz.utils.environment import CloudRunEnvironment
from recidiviz.utils.metadata import CloudRunMetadata

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
        trace_id = f"projects/{self.project_id}/traces/{labels.get(handlers.app_engine._TRACE_ID_LABEL, None)}"
        self.transport.send(
            record,
            _truncate_if_needed(super().format(record)),
            resource=self.resource,
            labels=labels,
            trace=trace_id,
        )


class StructuredCloudLoggingHandler(handlers.CloudLoggingHandler):
    """
    Customized CloudLoggingHandler to add trace context to log messages
    """

    @cached_property
    def logging_resource(self) -> Resource:
        if environment.in_cloud_run():
            cloud_run_metadata = CloudRunMetadata.build_from_metadata_server()

            return Resource(
                type="cloud_run_revision",
                labels={
                    "location": cloud_run_metadata.region,
                    "project_id": cloud_run_metadata.project_id,
                    "configuration_name": CloudRunEnvironment.get_configuration_name(),
                    "revision_name": CloudRunEnvironment.get_revision_name(),
                    "service_name": CloudRunEnvironment.get_service_name(),
                },
            )

        return Resource(type="global", labels={})

    def emit(self, record: logging.LogRecord) -> None:
        """Overrides the emit method for CloudLoggingHandler to include the trace id"""
        trace_id = None
        # Inspired by https://cloud.google.com/run/docs/logging#writing_structured_logs
        request_is_defined = "request" in globals() or "request" in locals()
        if request_is_defined and request:
            trace_header = request.headers.get("X-Cloud-Trace-Context")

            if trace_header:
                trace = trace_header.split("/")
                trace_id = f"projects/{self.client.project}/traces/{trace[0]}"

        # No longer following the example above- it seems like it would be nicer to call this
        # function than to print() a JSON string.
        self.transport.send(
            record,
            super().format(record),
            resource=self.logging_resource,
            labels=self.labels,
            trace=trace_id,
        )


STRUCTURED_LOGGERS = (StructuredAppEngineHandler, StructuredCloudLoggingHandler)


def setup() -> None:
    """Setup logging"""
    # Set the region on log records.
    logging.setLogRecordFactory(ContextualLogRecord)
    logger = logging.getLogger()

    # Send logs directly via the logging client if possible. This ensures trace
    # ids are propagated and allows us to send structured messages.
    if environment.in_gcp():
        client = Client()
        if environment.in_cloud_run():
            structured_handler_cls = StructuredCloudLoggingHandler
        else:
            structured_handler_cls = StructuredAppEngineHandler

        structured_handler = structured_handler_cls(client)

        handlers.setup_logging(structured_handler, log_level=logging.INFO)

        before_request_handler = structured_handler_cls(client, name=BEFORE_REQUEST_LOG)
        logging.getLogger(BEFORE_REQUEST_LOG).addHandler(before_request_handler)

        # Streams unstructured logs to stdout - these logs will still show up
        # under the stdout Stackdriver logs bucket,
        # even if other logs are stalled on the global interpreter lock or some
        # other issue.
        stdout_handler = logging.StreamHandler(sys.stdout)
        handlers.setup_logging(stdout_handler, log_level=logging.INFO)
        for handler in logger.handlers:
            if not isinstance(
                handler,
                (
                    logging.StreamHandler,
                    *STRUCTURED_LOGGERS,
                ),
            ):
                logger.removeHandler(handler)
    else:
        logging.basicConfig()

    for handler in logger.handlers:
        # If we aren't writing directly to Stackdriver, prefix the log with important
        # context that would be in the labels.
        if not isinstance(handler, STRUCTURED_LOGGERS):
            handler.setFormatter(
                logging.Formatter(
                    "[pid: %(process)d] (%(region)s) %(module)s/%(funcName)s : %(message)s"
                )
            )

    # Export gunicorn errors using the same handlers as other logs, so that they
    # go to Stackdriver in production.
    gunicorn_logger = logging.getLogger("gunicorn.error")
    gunicorn_logger.handlers = logger.handlers
