# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Links Case Triage Sentry events to the Cloud Logging entries for the same request."""
from urllib.parse import quote

import sentry_sdk

# google-cloud-logging uses this function to set the trace field on every log entry
# our CloudLoggingHandler writes, so reading the trace ID with it keeps the Sentry tag
# and the log entries in agreement.
from google.cloud.logging_v2.handlers._helpers import get_request_data_from_flask

GCP_TRACE_ID_TAG = "gcp_trace_id"
GCP_LOGS_CONTEXT = "gcp_logs"


def logs_explorer_url(project_id: str, trace_id: str) -> str:
    """Returns a Logs Explorer URL that shows every log entry for trace_id in
    project_id."""
    query = f'trace="projects/{project_id}/traces/{trace_id}"'
    return (
        "https://console.cloud.google.com/logs/query;"
        f"query={quote(query, safe='')}?project={project_id}"
    )


def tag_sentry_scope_with_gcp_trace(project_id: str) -> None:
    """Tags the current request's Sentry scope with its Cloud Trace ID and a Logs
    Explorer link, so every Sentry event from the request links to its log entries.

    The trace ID comes from the traceparent header, or from X-Cloud-Trace-Context if
    that is missing, which is where Cloud Logging gets it too. Does nothing when the
    request has neither header. Must run inside a Flask request context, for example
    from a before_request hook.
    """
    _, trace_id, _, _ = get_request_data_from_flask()
    if not trace_id:
        return
    sentry_sdk.set_tag(GCP_TRACE_ID_TAG, trace_id)
    sentry_sdk.set_context(
        GCP_LOGS_CONTEXT, {"url": logs_explorer_url(project_id, trace_id)}
    )
