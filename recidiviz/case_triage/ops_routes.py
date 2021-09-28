# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""This file defines routes useful for operating Case Triage that do _not_
live in the Case Triage app. Instead these live on the main recidiviz app and
are triggered manually or by cron processes."""
import logging
import os
from http import HTTPStatus
from typing import Tuple

from flask import Blueprint, request

from recidiviz.case_triage.views.view_config import CASE_TRIAGE_EXPORTED_VIEW_BUILDERS
from recidiviz.cloud_sql.gcs_import_to_cloud_sql import import_gcs_csv_to_cloud_sql
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.google_cloud.cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    CloudTaskQueueManager,
    get_cloud_task_json_body,
)
from recidiviz.metrics.export.export_config import (
    CASE_TRIAGE_VIEWS_OUTPUT_DIRECTORY_URI,
)
from recidiviz.persistence.database.schema import case_triage as case_triage_schema
from recidiviz.persistence.database.schema_utils import (
    SchemaType,
    get_database_entity_by_table_name,
)
from recidiviz.utils import metadata
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.params import get_only_str_param_value

CASE_TRIAGE_DB_OPERATIONS_QUEUE = "case-triage-db-operations-queue"

case_triage_ops_blueprint = Blueprint("/case_triage_ops", __name__)


@case_triage_ops_blueprint.route("/run_gcs_imports", methods=["GET", "POST"])
@requires_gae_auth
def _run_gcs_imports() -> Tuple[str, HTTPStatus]:
    """Exposes an endpoint to trigger standard GCS imports."""
    body = get_cloud_task_json_body()
    filename = body.get("filename")
    if not filename:
        return "Must include `filename` in the json payload", HTTPStatus.BAD_REQUEST
    for builder in CASE_TRIAGE_EXPORTED_VIEW_BUILDERS:
        if f"{builder.view_id}.csv" != filename:
            continue

        csv_path = GcsfsFilePath.from_absolute_path(
            os.path.join(
                CASE_TRIAGE_VIEWS_OUTPUT_DIRECTORY_URI.format(
                    project_id=metadata.project_id()
                ),
                filename,
            )
        )

        import_gcs_csv_to_cloud_sql(
            SchemaType.CASE_TRIAGE,
            get_database_entity_by_table_name(case_triage_schema, builder.view_id),
            csv_path,
            builder.columns,
            seconds_to_wait=180,
        )
        logging.info("View (%s) successfully imported", builder.view_id)

    return "", HTTPStatus.OK


@case_triage_ops_blueprint.route("/handle_gcs_imports", methods=["GET"])
@requires_gae_auth
def _handle_gcs_imports() -> Tuple[str, HTTPStatus]:
    """Exposes an endpoint that enqueues a Cloud Task to trigger the GCS imports."""
    filename = get_only_str_param_value("filename", request.args, preserve_case=True)
    if not filename:
        return "Must include a filename query parameter", HTTPStatus.BAD_REQUEST

    cloud_task_manager = CloudTaskQueueManager(
        queue_info_cls=CloudTaskQueueInfo, queue_name=CASE_TRIAGE_DB_OPERATIONS_QUEUE
    )
    cloud_task_manager.create_task(
        relative_uri="/case_triage_ops/run_gcs_imports", body={"filename": filename}
    )
    logging.info("Enqueued gcs_import task to %s", CASE_TRIAGE_DB_OPERATIONS_QUEUE)
    return "", HTTPStatus.OK
