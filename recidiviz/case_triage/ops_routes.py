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

from flask import Blueprint

from recidiviz.case_triage.views.view_config import CASE_TRIAGE_EXPORTED_VIEW_BUILDERS
from recidiviz.cloud_sql.gcs_import_to_cloud_sql import import_gcs_csv_to_cloud_sql
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.metrics.export.export_config import (
    CASE_TRIAGE_VIEWS_OUTPUT_DIRECTORY_URI,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.utils import metadata
from recidiviz.utils.auth.gae import requires_gae_auth


case_triage_ops_blueprint = Blueprint("/case_triage_ops", __name__)


@case_triage_ops_blueprint.route("/run_standard_cron_gcs_imports", methods=["GET"])
@requires_gae_auth
def _run_cron_gcs_import() -> Tuple[str, HTTPStatus]:
    """Exposes an endpoint to trigger standard GCS imports via cron."""
    for builder in CASE_TRIAGE_EXPORTED_VIEW_BUILDERS:
        if builder.view_id == "etl_officers":
            # TODO(#6202): Until we get more consistent rosters, pushing `etl_officers`
            # may lead to inconsistencies (as we had to manually add 1-2 trusted testers
            # who were not on our rosters).
            continue

        csv_path = GcsfsFilePath.from_absolute_path(
            os.path.join(
                CASE_TRIAGE_VIEWS_OUTPUT_DIRECTORY_URI.format(
                    project_id=metadata.project_id()
                ),
                f"{builder.view_id}.csv",
            )
        )

        import_gcs_csv_to_cloud_sql(
            SchemaType.CASE_TRIAGE,
            builder.view_id,
            csv_path,
            builder.columns,
        )
        logging.info("View (%s) successfully imported", builder.view_id)

    return "", HTTPStatus.OK
