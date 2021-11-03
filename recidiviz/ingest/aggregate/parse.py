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

"""Exposes APIs for parsing and persisting aggregate reports."""
import logging
from http import HTTPStatus
from typing import Tuple

from flask import Blueprint, request

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.ingest.aggregate.regions.ca import ca_aggregate_ingest
from recidiviz.ingest.aggregate.regions.co import co_aggregate_ingest
from recidiviz.ingest.aggregate.regions.fl import fl_aggregate_ingest
from recidiviz.ingest.aggregate.regions.ga import ga_aggregate_ingest
from recidiviz.ingest.aggregate.regions.hi import hi_aggregate_ingest
from recidiviz.ingest.aggregate.regions.indiana import in_aggregate_ingest
from recidiviz.ingest.aggregate.regions.ky import ky_aggregate_ingest
from recidiviz.ingest.aggregate.regions.ma import ma_aggregate_ingest
from recidiviz.ingest.aggregate.regions.ny import ny_aggregate_ingest
from recidiviz.ingest.aggregate.regions.pa import pa_aggregate_ingest
from recidiviz.ingest.aggregate.regions.tn import tn_aggregate_ingest
from recidiviz.ingest.aggregate.regions.tx import tx_aggregate_ingest
from recidiviz.ingest.aggregate.regions.wv import wv_aggregate_ingest
from recidiviz.persistence.database.schema.aggregate import dao
from recidiviz.utils import metadata
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.params import get_str_param_value
from recidiviz.utils.string import StrictStringFormatter

aggregate_parse_blueprint = Blueprint("aggregate_parse", __name__)


class StateAggregateError(Exception):
    """Errors thrown in the state aggregate endpoint"""


HISTORICAL_BUCKET = "{project_id}-processed-state-aggregates"

# Please add new states in alphabetical order
STATE_TO_PARSER = {
    "california": ca_aggregate_ingest.parse,
    "colorado": co_aggregate_ingest.parse,
    "florida": fl_aggregate_ingest.parse,
    "georgia": ga_aggregate_ingest.parse,
    "hawaii": hi_aggregate_ingest.parse,
    "indiana": in_aggregate_ingest.parse,
    "kentucky": ky_aggregate_ingest.parse,
    "massachusetts": ma_aggregate_ingest.parse,
    "new_york": ny_aggregate_ingest.parse,
    "pennsylvania": pa_aggregate_ingest.parse,
    "tennessee": tn_aggregate_ingest.parse,
    "texas": tx_aggregate_ingest.parse,
    "west_virginia": wv_aggregate_ingest.parse,
}


@aggregate_parse_blueprint.route("/persist_file")
@requires_gae_auth
def state_aggregate() -> Tuple[str, HTTPStatus]:
    """Calls state aggregates"""
    bucket = get_str_param_value("bucket", request.args)
    state = get_str_param_value("state", request.args)
    filename = get_str_param_value("filename", request.args)
    project_id = metadata.project_id()
    logging.info("The project id is %s", project_id)
    if not bucket or not state or not filename:
        raise StateAggregateError("All of state, bucket, and filename must be provided")
    directory_path = GcsfsDirectoryPath(bucket, state)
    path = GcsfsFilePath.from_directory_and_file_name(directory_path, filename)
    parser = STATE_TO_PARSER[state]
    fs = GcsfsFactory.build()
    logging.info("The path to download from is %s", path)

    logging.info("The files in the directory are:")
    logging.info(
        fs.ls_with_blob_prefix(
            bucket_name=directory_path.bucket_name,
            blob_prefix=directory_path.relative_path,
        )
    )

    # Providing a stream buffer to tabula reader does not work because it
    # tries to load the file into the local filesystem, since appengine is a
    # read only filesystem (except for the tmpdir) we download the file into
    # the local tmpdir and pass that in.
    handle = fs.download_to_temp_file(path, retain_original_filename=True)
    if not handle:
        raise StateAggregateError(f"Unable to download file: {path}")
    logging.info("Successfully downloaded file from gcs: %s", handle.local_file_path)

    result = parser(handle.local_file_path)
    logging.info("Successfully parsed the report")
    for table, df in result.items():
        dao.write_df(table, df)

    # If we are successful, we want to move the file out of the cloud
    # function triggered directory, and into the historical path.
    historical_path = GcsfsFilePath.from_directory_and_file_name(
        GcsfsDirectoryPath(
            StrictStringFormatter().format(HISTORICAL_BUCKET, project_id=project_id),
            state,
        ),
        filename,
    )
    fs.mv(path, historical_path)
    return "", HTTPStatus.OK
