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

"""Functionality to perform direct ingest.
"""

import os

import logging

from flask import Blueprint, request
import gcsfs
import dropbox
from dropbox.files import DeletedMetadata, FolderMetadata
from recidiviz.utils.auth import authenticate_request
from recidiviz.utils.params import get_value


_DIRECT_INGEST_GCS_BUCKET_NAME = 'direct-ingest'

check_direct_ingest_upload_blueprint = Blueprint(
    'check_direct_ingest_upload', __name__)


class DirectIngestUploadError(Exception):
    """Errors thrown in the direct ingest upload endpoint"""


def mv_from_db_to_gcs(region_name):
    """Function to move the contents of a Dropbox folder to a GCS bucket.

    Args
        region_name: (str) the name of the region to move files for.
    """
    logging.info("Processing %s", region_name)

    token = os.environ.get('DB_TOKEN')
    if not token:
        logging.error("No Dropbox token found, check your environment.")
        return
    dbx = dropbox.Dropbox(token)

    project_id = os.environ.get('GCP_PROJECT')
    fs = gcsfs.GCSFileSystem(project=project_id, cache_timeout=-1)

    db_folder_path = '/{}'.format(region_name)
    processed_db_folder_path = db_folder_path + '/processed/'
    response = dbx.files_list_folder(path=db_folder_path)
    for entry in response.entries:
        if isinstance(entry, (DeletedMetadata, FolderMetadata)):
            continue

        db_path = entry.path_lower
        filename = os.path.basename(db_path)
        logging.info("Processing %s", filename)
        _, resp = dbx.files_download(db_path)

        # Note that this assumes that the bucket name is prefixed with
        # the region_name, which is a good idea regardless because
        # bucket names must be unique across Google(!).
        bucket = '{}-{}'.format(project_id, _DIRECT_INGEST_GCS_BUCKET_NAME)
        gcs_path = os.path.join(bucket, region_name, filename)
        with fs.open(gcs_path, mode='wb') as gcs_fp:
            gcs_fp.write(resp.content)

        dbx.files_move(db_path,
                       os.path.join(processed_db_folder_path, filename),
                       allow_shared_folder=True, allow_ownership_transfer=True)

        logging.info("Completed processing %s", filename)


@check_direct_ingest_upload_blueprint.route('/check_direct_ingest_upload')
@authenticate_request
def check_direct_ingest_upload():
    """Endpoint that is hit to check whether new data exists for direct ingest.
    """
    region_name = get_value('region', request.args)
    mv_from_db_to_gcs(region_name)
