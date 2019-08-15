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

"""Entrypoint to read_pdf application."""
import logging
import os
import pickle
import tempfile

import gcsfs
import tabula
from flask import Flask, request

from recidiviz.cloud_functions.cloud_function_utils import GCSFS_NO_CACHING
from recidiviz.utils import metadata
from recidiviz.utils.auth import authenticate_request

app = Flask(__name__)


@app.route('/read_pdf', methods=['POST'])
@authenticate_request
def read_pdf():
    """Reads a PDF from GCS and calls tabula.read_pdf.
    This endpoint accepts both query params and post data. The query params are
    required to specify the file and its location in GCS. Additionally, the
    endpoint accepts JSON post data, which it passes as arguments to
    tabula.read_pdf.

    Query Params:
      - location: the bucket/path location of the file in GCS
      - filename: the name of the file in the given |location|.

    JSON post data:
      - the kwargs passed to |tabula.read_csv|; a dictionary keyed by option
        names with values that are possibly nested dictionaries, as in the
        'pandas_options' kwarg.

    The HTTP response is the pandas dataframe as a csv.
    """
    if 'location' not in request.args or 'filename' not in request.args:
        raise ValueError("'location' and 'filename' must be provided.")
    location = request.args['location']
    filename = request.args['filename']
    project_id = metadata.project_id()
    logging.info("The project id is [%s]", project_id)
    path = os.path.join(location, filename)
    # Don't use the gcsfs cache
    fs = gcsfs.GCSFileSystem(project=project_id, cache_timeout=GCSFS_NO_CACHING)
    logging.info("The path to download from is [%s]", filename)
    logging.info("The files in the directory are:")
    logging.info(fs.ls(location))

    # Providing a stream buffer to tabula reader does not work because it
    # tries to load the file into the local filesystem, since appengine is a
    # read only filesystem (except for the tmpdir) we download the file into
    # the local tmpdir and pass that in.
    tmpdir_path = os.path.join(tempfile.gettempdir(), filename)
    fs.get(path, tmpdir_path)
    output = tabula.read_pdf(tmpdir_path, **(request.json or {}))
    return pickle.dumps(output)


@app.errorhandler(500)
def server_error(e):
    logging.exception("An error occurred during a request.")
    return str(e), 500
