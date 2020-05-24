# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""This file contains all of the code to ingest and aggregate covid sources"""

import datetime
import logging
import os
import gcsfs


COVID_STITCHED_UPLOAD_BUCKET = '{}-covid-aggregation'
COVID_STITCHED_HISTORICAL_UPLOAD_BUCKET = '{}-covid-aggregation-storage'


class CovidIngestError(Exception):
    """Generic error when covid ingest fails."""


def parse_to_csv(bucket, source, filename):
    """Ingests covid sources"""
    if not bucket or not source or not filename:
        raise CovidIngestError(
            "All of source, bucket, and filename must be provided")

    all_sources = {'prison': None, 'ucla': None, 'recidiviz_manual': None}

    project_id = os.environ.get('GCP_PROJECT')
    path = os.path.join(bucket, source, filename)
    # Don't use the gcsfs cache
    fs = gcsfs.GCSFileSystem(project=project_id, cache_timeout=-1)
    logging.info("The path to download from is %s", path)
    bucket_path = os.path.join(bucket, source)
    logging.info("The files in the directory are:")
    logging.info(fs.ls(bucket_path))

    # Next we try to find the latest version of all three sources, if for
    # whatever reason a source folder is completely empty, we abort the
    # stitching process.
    for covid_source in all_sources:
        all_sources[covid_source] = _get_latest_source_file(
            fs, bucket, covid_source)

    # Once we have the latest file for each source, start stitching
    return _stitch_and_upload(fs, all_sources)


def _get_latest_source_file(fs, bucket, source):
    """Walks the source directory given and finds the newest file for the given
    data source."""
    source_path = os.path.join(bucket, source)
    files = fs.ls(source_path)
    if not files:
        logging.info("No files in source bucket %s, aborting ingest", source)
        return None

    # Pick an old date since it will be replaced.
    latest_time = datetime.datetime(1970, 1, 1)
    latest_file = ''
    for file in files:
        info = fs.info(os.path.join(bucket, source, file))
        cur_time = datetime.datetime(info['created'])
        if cur_time > latest_time:
            latest_file = file
    logging.info("Found newest file from source bucket %s", source)
    return os.path.join(bucket, source, latest_file)


def _stitch_and_upload(fs, all_sources):  # pylint: disable=unused-argument
    """Stitches all sources together into one combined CSV and uploads the
    output file.

    This function will abort if any of the sources are not provided.  Once it
    has been stitched, it checks the covid upload bucket for the current
    freshest file and replaces it if the current one is newer; moving the old
    one to the historical bucket.  This function should use the timestamp of the
    freshest of the sources as its timestamp and append the datetime to the name
    of the file.

    Returns:
        The GCS path of the final, aggregated csv.
    """
