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


import csv
import datetime
import logging
import os
import requests
import xlrd

import gcsfs

# Mypy error "Cannot find implementation or library stub for module named 'covid'" ignored here because cloud functions
# require that imports are declared relative to the cloud functions package itself. In general, we should avoid shipping
# complex code in cloud functions. The function itself should call an API endpoint that can live in an external package
# with proper import resolution.
from covid import covid_aggregator  # type: ignore[import]


SOURCES_BUCKET = '{}-covid-aggregation'
OUTPUT_BUCKET = '{}-covid-aggregation-output'
HISTORICAL_OUTPUT_BUCKET = '{}-covid-aggregation-storage'

PRISON_FOLDER = 'prison'
UCLA_FOLDER = 'ucla'

# Recidiviz Google Sheets data, as CSV
RECIDIVIZ_FILE_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTbxP67VHDHQt4xvpNmzbsXyT0pSh_b1Pn7aY5Ac089KKYnPDT6PpskMBMvhOX_PA08Zqkxt4zNn8_y/pub?gid=0&single=true&output=csv' # pylint:disable=line-too-long
# Mapping file used to determine canonical identifying info for each facility
FACILITY_INFO_MAPPING_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vT95DUfwcHbauuuMScd1Jb9u3vLCdfCcieXrRthNowoSbrmeWF3ibv06LkfcDxl1Vd97S5aujvnHdZX/pub?gid=1112897899&single=true&output=csv' # pylint:disable=line-too-long

OUTPUT_FILE_NAME = 'merged_data_{}.csv'
OUTPUT_FILE_TIMESTAMP_FORMAT = '%Y_%m_%d_%H_%M_%S'


class CovidIngestError(Exception):
    """Generic error when covid ingest fails."""


def ingest_and_aggregate_latest_data():
    """Ingests latest available COVID data and writes them to an aggregate
    file
    """
    project_id = os.environ.get('GCP_PROJECT')
    # Don't use the gcsfs cache
    file_system = gcsfs.GCSFileSystem(project=project_id, cache_timeout=-1)
    sources_bucket = SOURCES_BUCKET.format(project_id)
    output_bucket = OUTPUT_BUCKET.format(project_id)
    historical_output_bucket = HISTORICAL_OUTPUT_BUCKET.format(project_id)

    prison_files = _get_content_of_all_files_from_directory(
        file_system, os.path.join(sources_bucket, PRISON_FOLDER), 'rt')
    # UCLA file is an Excel workbook and so should not be read as text
    ucla_file = _get_content_of_latest_file_from_directory(
        file_system, os.path.join(sources_bucket, UCLA_FOLDER), 'rb')
    recidiviz_file = _fetch_remote_file(RECIDIVIZ_FILE_URL)
    facility_mapping_file = _fetch_remote_file(FACILITY_INFO_MAPPING_URL)

    # Convert files into the format the aggregator expects
    prison_csv_dicts = [
        csv.DictReader(prison_file.splitlines(), delimiter=',')
        for prison_file
        in prison_files]
    ucla_workbook = xlrd.open_workbook(file_contents=ucla_file)
    recidiviz_csv_dict = csv.DictReader(
        recidiviz_file.splitlines(), delimiter=',')
    facility_mapping_csv = csv.reader(
        facility_mapping_file.splitlines(), delimiter=',')

    aggregated_csv = covid_aggregator.aggregate(
        prison_csv_dicts,
        ucla_workbook,
        recidiviz_csv_dict,
        facility_mapping_csv)

    # Clear out any existing files in the output bucket by moving them to the
    # historical bucket
    output_bucket_files = file_system.ls(output_bucket)
    for file_path in output_bucket_files:
        file_name = file_path.split('/')[-1]
        target_path = os.path.join(historical_output_bucket, file_name)
        file_system.mv(file_path, target_path)

    output_file_path = os.path.join(
        output_bucket, OUTPUT_FILE_NAME.format(
            datetime.datetime.now().strftime(OUTPUT_FILE_TIMESTAMP_FORMAT)))
    with file_system.open(output_file_path, 'wt') as output_file:
        output_file.write(aggregated_csv)


def _get_content_of_all_files_from_directory(file_system, directory, mode):
    """Reads all files in the provided directory and returns their content as
    a list with one element per file"""
    file_contents = []

    files = file_system.ls(directory)
    if not files:
        raise CovidIngestError('No files in folder {}, aborting ingest'
                               .format(directory))
    for file_path in files:
        with file_system.open(file_path, mode) as file:
            file_contents.append(file.read())

    logging.info('Read all files from directory %s', directory)
    return file_contents


def _get_content_of_latest_file_from_directory(file_system, directory, mode):
    """Reads the latest file in the provided directory and returns its content
    """
    file_path = _get_latest_file_path_from_directory(file_system, directory)
    file_content = None
    with file_system.open(file_path, mode) as file:
        file_content = file.read()
    logging.info('Got latest file %s in directory %s', file_path, directory)
    return file_content


def _fetch_remote_file(url):
    """Fetches the content of a remote file as a string"""
    response = requests.get(url)
    logging.info('Fetched remote file from %s', url)
    return response.content.decode('utf-8')


def _get_latest_file_path_from_directory(file_system, directory):
    """Walks the directory given and returns the most recently uploaded file"""
    files = file_system.ls(directory)
    if not files:
        raise CovidIngestError('No files in folder {}, aborting ingest'
                               .format(directory))

    latest_upload_time = None
    latest_file_path = None
    for file_path in files:
        upload_time = _get_upload_time_from_file_info(
            file_system.info(file_path))
        if not latest_upload_time or upload_time > latest_upload_time:
            latest_upload_time = upload_time
            latest_file_path = file_path
    return latest_file_path


def _get_upload_time_from_file_info(info):
    """Returns the file upload time as a datetime"""
    # Have to strip trailing Z, because fromisoformat can't parse it
    upload_timestamp = info['timeCreated'].strip('Z')
    return datetime.datetime.fromisoformat(upload_timestamp)
