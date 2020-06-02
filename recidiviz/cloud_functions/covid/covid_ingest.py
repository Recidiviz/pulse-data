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
import xlrd

from covid import covid_aggregator


COVID_CURRENT_OUTPUT_BUCKET = '{}-covid-aggregation-output'
COVID_HISTORICAL_OUTPUT_BUCKET = '{}-covid-aggregation-storage'

PRISON_DATA_FOLDER = 'prison'
UCLA_DATA_FOLDER = 'ucla'
RECIDIVIZ_DATA_FOLDER = 'recidiviz'

OUTPUT_FILE_NAME = 'merged_data_{}.csv'
OUTPUT_FILE_TIMESTAMP_FORMAT = '%Y_%m_%d_%H_%M_%S'


class CovidIngestError(Exception):
    """Generic error when covid ingest fails."""


def parse_to_csv(bucket, source, filename):
    """Ingests covid sources"""
    if not bucket or not source or not filename:
        raise CovidIngestError(
            "All of bucket, source, and filename must be provided: Bucket - "
            + "{}, source - {}, filename - {}"
            .format(bucket, source, filename))

    all_sources = {PRISON_DATA_FOLDER: None,
                   UCLA_DATA_FOLDER: None,
                   RECIDIVIZ_DATA_FOLDER: None}

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
        all_sources[covid_source] = _get_latest_source_file_name_and_timestamp(
            fs, bucket, covid_source)

    # Once we have the latest file for each source, start stitching
    return _stitch_and_upload(fs, all_sources)


def _get_latest_source_file_name_and_timestamp(fs, bucket, source):
    """Walks the source directory given and finds the newest file and its
    timestamp for the given data source."""
    source_path = os.path.join(bucket, source)
    files = fs.ls(source_path)
    if not files:
        logging.info("No files in source bucket %s, aborting ingest", source)
        return None

    # Pick an old date since it will be replaced.
    latest_time = datetime.datetime(1970, 1, 1)
    latest_file = ''
    # Note that each file name returned by ls already includes the full path
    for file in files:
        upload_time = _get_upload_time_from_file_info(fs.info(file))
        if upload_time > latest_time:
            latest_time = upload_time
            latest_file = file
    logging.info(
        "Found newest file %s from source bucket %s", latest_file, source)
    return (latest_file, latest_time)


def _stitch_and_upload(fs, all_sources):
    """Stitches all sources together into one combined CSV and uploads the
    output file, with a filename including the timestamp of the most recent
    source file included.

    Any existing files in the output bucket with a timestamp earlier than the
    output file will be moved to the historical output bucket. If there are any
    files in the output bucket with a timestamp later than the output file, this
    function will raise an error.

    This function will abort if any of the sources are not provided.

    Returns:
        The GCS path of the final, aggregated csv.
    """
    prison_data_file_path = all_sources[PRISON_DATA_FOLDER][0]
    ucla_file_path = all_sources[UCLA_DATA_FOLDER][0]
    recidiviz_file_path = all_sources[RECIDIVIZ_DATA_FOLDER][0]

    prison_data_date = all_sources[PRISON_DATA_FOLDER][1]
    ucla_date = all_sources[UCLA_DATA_FOLDER][1]
    recidiviz_date = all_sources[RECIDIVIZ_DATA_FOLDER][1]
    latest_source_time = max(prison_data_date, ucla_date, recidiviz_date)

    aggregated_csv = None
    with fs.open(ucla_file_path) as ucla_file:
        ucla_workbook = xlrd.open_workbook(file_contents=ucla_file.read())
        # GCSFS open mode defaults to rb, so to read a normal text file we need
        # to specify rt
        with fs.open(prison_data_file_path, "rt") as prison_data_file:
            with fs.open(recidiviz_file_path, "rt") as recidiviz_file:
                aggregated_csv = covid_aggregator.aggregate(
                    prison_data_file, ucla_workbook, recidiviz_file)

    project_id = os.environ.get('GCP_PROJECT')
    output_bucket = COVID_CURRENT_OUTPUT_BUCKET.format(project_id)
    historical_output_bucket = COVID_HISTORICAL_OUTPUT_BUCKET.format(project_id)

    output_bucket_files = fs.ls(output_bucket)
    for file_path in output_bucket_files:
        file_time = _get_upload_time_from_file_info(fs.info(file_path))
        if file_time > latest_source_time:
            raise CovidIngestError(
                ('File {filename} currently in output bucket {output_bucket} '
                 + 'has creation time of {existing_time}, which is greater '
                 + 'than latest input source time of {new_time}')
                .format(filename=file_path,
                        output_bucket=output_bucket,
                        existing_time=file_time.strftime('%Y-%m-%d %H:%M:%S'),
                        new_time=latest_source_time.strftime(
                            '%Y-%m-%d %H:%M:%S')))
        file_name = file_path.split('/')[-1]
        target_path = os.path.join(historical_output_bucket, file_name)
        fs.mv(file_path, target_path)

    output_file_path = os.path.join(
        output_bucket, OUTPUT_FILE_NAME.format(
            latest_source_time.strftime(OUTPUT_FILE_TIMESTAMP_FORMAT)))
    with fs.open(output_file_path, 'wt') as output_file:
        output_file.write(aggregated_csv)

    return output_file_path


def _get_upload_time_from_file_info(info):
    """Returns the file upload time as a datetime"""
    # Have to strip trailing Z, because fromisoformat can't parse it
    upload_timestamp = info['timeCreated'].strip('Z')
    return datetime.datetime.fromisoformat(upload_timestamp)
