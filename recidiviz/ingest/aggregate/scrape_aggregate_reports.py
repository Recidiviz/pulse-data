# Recidiviz - a platform for tracking granular recidivism metrics in real time
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

"""Exposes an endpoint to scrape all of the county websites."""

from http import HTTPStatus
import logging
import os
import tempfile
from typing import Optional, Tuple, Dict
from urllib.parse import urlparse
import requests
from flask import Blueprint, request
import gcsfs

from recidiviz.ingest.aggregate.regions.ca import ca_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.fl import fl_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.ga import ga_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.hi import hi_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.ky import ky_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.ny import ny_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.tx import tx_aggregate_site_scraper
from recidiviz.utils import metadata
from recidiviz.utils.auth import authenticate_request
from recidiviz.utils.params import get_value

scrape_aggregate_reports_blueprint = Blueprint(
    'scrape_aggregate_reports', __name__)


class ScrapeAggregateError(Exception):
    """Errors thrown in the state aggregate endpoint"""


# GCP has globally unique names for buckets, so we instead have to prepend
# the buckets with the gcp project.
HISTORICAL_BUCKET = '{}-processed-state-aggregates'
UPLOAD_BUCKET = '{}-state-aggregate-reports'


@scrape_aggregate_reports_blueprint.route('/scrape_state')
@authenticate_request
def scrape_aggregate_reports():
    """Calls state aggregates"""

    # Please add new states in alphabetical order
    state_to_scraper = {
        'california': ca_aggregate_site_scraper.get_urls_to_download,
        'florida': fl_aggregate_site_scraper.get_urls_to_download,
        'georgia': ga_aggregate_site_scraper.get_urls_to_download,
        'hawaii': hi_aggregate_site_scraper.get_urls_to_download,
        'kentucky': ky_aggregate_site_scraper.get_urls_to_download,
        'new_york': ny_aggregate_site_scraper.get_urls_to_download,
        'texas': tx_aggregate_site_scraper.get_urls_to_download,
    }
    state = get_value('state', request.args)
    # We want to always download the pdf if it is NY because they always have
    # the same name.
    always_download = (state == 'new_york')
    is_ca = (state == 'california')
    urls = state_to_scraper[state]()
    gcp_project = metadata.project_id()
    historical_bucket = HISTORICAL_BUCKET.format(gcp_project)
    upload_bucket = UPLOAD_BUCKET.format(gcp_project)
    fs = gcsfs.GCSFileSystem(project=gcp_project, cache_timeout=-1)
    logging.info('Scraping all pdfs for %s', state)

    for url in urls:
        post_data = None
        if isinstance(url, Tuple):
            url, post_data = url
            # We need to append the year of the report to create uniqueness in
            # the name since california sends post requests with the same url.
            pdf_name = state
            if is_ca:
                pdf_name += str(post_data['year'])
        else:
            pdf_name = urlparse(url).path.replace('/', '_').lower()
        historical_path = os.path.join(historical_bucket, state, pdf_name)
        file_to_upload = _get_file_to_upload(
            historical_path, fs, url, pdf_name, always_download, post_data)
        if file_to_upload:
            upload_path = os.path.join(upload_bucket, state, pdf_name)
            fs.put(file_to_upload, upload_path)
            logging.info('Successfully downloaded %s', url)
        else:
            logging.info(
                'Skipping %s because the file already exists', url)

    return '', HTTPStatus.OK


def _get_file_to_upload(
        path: str, fs: gcsfs.GCSFileSystem, url: str, pdf_name: str,
        always_download: bool, post_data: Dict) \
        -> Optional[str]:
    """This function checks first whether it needs to download, and then
    returns the locally downloaded pdf"""
    # First check if the path doesn't exist at all
    path_to_download = None
    if always_download or not fs.exists(path):
        if post_data:
            response = requests.post(url, data=post_data)
        else:
            response = requests.get(url)
        if response.status_code == 200:
            path_to_download = os.path.join(tempfile.gettempdir(), pdf_name)
            with open(path_to_download, 'wb') as f:
                # Need to use content since PDF needs to write raw bytes.
                f.write(response.content)
        else:
            raise ScrapeAggregateError(
                'Could not download file {}'.format(pdf_name))
    return path_to_download
