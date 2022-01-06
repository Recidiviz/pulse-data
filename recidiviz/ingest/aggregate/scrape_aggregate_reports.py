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

"""Exposes an endpoint to scrape all of the county websites."""

import logging
from datetime import date
from http import HTTPStatus
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

import requests
from flask import Blueprint, request
from requests.adapters import BaseAdapter

from recidiviz.cloud_storage.gcs_file_system import (
    GCSFileSystem,
    generate_random_temp_path,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.ingest.aggregate.regions.ca import ca_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.co import co_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.fl import fl_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.ga import ga_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.hi import hi_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.ky import ky_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.ma import ma_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.ny import ny_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.tn import tn_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.tx import tx_aggregate_site_scraper
from recidiviz.ingest.aggregate.regions.wv import wv_aggregate_site_scraper
from recidiviz.utils import metadata
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.params import get_str_param_value
from recidiviz.utils.string import StrictStringFormatter

scrape_aggregate_reports_blueprint = Blueprint("scrape_aggregate_reports", __name__)


class ScrapeAggregateError(Exception):
    """Errors thrown in the state aggregate endpoint"""


# GCP has globally unique names for buckets, so we instead have to prepend
# the buckets with the gcp project.
HISTORICAL_BUCKET = "{project_id}-processed-state-aggregates"
UPLOAD_BUCKET = "{project_id}-state-aggregate-reports"


def build_path(bucket_template: str, state: str, pdf_name: str) -> GcsfsFilePath:
    return GcsfsFilePath.from_directory_and_file_name(
        GcsfsDirectoryPath(
            StrictStringFormatter().format(
                bucket_template, project_id=metadata.project_id()
            ),
            state,
        ),
        pdf_name,
    )


@scrape_aggregate_reports_blueprint.route("/scrape_state")
@requires_gae_auth
def scrape_aggregate_reports() -> Tuple[str, HTTPStatus]:
    """Calls state aggregates"""

    # Please add new states in alphabetical order
    state_to_scraper = {
        "california": ca_aggregate_site_scraper.get_urls_to_download,
        "colorado": co_aggregate_site_scraper.get_urls_to_download,
        "florida": fl_aggregate_site_scraper.get_urls_to_download,
        "georgia": ga_aggregate_site_scraper.get_urls_to_download,
        "hawaii": hi_aggregate_site_scraper.get_urls_to_download,
        "kentucky": ky_aggregate_site_scraper.get_urls_to_download,
        "massachusetts": ma_aggregate_site_scraper.get_urls_to_download,
        "new_york": ny_aggregate_site_scraper.get_urls_to_download,
        "tennessee": tn_aggregate_site_scraper.get_urls_to_download,
        "texas": tx_aggregate_site_scraper.get_urls_to_download,
        "west_virginia": wv_aggregate_site_scraper.get_urls_to_download,
    }
    state = get_str_param_value("state", request.args)
    if not state:
        return (
            f"Error with arguments state: {state} given, but not valid",
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )
    # We want to always download the pdf if it is NY because they always have
    # the same name.
    always_download = state == "new_york"
    is_ca = state == "california"
    is_co = state == "colorado"
    is_wv = state == "west_virginia"
    verify_ssl = state != "kentucky"
    urls = state_to_scraper[state]()
    fs = GcsfsFactory.build()
    logging.info("Scraping all pdfs for %s", state)

    adapter = wv_aggregate_site_scraper.TLSAdapter() if is_wv else None

    for url in urls:
        post_data = None
        if isinstance(url, tuple):
            url, post_data = url
            # We need to append the year of the report to create uniqueness in
            # the name since california sends post requests with the same url.
            pdf_name = state
            if is_ca:
                if not post_data["year"]:
                    return (
                        "Error with parsing year from post_data for CA",
                        HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                pdf_name += str(post_data["year"])
        elif is_co:
            pdf_name = date.today().strftime("colorado-%m-%Y")
        else:
            pdf_name = urlparse(url).path.replace("/", "_").lower()  # type: ignore
        historical_path = build_path(HISTORICAL_BUCKET, state, pdf_name)
        file_to_upload = _get_file_to_upload(
            historical_path,
            fs,
            url,  # type: ignore
            pdf_name,
            always_download,
            post_data,  # type: ignore
            verify_ssl,
            adapter=adapter,
        )
        if file_to_upload:
            upload_path = build_path(UPLOAD_BUCKET, state, pdf_name)
            fs.upload_from_contents_handle_stream(
                path=upload_path,
                contents_handle=file_to_upload,
                content_type="application/pdf",
            )
            logging.info("Successfully downloaded %s", url)
        else:
            logging.info("Skipping %s because the file already exists", url)

    return "", HTTPStatus.OK


def _get_file_to_upload(
    path: GcsfsFilePath,
    fs: GCSFileSystem,
    url: str,
    pdf_name: str,
    always_download: bool,
    post_data: Dict,
    verify_ssl: bool,
    adapter: BaseAdapter = None,
) -> Optional[LocalFileContentsHandle]:
    """This function checks first whether it needs to download, and then
    returns the locally downloaded pdf"""
    # If it already exists in GCS, return.
    if fs.exists(path) and not always_download:
        return None

    session = requests.session()
    if adapter:
        session.mount(url, adapter)

    if post_data:
        response = session.post(url, data=post_data, verify=verify_ssl)
    else:
        response = session.get(url, verify=verify_ssl)
    if response.status_code == 200:
        local_path = generate_random_temp_path()
        with open(local_path, "wb") as f:
            f.write(response.content)
        # This is a PDF so use content to get the bytes directly.
        return LocalFileContentsHandle(local_path, cleanup_file=True)

    raise ScrapeAggregateError(f"Could not download file {pdf_name}")
