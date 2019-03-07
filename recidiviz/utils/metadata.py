# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

"""Utility methods for fetching app engine related metadata."""
import logging
import os
from typing import Dict

import requests

BASE_METADATA_URL = 'http://metadata/computeMetadata/v1/'
HEADERS = {'Metadata-Flavor': 'Google'}
TIMEOUT = 2

metadata_cache: Dict[str, str] = {}

def _get_metadata(url: str):
    if url in metadata_cache:
        return metadata_cache[url]

    try:
        r = requests.get(
            BASE_METADATA_URL + url,
            headers=HEADERS,
            timeout=TIMEOUT)
        r.raise_for_status()
        metadata_cache[url] = r.text
        return r.text
    except Exception as e:
        logging.error('Failed to fetch metadata \'%s\': \'%s\'', url, e)
        return None

def project_number():
    """Gets the numeric_project_id (project number) for this instance from the
    Compute Engine metadata server.
    """
    return _get_metadata('project/numeric-project-id')

def project_id():
    """Gets the project_id for this instance from the Compute Engine metadata
    server. If the metadata server is unavailable, it assumes that the
    application is running locally and falls back to the GOOGLE_CLOUD_PROJECT
    environment variable.
    """
    return (
        _get_metadata('project/project-id') or os.getenv('GOOGLE_CLOUD_PROJECT')
    )

def instance_id():
    """Returns the GCP instnance ID of the current instance."""
    return _get_metadata('instance/id')

def zone():
    """Returns the GCP zone of the current instance."""
    zone_string = _get_metadata('instance/zone')
    if zone_string:
        # Of the form 'projects/123456789012/zones/us-east1-c'
        zone_string = zone_string.split('/')[-1]

    return zone_string

def region():
    """Returns the GCP region of the current instance."""
    region_string = None
    zone_string = zone()
    if zone_string:
        # Of the form 'us-east1-c'
        region_split = zone_string.split('-')[:2]
        region_string = '-'.join(region_split)

    return region_string
