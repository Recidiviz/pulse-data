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

"""Utility methods for fetching app engine related metadata."""
import logging
import os
from typing import Dict

import requests

from recidiviz.utils.environment import local_only

BASE_METADATA_URL = 'http://metadata/computeMetadata/v1/'
HEADERS = {'Metadata-Flavor': 'Google'}
TIMEOUT = 2

_metadata_cache: Dict[str, str] = {}


def _get_metadata(url: str):
    if url in _metadata_cache:
        return _metadata_cache[url]

    try:
        r = requests.get(
            BASE_METADATA_URL + url,
            headers=HEADERS,
            timeout=TIMEOUT)
        r.raise_for_status()
        _metadata_cache[url] = r.text
        return r.text
    except Exception as e:
        logging.error('Failed to fetch metadata [%s]: [%s]', url, e)
        return None


def project_number():
    """Gets the numeric_project_id (project number) for this instance from the
    Compute Engine metadata server.
    """
    return _get_metadata('project/numeric-project-id')


_PROJECT_ID_URL = 'project/project-id'

_override_set = False


class local_project_id_override:
    """Allows us to set a local project override for scripts running locally.

    Usage:
    if __name__ == '__main__':
        print(metadata.project_id())
        with local_project_id_override(GAE_PROJECT_STAGING):
            print(metadata.project_id())
         print(metadata.project_id())

    Prints:
        None
        recidiviz-staging
        None
    """
    def __init__(self, project_id_override: str):
        self.project_id_override = project_id_override
        self.original_project_id = None

    @local_only
    def __enter__(self):
        global _override_set
        if _override_set:
            raise ValueError(f'Project id override already set to {project_id()}')
        _override_set = True

        if _PROJECT_ID_URL in _metadata_cache:
            self.original_project_id = _metadata_cache[_PROJECT_ID_URL]
        _metadata_cache[_PROJECT_ID_URL] = self.project_id_override

    def __exit__(self, _type, _value, _traceback):
        del _metadata_cache[_PROJECT_ID_URL]
        if self.original_project_id:
            _metadata_cache[_PROJECT_ID_URL] = self.original_project_id

        global _override_set
        _override_set = False


def project_id():
    """Gets the project_id for this instance from the Compute Engine metadata
    server. If the metadata server is unavailable, it assumes that the
    application is running locally and falls back to the GOOGLE_CLOUD_PROJECT
    environment variable.
    """
    return (
        _get_metadata(_PROJECT_ID_URL) or os.getenv('GOOGLE_CLOUD_PROJECT')
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
